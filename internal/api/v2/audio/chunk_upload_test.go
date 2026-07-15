package audio

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tphakala/birdnet-go/internal/api/v2/apicore"
	"github.com/tphakala/birdnet-go/internal/audiocore"
	"github.com/tphakala/birdnet-go/internal/conf"
)

type mockChunkUploadIngestor struct {
	calls        int
	sourceID     string
	body         []byte
	maxSeconds   int
	err          error
	beforeReturn func()
}

type readTrackingBody struct {
	read bool
}

func (b *readTrackingBody) Read(_ []byte) (int, error) {
	b.read = true
	return 0, io.EOF
}

func (b *readTrackingBody) Close() error { return nil }

func (m *mockChunkUploadIngestor) IngestAudioChunk(_ context.Context, sourceID string, wav []byte, maxSeconds int) error {
	m.calls++
	m.sourceID = sourceID
	m.body = bytes.Clone(wav)
	m.maxSeconds = maxSeconds
	if m.beforeReturn != nil {
		m.beforeReturn()
	}
	return m.err
}

func newChunkUploadHandler(t *testing.T, cfg conf.ChunkUploadSettings) (*echo.Echo, *Handler) { //nolint:gocritic // Value keeps table-style test setup concise.
	t.Helper()

	e := echo.New()
	h := &Handler{Core: &apicore.Core{Echo: e, Group: e.Group("/api/v2")}}
	h.Settings.Store(&conf.Settings{
		Realtime: conf.RealtimeSettings{
			Audio: conf.AudioSettings{
				ChunkUpload: cfg,
			},
		},
	})
	return e, h
}

func testWAVPayload() []byte {
	// Minimal RIFF/WAVE header with a short PCM payload.
	return []byte{
		'R', 'I', 'F', 'F', 36, 0, 0, 0,
		'W', 'A', 'V', 'E',
		'f', 'm', 't', ' ', 16, 0, 0, 0,
		1, 0, 1, 0,
		0x80, 0xbb, 0x00, 0x00,
		0x00, 0x77, 0x01, 0x00,
		2, 0, 16, 0,
		'd', 'a', 't', 'a', 2, 0, 0, 0,
		0x00, 0x00,
	}
}

func TestUploadAudioChunk(t *testing.T) {
	t.Parallel()

	t.Run("disabled endpoint returns not found", func(t *testing.T) {
		e, h := newChunkUploadHandler(t, conf.ChunkUploadSettings{Enabled: false})

		req := httptest.NewRequest(http.MethodPost, "/api/v2/streams/chunks/yard", bytes.NewReader(testWAVPayload()))
		req.Header.Set(echo.HeaderAuthorization, "Bearer token")
		req.Header.Set(echo.HeaderContentType, "audio/wav")
		rec := httptest.NewRecorder()
		ctx := e.NewContext(req, rec)
		ctx.SetPath("/api/v2/streams/chunks/:source")
		ctx.SetParamNames("source")
		ctx.SetParamValues("yard")

		err := h.UploadAudioChunk(ctx)
		require.NoError(t, err)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("invalid token returns unauthorized", func(t *testing.T) {
		tmp := t.TempDir()
		e, h := newChunkUploadHandler(t, conf.ChunkUploadSettings{
			Enabled:  true,
			Token:    "expected-token",
			Path:     tmp,
			Save:     true,
			MaxBytes: 1024,
		})

		req := httptest.NewRequest(http.MethodPost, "/api/v2/streams/chunks/yard", bytes.NewReader(testWAVPayload()))
		req.Header.Set(echo.HeaderAuthorization, "Bearer wrong")
		req.Header.Set(echo.HeaderContentType, "audio/wav")
		rec := httptest.NewRecorder()
		ctx := e.NewContext(req, rec)
		ctx.SetPath("/api/v2/streams/chunks/:source")
		ctx.SetParamNames("source")
		ctx.SetParamValues("yard")

		err := h.UploadAudioChunk(ctx)
		require.NoError(t, err)
		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("invalid format returns unsupported media type", func(t *testing.T) {
		tmp := t.TempDir()
		e, h := newChunkUploadHandler(t, conf.ChunkUploadSettings{
			Enabled:  true,
			Token:    "expected-token",
			Path:     tmp,
			Save:     true,
			MaxBytes: 1024,
		})

		req := httptest.NewRequest(http.MethodPost, "/api/v2/streams/chunks/yard", bytes.NewReader([]byte("not-wav")))
		req.Header.Set(echo.HeaderAuthorization, "Bearer expected-token")
		req.Header.Set(echo.HeaderContentType, "audio/wav")
		rec := httptest.NewRecorder()
		ctx := e.NewContext(req, rec)
		ctx.SetPath("/api/v2/streams/chunks/:source")
		ctx.SetParamNames("source")
		ctx.SetParamValues("yard")

		err := h.UploadAudioChunk(ctx)
		require.NoError(t, err)
		assert.Equal(t, http.StatusUnsupportedMediaType, rec.Code)
	})

	t.Run("oversized chunk returns request entity too large", func(t *testing.T) {
		tmp := t.TempDir()
		wav := testWAVPayload()
		e, h := newChunkUploadHandler(t, conf.ChunkUploadSettings{
			Enabled:  true,
			Token:    "expected-token",
			Path:     tmp,
			Save:     true,
			MaxBytes: int64(len(wav) - 1),
		})

		req := httptest.NewRequest(http.MethodPost, "/api/v2/streams/chunks/yard", bytes.NewReader(wav))
		req.Header.Set(echo.HeaderAuthorization, "Bearer expected-token")
		req.Header.Set(echo.HeaderContentType, "audio/wav")
		rec := httptest.NewRecorder()
		ctx := e.NewContext(req, rec)
		ctx.SetPath("/api/v2/streams/chunks/:source")
		ctx.SetParamNames("source")
		ctx.SetParamValues("yard")

		err := h.UploadAudioChunk(ctx)
		require.NoError(t, err)
		assert.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
	})

	t.Run("valid upload saves chunk", func(t *testing.T) {
		tmp := t.TempDir()
		wav := testWAVPayload()
		e, h := newChunkUploadHandler(t, conf.ChunkUploadSettings{
			Enabled:    true,
			Token:      "expected-token",
			Path:       tmp,
			Save:       true,
			MaxBytes:   1024 * 1024,
			MaxSeconds: 15,
		})
		persistedBeforeIngest := false
		ingestor := &mockChunkUploadIngestor{beforeReturn: func() {
			entries, readErr := os.ReadDir(filepath.Join(tmp, "yard"))
			persistedBeforeIngest = readErr == nil && len(entries) == 1 && filepath.Ext(entries[0].Name()) == ".wav"
		}}
		h.SetChunkUploadIngestor(ingestor)

		req := httptest.NewRequest(http.MethodPost, "/api/v2/streams/chunks/yard", bytes.NewReader(wav))
		req.Header.Set(echo.HeaderAuthorization, "Bearer expected-token")
		req.Header.Set(echo.HeaderContentType, "audio/wav")
		req.Header.Set("X-Sequence", "12345")
		rec := httptest.NewRecorder()
		ctx := e.NewContext(req, rec)
		ctx.SetPath("/api/v2/streams/chunks/:source")
		ctx.SetParamNames("source")
		ctx.SetParamValues("yard")

		err := h.UploadAudioChunk(ctx)
		require.NoError(t, err)
		assert.Equal(t, http.StatusAccepted, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, "accepted", resp["status"])
		assert.Equal(t, "yard", resp["source"])
		assert.Equal(t, true, resp["saved"])
		assert.Equal(t, true, resp["processed"])
		assert.Equal(t, 1, ingestor.calls)
		assert.Equal(t, "yard", ingestor.sourceID)
		assert.Equal(t, wav, ingestor.body)
		assert.Equal(t, 15, ingestor.maxSeconds)
		assert.True(t, persistedBeforeIngest)

		sourceDir := filepath.Join(tmp, "yard")
		entries, err := os.ReadDir(sourceDir)
		require.NoError(t, err)
		require.Len(t, entries, 1)
		assert.Equal(t, ".wav", filepath.Ext(entries[0].Name()))
	})

	t.Run("valid upload without save still processes chunk", func(t *testing.T) {
		wav := testWAVPayload()
		e, h := newChunkUploadHandler(t, conf.ChunkUploadSettings{
			Enabled:  true,
			Token:    "expected-token",
			Save:     false,
			MaxBytes: 1024 * 1024,
		})
		ingestor := &mockChunkUploadIngestor{}
		h.SetChunkUploadIngestor(ingestor)

		req := httptest.NewRequest(http.MethodPost, "/api/v2/streams/chunks/yard", bytes.NewReader(wav))
		req.Header.Set(echo.HeaderAuthorization, "Bearer expected-token")
		req.Header.Set(echo.HeaderContentType, "audio/wav")
		rec := httptest.NewRecorder()
		ctx := e.NewContext(req, rec)
		ctx.SetPath("/api/v2/streams/chunks/:source")
		ctx.SetParamNames("source")
		ctx.SetParamValues("yard")

		err := h.UploadAudioChunk(ctx)
		require.NoError(t, err)
		assert.Equal(t, http.StatusAccepted, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, false, resp["saved"])
		assert.Equal(t, true, resp["processed"])
		assert.Equal(t, 1, ingestor.calls)
		assert.Equal(t, "yard", ingestor.sourceID)
		assert.Equal(t, wav, ingestor.body)
	})

	t.Run("valid upload without processor returns service unavailable", func(t *testing.T) {
		tmp := t.TempDir()
		e, h := newChunkUploadHandler(t, conf.ChunkUploadSettings{
			Enabled:  true,
			Token:    "expected-token",
			Path:     tmp,
			Save:     true,
			MaxBytes: 1024 * 1024,
		})

		req := httptest.NewRequest(http.MethodPost, "/api/v2/streams/chunks/yard", bytes.NewReader(testWAVPayload()))
		req.Header.Set(echo.HeaderAuthorization, "Bearer expected-token")
		req.Header.Set(echo.HeaderContentType, "audio/wav")
		rec := httptest.NewRecorder()
		ctx := e.NewContext(req, rec)
		ctx.SetPath("/api/v2/streams/chunks/:source")
		ctx.SetParamNames("source")
		ctx.SetParamValues("yard")

		err := h.UploadAudioChunk(ctx)
		require.NoError(t, err)
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})

	t.Run("processor rejection does not save chunk", func(t *testing.T) {
		tmp := t.TempDir()
		e, h := newChunkUploadHandler(t, conf.ChunkUploadSettings{
			Enabled: true, Token: "expected-token", Path: tmp, Save: true, MaxBytes: 1024 * 1024,
		})
		ingestor := &mockChunkUploadIngestor{err: assert.AnError}
		h.SetChunkUploadIngestor(ingestor)

		req := httptest.NewRequest(http.MethodPost, "/api/v2/streams/chunks/yard", bytes.NewReader(testWAVPayload()))
		req.Header.Set(echo.HeaderAuthorization, "Bearer expected-token")
		req.Header.Set(echo.HeaderContentType, "audio/wav")
		rec := httptest.NewRecorder()
		ctx := e.NewContext(req, rec)
		ctx.SetParamNames("source")
		ctx.SetParamValues("yard")

		require.NoError(t, h.UploadAudioChunk(ctx))
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
		entries, err := os.ReadDir(filepath.Join(tmp, "yard"))
		require.NoError(t, err)
		assert.Empty(t, entries)
	})

	t.Run("cleanup failure leaves an observable rejected file", func(t *testing.T) {
		tmp := t.TempDir()
		e, h := newChunkUploadHandler(t, conf.ChunkUploadSettings{
			Enabled: true, Token: "expected-token", Path: tmp, Save: true, MaxBytes: 1024 * 1024,
		})
		h.SetChunkUploadIngestor(&mockChunkUploadIngestor{err: audiocore.ErrChunkUploadInvalidAudio})
		h.removeChunkUploadFile = func(string) error { return assert.AnError }

		req := httptest.NewRequest(http.MethodPost, "/api/v2/streams/chunks/yard", bytes.NewReader(testWAVPayload()))
		req.Header.Set(echo.HeaderAuthorization, "Bearer expected-token")
		rec := httptest.NewRecorder()
		ctx := e.NewContext(req, rec)
		ctx.SetParamNames("source")
		ctx.SetParamValues("yard")

		require.NoError(t, h.UploadAudioChunk(ctx))
		assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		entries, err := os.ReadDir(filepath.Join(tmp, "yard"))
		require.NoError(t, err)
		require.Len(t, entries, 1)
		assert.Equal(t, ".wav", filepath.Ext(entries[0].Name()))
	})

	t.Run("overlong source id is rejected", func(t *testing.T) {
		e, h := newChunkUploadHandler(t, conf.ChunkUploadSettings{
			Enabled: true, Token: "expected-token", MaxBytes: 1024 * 1024,
		})
		sourceID := strings.Repeat("a", maxChunkUploadSourceIDLen+1)
		req := httptest.NewRequest(http.MethodPost, "/api/v2/streams/chunks/"+sourceID, bytes.NewReader(testWAVPayload()))
		req.Header.Set(echo.HeaderAuthorization, "Bearer expected-token")
		rec := httptest.NewRecorder()
		ctx := e.NewContext(req, rec)
		ctx.SetParamNames("source")
		ctx.SetParamValues(sourceID)

		require.NoError(t, h.UploadAudioChunk(ctx))
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("processor errors map to stable HTTP statuses", func(t *testing.T) {
		tests := []struct {
			name   string
			err    error
			status int
		}{
			{name: "invalid audio", err: audiocore.ErrChunkUploadInvalidAudio, status: http.StatusUnprocessableEntity},
			{name: "duration limit", err: audiocore.ErrChunkUploadTooLong, status: http.StatusRequestEntityTooLarge},
			{name: "source collision", err: audiocore.ErrChunkUploadSourceConflict, status: http.StatusConflict},
			{name: "source limit", err: audiocore.ErrChunkUploadSourceLimit, status: http.StatusTooManyRequests},
			{name: "queue pressure", err: audiocore.ErrChunkUploadQueueFull, status: http.StatusServiceUnavailable},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				e, h := newChunkUploadHandler(t, conf.ChunkUploadSettings{
					Enabled: true, Token: "expected-token", MaxBytes: 1024 * 1024,
				})
				h.SetChunkUploadIngestor(&mockChunkUploadIngestor{err: tt.err})
				req := httptest.NewRequest(http.MethodPost, "/api/v2/streams/chunks/yard", bytes.NewReader(testWAVPayload()))
				req.Header.Set(echo.HeaderAuthorization, "Bearer expected-token")
				rec := httptest.NewRecorder()
				ctx := e.NewContext(req, rec)
				ctx.SetParamNames("source")
				ctx.SetParamValues("yard")

				require.NoError(t, h.UploadAudioChunk(ctx))
				assert.Equal(t, tt.status, rec.Code)
			})
		}
	})
}

func TestUploadAudioChunkRejectsBeforeReadingWhenAtCapacity(t *testing.T) {
	t.Parallel()

	e, h := newChunkUploadHandler(t, conf.ChunkUploadSettings{
		Enabled: true,
		Token:   "expected-token",
	})
	require.True(t, h.acquireChunkUploadSlot())
	require.True(t, h.acquireChunkUploadSlot())
	t.Cleanup(func() {
		h.releaseChunkUploadSlot()
		h.releaseChunkUploadSlot()
	})

	body := &readTrackingBody{}
	req := httptest.NewRequest(http.MethodPost, "/api/v2/streams/chunks/yard", body)
	req.Header.Set(echo.HeaderAuthorization, "Bearer expected-token")
	req.Header.Set(echo.HeaderContentType, "audio/wav")
	rec := httptest.NewRecorder()
	ctx := e.NewContext(req, rec)
	ctx.SetPath("/api/v2/streams/chunks/:source")
	ctx.SetParamNames("source")
	ctx.SetParamValues("yard")

	require.NoError(t, h.UploadAudioChunk(ctx))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.False(t, body.read)
}

func TestRegisterChunkUploadRoutes(t *testing.T) {
	t.Parallel()

	e, h := newChunkUploadHandler(t, conf.ChunkUploadSettings{Enabled: false})
	h.RegisterChunkUploadRoutes(e.Group("/api/v2"))
	req := httptest.NewRequest(http.MethodPost, "/api/v2/streams/chunks/yard", bytes.NewReader(testWAVPayload()))
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
