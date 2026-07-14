package audio

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tphakala/birdnet-go/internal/api/v2/apicore"
	"github.com/tphakala/birdnet-go/internal/conf"
)

type mockChunkUploadIngestor struct {
	calls      int
	sourceID   string
	body       []byte
	maxSeconds int
	err        error
}

func (m *mockChunkUploadIngestor) IngestAudioChunk(_ context.Context, sourceID string, wav []byte, maxSeconds int) error {
	m.calls++
	m.sourceID = sourceID
	m.body = append([]byte(nil), wav...)
	m.maxSeconds = maxSeconds
	return m.err
}

func newChunkUploadHandler(t *testing.T, cfg conf.ChunkUploadSettings) (*echo.Echo, *Handler) {
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
		ingestor := &mockChunkUploadIngestor{}
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
}
