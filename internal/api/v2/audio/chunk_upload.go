package audio

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
)

const (
	defaultChunkUploadMaxBytes = int64(5 * 1024 * 1024)
	chunkUploadPermFile        = 0o644
	chunkUploadPermDir         = 0o755

	// ChunkUploadPath is the route pattern for bearer-token WAV chunk uploads.
	ChunkUploadPath = "/streams/chunks/:source"
)

// RegisterChunkUploadRoutes registers the v2 chunk upload endpoint.
func (c *Handler) RegisterChunkUploadRoutes(g *echo.Group) {
	g.POST(ChunkUploadPath, c.UploadAudioChunk)
}

// UploadAudioChunk accepts a WAV chunk upload for a logical source ID.
func (c *Handler) UploadAudioChunk(ctx echo.Context) error {
	settings := c.CurrentSettings()
	if settings == nil {
		return c.HandleError(ctx, nil, "settings unavailable", http.StatusServiceUnavailable)
	}

	cfg := settings.Realtime.Audio.ChunkUpload
	if !cfg.Enabled {
		return ctx.NoContent(http.StatusNotFound)
	}

	sourceID := strings.TrimSpace(ctx.Param("source"))
	if !isValidChunkSourceID(sourceID) {
		return c.HandleError(ctx, nil, "invalid source id", http.StatusBadRequest)
	}

	if !isBearerTokenAuthorized(ctx.Request().Header.Get(echo.HeaderAuthorization), cfg.Token) {
		return c.HandleError(ctx, nil, "invalid or missing token", http.StatusUnauthorized)
	}

	contentType := strings.ToLower(strings.TrimSpace(ctx.Request().Header.Get(echo.HeaderContentType)))
	if contentType != "" &&
		!strings.HasPrefix(contentType, "audio/wav") &&
		!strings.HasPrefix(contentType, "audio/x-wav") &&
		!strings.HasPrefix(contentType, "application/octet-stream") {
		return c.HandleError(ctx, nil, "unsupported audio format", http.StatusUnsupportedMediaType)
	}

	maxBytes := cfg.MaxBytes
	if maxBytes <= 0 {
		maxBytes = defaultChunkUploadMaxBytes
	}

	body, err := io.ReadAll(io.LimitReader(ctx.Request().Body, maxBytes+1))
	if err != nil {
		return c.HandleError(ctx, err, "failed to read upload body", http.StatusBadRequest)
	}
	if len(body) == 0 {
		return c.HandleError(ctx, nil, "empty upload body", http.StatusBadRequest)
	}
	if int64(len(body)) > maxBytes {
		return c.HandleError(ctx, nil, "chunk too large", http.StatusRequestEntityTooLarge)
	}
	if !looksLikeWAV(body) {
		return c.HandleError(ctx, nil, "unsupported audio format", http.StatusUnsupportedMediaType)
	}

	ingestor := c.chunkUploadIngestor
	if ingestor == nil {
		return c.HandleError(ctx, nil, "chunk upload processor unavailable", http.StatusServiceUnavailable)
	}

	if !cfg.Save {
		if err := ingestor.IngestAudioChunk(ctx.Request().Context(), sourceID, body, cfg.MaxSeconds); err != nil {
			return c.HandleError(ctx, err, "failed to process uploaded chunk", http.StatusInternalServerError)
		}
		return ctx.JSON(http.StatusAccepted, map[string]any{
			"status":    "accepted",
			"source":    sourceID,
			"bytes":     len(body),
			"saved":     false,
			"processed": true,
		})
	}

	basePath := strings.TrimSpace(cfg.Path)
	if basePath == "" {
		basePath = "chunks/inbox"
	}
	sourceDir := filepath.Join(basePath, sourceID)
	if err := os.MkdirAll(sourceDir, chunkUploadPermDir); err != nil {
		return c.HandleError(ctx, err, "failed to create chunk upload directory", http.StatusInternalServerError)
	}

	sequence := sanitizeFilenameToken(ctx.Request().Header.Get("X-Sequence"), "seq")
	filename := fmt.Sprintf("%s_%s.wav", time.Now().UTC().Format("20060102T150405.000Z"), sequence)
	filePath := filepath.Join(sourceDir, filename)
	if err := os.WriteFile(filePath, body, chunkUploadPermFile); err != nil {
		return c.HandleError(ctx, err, "failed to save uploaded chunk", http.StatusInternalServerError)
	}
	if err := ingestor.IngestAudioChunk(ctx.Request().Context(), sourceID, body, cfg.MaxSeconds); err != nil {
		return c.HandleError(ctx, err, "failed to process uploaded chunk", http.StatusInternalServerError)
	}

	return ctx.JSON(http.StatusAccepted, map[string]any{
		"status":    "accepted",
		"source":    sourceID,
		"bytes":     len(body),
		"saved":     true,
		"processed": true,
		"file":      filename,
	})
}

func looksLikeWAV(data []byte) bool {
	if len(data) < 12 {
		return false
	}
	return string(data[0:4]) == "RIFF" && string(data[8:12]) == "WAVE"
}

func isValidChunkSourceID(sourceID string) bool {
	if sourceID == "" {
		return false
	}
	for i := 0; i < len(sourceID); i++ {
		ch := sourceID[i]
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' {
			continue
		}
		return false
	}
	return true
}

func isBearerTokenAuthorized(authHeader, expectedToken string) bool {
	expectedToken = strings.TrimSpace(expectedToken)
	if expectedToken == "" {
		return false
	}

	parts := strings.SplitN(strings.TrimSpace(authHeader), " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return false
	}
	return strings.TrimSpace(parts[1]) == expectedToken
}

func sanitizeFilenameToken(raw, fallback string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback
	}
	b := strings.Builder{}
	b.Grow(len(raw))
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' {
			b.WriteByte(ch)
		}
	}
	if b.Len() == 0 {
		return fallback
	}
	return b.String()
}
