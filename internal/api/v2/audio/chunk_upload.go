package audio

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/tphakala/birdnet-go/internal/api/v2/apicore"
	"github.com/tphakala/birdnet-go/internal/audiocore"
	"github.com/tphakala/birdnet-go/internal/logger"
)

const (
	defaultChunkUploadMaxBytes = int64(5 * 1024 * 1024)
	chunkUploadPermFile        = 0o644
	chunkUploadPermDir         = 0o755
	maxChunkUploadSourceIDLen  = 64
	maxConcurrentChunkUploads  = 2

	// ChunkUploadPath is the route pattern for bearer-token WAV chunk uploads.
	ChunkUploadPath = "/streams/chunks/:source"
)

var chunkUploadFileSequence atomic.Uint64

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

	if !c.acquireChunkUploadSlot() {
		return c.HandleError(ctx, nil, "too many concurrent chunk uploads", http.StatusServiceUnavailable)
	}
	defer c.releaseChunkUploadSlot()

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

	holder := c.chunkUploadIngestor.Load()
	if holder == nil || holder.ingestor == nil {
		return c.HandleError(ctx, nil, "chunk upload processor unavailable", http.StatusServiceUnavailable)
	}
	ingestor := holder.ingestor

	var savedPath, filename string
	if cfg.Save {
		savedPath, _, filename, err = persistChunkUpload(cfg.Path, sourceID, ctx.Request().Header.Get("X-Sequence"), body)
		if err != nil {
			return c.HandleError(ctx, err, "failed to save uploaded chunk", http.StatusInternalServerError)
		}
	}

	if err := ingestor.IngestAudioChunk(ctx.Request().Context(), sourceID, body, cfg.MaxSeconds); err != nil {
		if savedPath != "" {
			removeFile := os.Remove
			if c.removeChunkUploadFile != nil {
				removeFile = c.removeChunkUploadFile
			}
			if removeErr := removeFile(savedPath); removeErr != nil {
				apicore.GetLogger().Error("failed to remove rejected chunk upload",
					logger.String("path", savedPath),
					logger.String("source_id", sourceID),
					logger.String("operation", "chunk_upload_rejection_cleanup"),
					logger.Error(removeErr))
			}
		}
		status := chunkUploadIngestStatus(err)
		return c.HandleError(ctx, err, "failed to process uploaded chunk", status)
	}

	if !cfg.Save {
		return ctx.JSON(http.StatusAccepted, map[string]any{
			"status":    "accepted",
			"source":    sourceID,
			"bytes":     len(body),
			"saved":     false,
			"processed": true,
		})
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

func (c *Handler) acquireChunkUploadSlot() bool {
	c.chunkUploadSlotsOnce.Do(func() {
		c.chunkUploadSlots = make(chan struct{}, maxConcurrentChunkUploads)
	})
	select {
	case c.chunkUploadSlots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (c *Handler) releaseChunkUploadSlot() {
	<-c.chunkUploadSlots
}

func persistChunkUpload(basePath, sourceID, sequenceHeader string, body []byte) (filePath, sourceDir, filename string, err error) {
	basePath = strings.TrimSpace(basePath)
	if basePath == "" {
		basePath = "chunks/inbox"
	}
	sourceDir = filepath.Join(basePath, sourceID)
	if mkdirErr := os.MkdirAll(sourceDir, chunkUploadPermDir); mkdirErr != nil {
		return "", sourceDir, "", mkdirErr
	}

	sequence := sanitizeFilenameToken(sequenceHeader, "seq")
	filename = fmt.Sprintf("%s_%s_%d.wav",
		time.Now().UTC().Format("20060102T150405.000000000Z"), sequence, chunkUploadFileSequence.Add(1))
	filePath = filepath.Join(sourceDir, filename)
	tempPath := filePath + ".part"
	file, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, chunkUploadPermFile)
	if err != nil {
		return "", sourceDir, "", err
	}
	if _, err = file.Write(body); err != nil {
		_ = file.Close()
		_ = os.Remove(tempPath)
		return "", sourceDir, "", err
	}
	if err = file.Close(); err != nil {
		_ = os.Remove(tempPath)
		return "", sourceDir, "", err
	}
	if err = os.Rename(tempPath, filePath); err != nil {
		_ = os.Remove(tempPath)
		return "", sourceDir, "", err
	}
	return filePath, sourceDir, filename, nil
}

func chunkUploadIngestStatus(err error) int {
	switch {
	case errors.Is(err, audiocore.ErrChunkUploadInvalidAudio):
		return http.StatusUnprocessableEntity
	case errors.Is(err, audiocore.ErrChunkUploadTooLong):
		return http.StatusRequestEntityTooLarge
	case errors.Is(err, audiocore.ErrChunkUploadSourceConflict):
		return http.StatusConflict
	case errors.Is(err, audiocore.ErrChunkUploadSourceLimit):
		return http.StatusTooManyRequests
	default:
		return http.StatusServiceUnavailable
	}
}

func looksLikeWAV(data []byte) bool {
	if len(data) < 12 {
		return false
	}
	return string(data[0:4]) == "RIFF" && string(data[8:12]) == "WAVE"
}

func isValidChunkSourceID(sourceID string) bool {
	if sourceID == "" || len(sourceID) > maxChunkUploadSourceIDLen {
		return false
	}
	for i := range len(sourceID) {
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
	providedHash := sha256.Sum256([]byte(strings.TrimSpace(parts[1])))
	expectedHash := sha256.Sum256([]byte(expectedToken))
	return subtle.ConstantTimeCompare(providedHash[:], expectedHash[:]) == 1
}

func sanitizeFilenameToken(raw, fallback string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback
	}
	b := strings.Builder{}
	b.Grow(len(raw))
	for i := range len(raw) {
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
