package audiocore

import "errors"

// Chunk-upload sentinel errors let transports distinguish permanent client
// failures from retryable pipeline pressure without parsing error strings.
var (
	ErrChunkUploadInvalidAudio   = errors.New("invalid chunk upload audio")
	ErrChunkUploadTooLong        = errors.New("chunk upload exceeds maximum duration")
	ErrChunkUploadSourceConflict = errors.New("chunk upload source conflict")
	ErrChunkUploadSourceLimit    = errors.New("chunk upload source limit reached")
	ErrChunkUploadQueueFull      = errors.New("chunk upload queue full")
	ErrChunkUploadUnavailable    = errors.New("chunk upload pipeline unavailable")
)
