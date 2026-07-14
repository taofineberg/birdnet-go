package analysis

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/tphakala/birdnet-go/internal/audiocore"
	"github.com/tphakala/birdnet-go/internal/audiocore/ffmpeg"
	"github.com/tphakala/birdnet-go/internal/conf"
	"github.com/tphakala/birdnet-go/internal/logger"
)

const (
	chunkUploadSourcePrefix = "chunk_"
	chunkUploadScheme       = "chunk-upload://"
	chunkUploadFrameSize    = conf.SampleRate * conf.NumChannels * conf.BytesPerSample / 10
)

// IngestAudioChunk decodes an uploaded WAV chunk and feeds it into the same
// router/consumer path used by live audio sources.
func (p *AudioPipelineService) IngestAudioChunk(ctx context.Context, sourceID string, wav []byte, maxSeconds int) error {
	if p == nil || p.engine == nil || p.bufferMgr == nil || p.apiService == nil {
		return fmt.Errorf("chunk upload pipeline is not ready")
	}

	runtimeID := chunkUploadRuntimeSourceID(sourceID)
	displayName := chunkUploadDisplayName(sourceID)
	if err := p.ensureChunkUploadSource(runtimeID, displayName, chunkUploadScheme+sourceID); err != nil {
		return err
	}

	pcm, err := decodeChunkUploadWAV(ctx, conf.Setting().Realtime.Audio.FfmpegPath, wav)
	if err != nil {
		return err
	}
	if len(pcm) == 0 {
		return fmt.Errorf("decoded chunk is empty")
	}
	if maxSeconds > 0 {
		maxPCMBytes := maxSeconds * conf.SampleRate * conf.NumChannels * conf.BytesPerSample
		if len(pcm) > maxPCMBytes {
			return fmt.Errorf("decoded chunk duration exceeds maxSeconds: %d > %d", decodedPCMSeconds(len(pcm)), maxSeconds)
		}
	}

	p.dispatchChunkPCM(runtimeID, displayName, pcm)
	p.engine.Registry().RecordAudioData(runtimeID, len(pcm))
	return nil
}

func (p *AudioPipelineService) ensureChunkUploadSource(runtimeID, displayName, connectionString string) error {
	p.sourcesMu.Lock()
	defer p.sourcesMu.Unlock()

	if _, ok := p.engine.Registry().Get(runtimeID); ok {
		return nil
	}

	cfg := &audiocore.SourceConfig{
		ID:               runtimeID,
		DisplayName:      displayName,
		Type:             audiocore.SourceTypeChunkUpload,
		ConnectionString: connectionString,
		SampleRate:       conf.SampleRate,
		SourceSampleRate: conf.SampleRate,
		BitDepth:         conf.BitDepth,
		Channels:         conf.NumChannels,
		SourceChannels:   conf.NumChannels,
	}
	if err := p.engine.AddSource(cfg); err != nil {
		return fmt.Errorf("add chunk upload source: %w", err)
	}
	_ = p.engine.Registry().UpdateState(runtimeID, audiocore.SourceRunning)

	sourceIDs := []string{runtimeID}
	sourceModelMap := map[string][]string{runtimeID: nil}
	p.registerConsumersForSources(sourceIDs, sourceModelMap, p.apiService.AudioLevelChan(), "chunk_upload")
	p.registerSoundLevelConsumers(sourceIDs, "chunk_upload")

	sourceMonitorConfigs := p.buildMonitorConfigs(sourceModelMap, sourceIDs)
	if err := p.bufferMgr.AddMonitors(runtimeID, sourceMonitorConfigs[runtimeID]); err != nil {
		return fmt.Errorf("add chunk upload monitors: %w", err)
	}

	audiocore.GetLogger().Info("registered chunk upload audio source",
		logger.String("source_id", runtimeID),
		logger.String("display_name", displayName),
		logger.String("operation", "chunk_upload"))

	return nil
}

func decodeChunkUploadWAV(ctx context.Context, ffmpegPath string, wav []byte) ([]byte, error) {
	if err := ffmpeg.ValidateFFmpegPath(ffmpegPath); err != nil {
		return nil, err
	}

	args := []string{
		"-hide_banner",
		"-loglevel", "error",
		"-f", "wav",
		"-i", "pipe:0",
		"-ac", fmt.Sprintf("%d", conf.NumChannels),
		"-ar", fmt.Sprintf("%d", conf.SampleRate),
		"-f", "s16le",
		"pipe:1",
	}
	cmd := exec.CommandContext(ctx, ffmpegPath, args...) //nolint:gosec // G204: ffmpegPath validated by ValidateFFmpegPath, args built internally
	cmd.Stdin = bytes.NewReader(wav)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("decode uploaded WAV: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

func (p *AudioPipelineService) dispatchChunkPCM(sourceID, displayName string, pcm []byte) {
	now := time.Now()
	frameSize := chunkUploadFrameSize
	if frameSize <= 0 {
		frameSize = len(pcm)
	}

	for offset := 0; offset < len(pcm); offset += frameSize {
		end := offset + frameSize
		if end > len(pcm) {
			end = len(pcm)
		}
		p.engine.Router().Dispatch(audiocore.AudioFrame{
			SourceID:   sourceID,
			SourceName: displayName,
			Data:       pcm[offset:end],
			SampleRate: conf.SampleRate,
			BitDepth:   conf.BitDepth,
			Channels:   conf.NumChannels,
			Timestamp:  now.Add(time.Duration(offset/frameSize) * 100 * time.Millisecond),
		})
	}
}

func chunkUploadRuntimeSourceID(sourceID string) string {
	return chunkUploadSourcePrefix + sourceID
}

func chunkUploadDisplayName(sourceID string) string {
	return "Chunk Upload: " + sourceID
}

func decodedPCMSeconds(byteCount int) int {
	bytesPerSecond := conf.SampleRate * conf.NumChannels * conf.BytesPerSample
	if bytesPerSecond <= 0 {
		return 0
	}
	return (byteCount + bytesPerSecond - 1) / bytesPerSecond
}
