package analysis

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"sync"
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
	chunkUploadPacerQueue   = 8
)

type chunkUploadPCM struct {
	sourceID    string
	displayName string
	pcm         []byte
}

type chunkUploadPacer struct {
	ch        chan chunkUploadPCM
	closeOnce sync.Once
}

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

	p.enqueueChunkPCM(runtimeID, displayName, pcm)
	return nil
}

func (p *AudioPipelineService) ensureChunkUploadSource(runtimeID, displayName, connectionString string) error {
	p.sourcesMu.Lock()
	defer p.sourcesMu.Unlock()

	sourceIDs := []string{runtimeID}
	sourceModelMap := map[string][]string{runtimeID: chunkUploadModelIDs()}

	if _, ok := p.engine.Registry().Get(runtimeID); ok {
		if p.chunkUploadModelsChanged(runtimeID, sourceModelMap[runtimeID]) {
			p.reconfigureChunkUploadSource(runtimeID, sourceIDs, sourceModelMap)
		}
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

func (p *AudioPipelineService) chunkUploadModelsChanged(sourceID string, configModelIDs []string) bool {
	if p == nil || p.bnAnalyzer == nil || p.bnAnalyzer.BirdNET() == nil || p.engine == nil {
		return false
	}
	loadedModels := loadedModelInfoMap(p.bnAnalyzer.BirdNET().ModelInfos())
	primaryModelID := p.bnAnalyzer.BirdNET().PrimaryModelInfo().ID
	return sourceModelsChanged(p.engine.BufferManager(), sourceID, configModelIDs, loadedModels, primaryModelID)
}

func (p *AudioPipelineService) reconfigureChunkUploadSource(sourceID string, sourceIDs []string, sourceModelMap map[string][]string) {
	p.engine.Router().RemoveAllRoutes(sourceID)
	p.untrackSoundLevelConsumer(sourceID)

	loadedModels := loadedModelInfoMap(p.bnAnalyzer.BirdNET().ModelInfos())
	primaryModelID := p.bnAnalyzer.BirdNET().PrimaryModelInfo().ID
	desiredSet := resolveDesiredModelSet(sourceModelMap[sourceID], loadedModels, primaryModelID)
	deallocateStaleAnalysisBuffers(p.engine.BufferManager(), sourceID, desiredSet)

	p.registerConsumersForSources(sourceIDs, sourceModelMap, p.apiService.AudioLevelChan(), "chunk_upload_model_change")
	p.registerSoundLevelConsumers(sourceIDs, "chunk_upload_model_change")

	if p.bufferMgr != nil {
		monitorMap := p.buildMonitorConfigs(sourceModelMap, sourceIDs)
		if err := p.bufferMgr.UpdateMonitors(monitorMap); err != nil {
			audiocore.GetLogger().Warn("buffer monitor update failed during chunk upload model change",
				logger.String("source_id", sourceID),
				logger.Error(err),
				logger.String("operation", "chunk_upload_model_change"))
		}
	}
}

func (p *AudioPipelineService) enqueueChunkPCM(sourceID, displayName string, pcm []byte) {
	pacer := p.chunkUploadPacer(sourceID)
	pacer.ch <- chunkUploadPCM{
		sourceID:    sourceID,
		displayName: displayName,
		pcm:         pcm,
	}
}

func (p *AudioPipelineService) chunkUploadPacer(sourceID string) *chunkUploadPacer {
	p.chunkUploadMu.Lock()
	defer p.chunkUploadMu.Unlock()

	if p.chunkUploadPacers == nil {
		p.chunkUploadPacers = make(map[string]*chunkUploadPacer)
	}
	if pacer, ok := p.chunkUploadPacers[sourceID]; ok {
		return pacer
	}

	pacer := &chunkUploadPacer{ch: make(chan chunkUploadPCM, chunkUploadPacerQueue)}
	p.chunkUploadPacers[sourceID] = pacer
	p.wg.Add(1)
	go p.runChunkUploadPacer(pacer)
	return pacer
}

func (p *AudioPipelineService) runChunkUploadPacer(pacer *chunkUploadPacer) {
	defer p.wg.Done()

	for {
		select {
		case <-p.done:
			pacer.closeOnce.Do(func() { close(pacer.ch) })
			return
		case item := <-pacer.ch:
			p.dispatchChunkPCMRealtime(item.sourceID, item.displayName, item.pcm)
		}
	}
}

func chunkUploadModelIDs() []string {
	models := conf.Setting().Realtime.Audio.ChunkUpload.Models
	if len(models) == 0 {
		return nil
	}
	ids := make([]string, 0, len(models))
	for _, model := range models {
		model = strings.TrimSpace(model)
		if model == "" {
			continue
		}
		ids = append(ids, model)
	}
	if len(ids) == 0 {
		return nil
	}
	return ids
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

func (p *AudioPipelineService) dispatchChunkPCMRealtime(sourceID, displayName string, pcm []byte) {
	frameSize := chunkUploadFrameSize
	if frameSize <= 0 {
		frameSize = len(pcm)
	}
	frameDuration := chunkUploadFrameDuration(frameSize)
	ticker := time.NewTicker(frameDuration)
	defer ticker.Stop()

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
			Timestamp:  time.Now(),
		})
		p.engine.Registry().RecordAudioData(sourceID, end-offset)

		if end >= len(pcm) {
			return
		}
		select {
		case <-p.done:
			return
		case <-ticker.C:
		}
	}
}

func chunkUploadFrameDuration(frameBytes int) time.Duration {
	bytesPerSecond := conf.SampleRate * conf.NumChannels * conf.BytesPerSample
	if bytesPerSecond <= 0 {
		return 100 * time.Millisecond
	}
	duration := time.Duration(float64(frameBytes) / float64(bytesPerSecond) * float64(time.Second))
	if duration <= 0 {
		return 100 * time.Millisecond
	}
	return duration
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
