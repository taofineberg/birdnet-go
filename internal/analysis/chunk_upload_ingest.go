package analysis

import (
	"bytes"
	"context"
	"fmt"
	"io"
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
	maxChunkUploadSources   = 32
	maxChunkUploadPCMBytes  = int64(256 * 1024 * 1024)
	chunkUploadIdleTimeout  = 10 * time.Minute
)

type chunkUploadPCM struct {
	sourceID    string
	displayName string
	pcm         []byte
}

type chunkUploadPacer struct {
	ch chan chunkUploadPCM

	mu           sync.Mutex
	lastActivity time.Time
	processing   bool
	accepting    bool
	stop         chan struct{}
	stopOnce     sync.Once
}

// IngestAudioChunk decodes an uploaded WAV chunk and feeds it into the same
// router/consumer path used by live audio sources.
func (p *AudioPipelineService) IngestAudioChunk(ctx context.Context, sourceID string, wav []byte, maxSeconds int) error {
	if p == nil || p.engine == nil || p.bufferMgr == nil || p.apiService == nil {
		return fmt.Errorf("%w: pipeline is not ready", audiocore.ErrChunkUploadUnavailable)
	}
	if p.chunkUploadsStopping() {
		return fmt.Errorf("%w: pipeline is stopping", audiocore.ErrChunkUploadUnavailable)
	}

	maxPCMBytes := 0
	if maxSeconds > 0 {
		maxPCMBytes = maxSeconds * conf.SampleRate * conf.NumChannels * conf.BytesPerSample
	}
	pcm, err := decodeChunkUploadWAV(ctx, conf.Setting().Realtime.Audio.FfmpegPath, wav, maxPCMBytes)
	if err != nil {
		return err
	}
	if len(pcm) == 0 {
		return fmt.Errorf("%w: decoded chunk is empty", audiocore.ErrChunkUploadInvalidAudio)
	}

	runtimeID := chunkUploadRuntimeSourceID(sourceID)
	displayName := chunkUploadDisplayName(sourceID)
	if err := p.ensureChunkUploadSource(runtimeID, displayName, chunkUploadScheme+sourceID); err != nil {
		return err
	}

	return p.enqueueChunkPCM(ctx, runtimeID, displayName, pcm)
}

func (p *AudioPipelineService) ensureChunkUploadSource(runtimeID, displayName, connectionString string) error {
	p.sourcesMu.Lock()
	defer p.sourcesMu.Unlock()
	if p.chunkUploadsStopping() {
		return fmt.Errorf("%w: pipeline is stopping", audiocore.ErrChunkUploadUnavailable)
	}

	sourceIDs := []string{runtimeID}
	sourceModelMap := map[string][]string{runtimeID: chunkUploadModelIDs()}

	if src, ok := p.engine.Registry().Get(runtimeID); ok {
		existingConnection, _ := p.engine.Registry().ConnectionStringByID(runtimeID)
		if src.Type != audiocore.SourceTypeChunkUpload || existingConnection != connectionString {
			return fmt.Errorf("%w: %s", audiocore.ErrChunkUploadSourceConflict, runtimeID)
		}
		if p.chunkUploadModelsChanged(runtimeID, sourceModelMap[runtimeID]) {
			p.reconfigureChunkUploadSource(runtimeID, sourceIDs, sourceModelMap)
		}
		return nil
	}
	if chunkUploadSourceCount(p.engine.Registry().List()) >= maxChunkUploadSources {
		if !p.evictIdleChunkUploadSource(time.Now()) {
			return fmt.Errorf("%w: maximum is %d", audiocore.ErrChunkUploadSourceLimit, maxChunkUploadSources)
		}
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
		return fmt.Errorf("%w: add source: %w", audiocore.ErrChunkUploadUnavailable, err)
	}
	_ = p.engine.Registry().UpdateState(runtimeID, audiocore.SourceRunning)

	p.registerConsumersForSources(sourceIDs, sourceModelMap, p.apiService.AudioLevelChan(), "chunk_upload")
	p.registerSoundLevelConsumers(sourceIDs, "chunk_upload")

	sourceMonitorConfigs := p.buildMonitorConfigs(sourceModelMap, sourceIDs)
	if err := p.bufferMgr.AddMonitors(runtimeID, sourceMonitorConfigs[runtimeID]); err != nil {
		p.untrackSoundLevelConsumer(runtimeID)
		if removeErr := p.engine.RemoveSource(runtimeID); removeErr != nil {
			audiocore.GetLogger().Warn("failed to roll back chunk upload source",
				logger.String("source_id", runtimeID),
				logger.Error(removeErr))
		}
		return fmt.Errorf("%w: add monitors: %w", audiocore.ErrChunkUploadUnavailable, err)
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
	// Remove only routes owned by the analysis pipeline. HLS routes are created
	// per listener and must survive a model-only reconfiguration.
	p.engine.Router().RemoveRoute(sourceID, "buffer_"+sourceID)
	p.engine.Router().RemoveRoute(sourceID, "audio_level_"+sourceID)
	p.engine.Router().RemoveRoute(sourceID, "soundlevel_"+sourceID)
	p.untrackSoundLevelConsumer(sourceID)

	loadedModels := loadedModelInfoMap(p.bnAnalyzer.BirdNET().ModelInfos())
	primaryModelID := p.bnAnalyzer.BirdNET().PrimaryModelInfo().ID
	desiredSet := resolveDesiredModelSet(sourceModelMap[sourceID], loadedModels, primaryModelID)
	deallocateStaleAnalysisBuffers(p.engine.BufferManager(), sourceID, desiredSet)

	p.registerConsumersForSources(sourceIDs, sourceModelMap, p.apiService.AudioLevelChan(), "chunk_upload_model_change")
	p.registerSoundLevelConsumers(sourceIDs, "chunk_upload_model_change")

	if p.bufferMgr != nil {
		monitorMap := p.buildMonitorConfigs(sourceModelMap, sourceIDs)
		if err := p.bufferMgr.RemoveMonitor(sourceID); err != nil {
			audiocore.GetLogger().Warn("failed to remove stale chunk upload monitors",
				logger.String("source_id", sourceID),
				logger.Error(err),
				logger.String("operation", "chunk_upload_model_change"))
		}
		if err := p.bufferMgr.AddMonitors(sourceID, monitorMap[sourceID]); err != nil {
			audiocore.GetLogger().Warn("buffer monitor update failed during chunk upload model change",
				logger.String("source_id", sourceID),
				logger.Error(err),
				logger.String("operation", "chunk_upload_model_change"))
		}
	}
}

func (p *AudioPipelineService) enqueueChunkPCM(ctx context.Context, sourceID, displayName string, pcm []byte) error {
	if !p.reserveChunkUploadPCM(len(pcm)) {
		return fmt.Errorf("%w: global PCM queue limit reached", audiocore.ErrChunkUploadQueueFull)
	}
	reserved := true
	defer func() {
		if reserved {
			p.chunkUploadPCMBytes.Add(-int64(len(pcm)))
		}
	}()

	pacer := p.chunkUploadPacer(sourceID)
	if pacer == nil {
		return fmt.Errorf("%w: pipeline is stopping", audiocore.ErrChunkUploadUnavailable)
	}
	item := chunkUploadPCM{
		sourceID:    sourceID,
		displayName: displayName,
		pcm:         pcm,
	}
	if err := pacer.enqueue(ctx, p.done, item); err != nil {
		return err
	}
	reserved = false
	return nil
}

func (p *AudioPipelineService) chunkUploadsStopping() bool {
	p.chunkUploadMu.Lock()
	defer p.chunkUploadMu.Unlock()
	return p.chunkUploadStopping
}

func (p *AudioPipelineService) stopAcceptingChunkUploads() {
	// Match the source-mutation lock order used by ensure/eviction so no source
	// or pacer can be created after the shutdown gate is raised.
	p.sourcesMu.Lock()
	p.chunkUploadMu.Lock()
	p.chunkUploadStopping = true
	for _, pacer := range p.chunkUploadPacers {
		pacer.mu.Lock()
		pacer.accepting = false
		pacer.mu.Unlock()
	}
	p.chunkUploadMu.Unlock()
	p.sourcesMu.Unlock()
}

func (p *AudioPipelineService) reserveChunkUploadPCM(size int) bool {
	if size <= 0 {
		return false
	}
	requested := int64(size)
	for {
		current := p.chunkUploadPCMBytes.Load()
		if requested > maxChunkUploadPCMBytes-current {
			return false
		}
		if p.chunkUploadPCMBytes.CompareAndSwap(current, current+requested) {
			return true
		}
	}
}

func (p *AudioPipelineService) chunkUploadPacer(sourceID string) *chunkUploadPacer {
	p.chunkUploadMu.Lock()
	defer p.chunkUploadMu.Unlock()
	if p.chunkUploadStopping {
		return nil
	}

	if p.chunkUploadPacers == nil {
		p.chunkUploadPacers = make(map[string]*chunkUploadPacer)
	}
	if pacer, ok := p.chunkUploadPacers[sourceID]; ok {
		return pacer
	}

	pacer := &chunkUploadPacer{
		ch:           make(chan chunkUploadPCM, chunkUploadPacerQueue),
		lastActivity: time.Now(),
		accepting:    true,
		stop:         make(chan struct{}),
	}
	p.chunkUploadPacers[sourceID] = pacer
	p.wg.Add(1)
	go p.runChunkUploadPacer(sourceID, pacer)
	return pacer
}

func (p *AudioPipelineService) runChunkUploadPacer(sourceID string, pacer *chunkUploadPacer) {
	defer func() {
		p.stopAndDrainChunkUploadPacer(sourceID, pacer)
		p.wg.Done()
	}()

	for {
		// Prefer shutdown over another queued item when shutdown was already
		// signaled before this iteration.
		select {
		case <-p.done:
			return
		case <-pacer.stop:
			return
		default:
		}
		select {
		case <-p.done:
			return
		case <-pacer.stop:
			return
		case item := <-pacer.ch:
			pacer.setProcessing(true)
			p.dispatchChunkPCMRealtime(item.sourceID, item.displayName, item.pcm)
			p.chunkUploadPCMBytes.Add(-int64(len(item.pcm)))
			pacer.setProcessing(false)
		}
	}
}

func (p *AudioPipelineService) stopAndDrainChunkUploadPacer(sourceID string, pacer *chunkUploadPacer) {
	pacer.mu.Lock()
	pacer.accepting = false
	var released int64
	for {
		select {
		case item := <-pacer.ch:
			released += int64(len(item.pcm))
		default:
			pacer.mu.Unlock()
			if released > 0 {
				p.chunkUploadPCMBytes.Add(-released)
			}
			p.chunkUploadMu.Lock()
			if current, ok := p.chunkUploadPacers[sourceID]; ok && current == pacer {
				delete(p.chunkUploadPacers, sourceID)
			}
			p.chunkUploadMu.Unlock()
			return
		}
	}
}

func (pacer *chunkUploadPacer) enqueue(ctx context.Context, done <-chan struct{}, item chunkUploadPCM) error {
	pacer.mu.Lock()
	defer pacer.mu.Unlock()
	if !pacer.accepting {
		return fmt.Errorf("%w: source is being retired", audiocore.ErrChunkUploadUnavailable)
	}
	select {
	case <-ctx.Done():
		return fmt.Errorf("%w: %w", audiocore.ErrChunkUploadUnavailable, ctx.Err())
	case <-done:
		return fmt.Errorf("%w: pipeline is stopping", audiocore.ErrChunkUploadUnavailable)
	case pacer.ch <- item:
		pacer.lastActivity = time.Now()
		return nil
	default:
		return fmt.Errorf("%w: source %s", audiocore.ErrChunkUploadQueueFull, item.sourceID)
	}
}

func (pacer *chunkUploadPacer) setProcessing(processing bool) {
	pacer.mu.Lock()
	pacer.processing = processing
	pacer.lastActivity = time.Now()
	pacer.mu.Unlock()
}

func (pacer *chunkUploadPacer) retireIfIdle(now time.Time) bool {
	pacer.mu.Lock()
	defer pacer.mu.Unlock()
	if !pacer.accepting || pacer.processing || len(pacer.ch) != 0 || now.Sub(pacer.lastActivity) < chunkUploadIdleTimeout {
		return false
	}
	pacer.accepting = false
	pacer.stopOnce.Do(func() { close(pacer.stop) })
	return true
}

// evictIdleChunkUploadSource frees one inactive dynamic source when the cap is
// reached. sourcesMu must be held by the caller so source teardown cannot race
// static-source reconciliation.
func (p *AudioPipelineService) evictIdleChunkUploadSource(now time.Time) bool {
	p.chunkUploadMu.Lock()
	var retiredID string
	for sourceID, pacer := range p.chunkUploadPacers {
		if pacer.retireIfIdle(now) {
			retiredID = sourceID
			delete(p.chunkUploadPacers, sourceID)
			break
		}
	}
	p.chunkUploadMu.Unlock()
	if retiredID == "" {
		return false
	}

	RemoveOverrunTrackers(retiredID)
	p.untrackSoundLevelConsumer(retiredID)
	if p.bufferMgr != nil {
		_ = p.bufferMgr.RemoveMonitor(retiredID)
	}
	if err := p.engine.RemoveSource(retiredID); err != nil {
		audiocore.GetLogger().Warn("failed to remove idle chunk upload source",
			logger.String("source_id", retiredID),
			logger.Error(err))
		return false
	}
	audiocore.GetLogger().Info("removed idle chunk upload source",
		logger.String("source_id", retiredID),
		logger.String("operation", "chunk_upload_idle_evict"))
	return true
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

// includeChunkUploadMonitorState adds active dynamic upload sources to a
// desired-state monitor update so reconciling static sources cannot remove
// their analysis monitors.
func (p *AudioPipelineService) includeChunkUploadMonitorState(sourceModelMap map[string][]string, sourceIDs []string) []string {
	seen := make(map[string]struct{}, len(sourceIDs))
	result := append([]string(nil), sourceIDs...)
	for _, sourceID := range sourceIDs {
		seen[sourceID] = struct{}{}
	}
	for _, source := range p.engine.Registry().List() {
		if source == nil || source.Type != audiocore.SourceTypeChunkUpload {
			continue
		}
		if p.chunkUploadModelsChanged(source.ID, chunkUploadModelIDs()) {
			p.reconfigureChunkUploadSource(source.ID, []string{source.ID}, map[string][]string{source.ID: chunkUploadModelIDs()})
		}
		sourceModelMap[source.ID] = chunkUploadModelIDs()
		if _, ok := seen[source.ID]; !ok {
			result = append(result, source.ID)
			seen[source.ID] = struct{}{}
		}
	}
	return result
}

func decodeChunkUploadWAV(ctx context.Context, ffmpegPath string, wav []byte, maxPCMBytes int) ([]byte, error) {
	if err := ffmpeg.ValidateFFmpegPath(ffmpegPath); err != nil {
		return nil, fmt.Errorf("%w: %w", audiocore.ErrChunkUploadUnavailable, err)
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
	stdout := newLimitedPCMBuffer(maxPCMBytes)
	var stderr bytes.Buffer
	cmd.Stdout = stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if stdout.exceeded {
			return nil, fmt.Errorf("%w: maximum PCM size is %d bytes", audiocore.ErrChunkUploadTooLong, maxPCMBytes)
		}
		return nil, fmt.Errorf("%w: decode uploaded WAV: %w: %s", audiocore.ErrChunkUploadInvalidAudio, err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

type limitedPCMBuffer struct {
	bytes.Buffer
	limit    int
	exceeded bool
}

func newLimitedPCMBuffer(limit int) *limitedPCMBuffer {
	return &limitedPCMBuffer{limit: limit}
}

func (b *limitedPCMBuffer) Write(p []byte) (int, error) {
	if b.limit <= 0 {
		return b.Buffer.Write(p)
	}
	remaining := b.limit - b.Len()
	if remaining <= 0 {
		b.exceeded = true
		return 0, fmt.Errorf("decoded PCM limit exceeded")
	}
	if len(p) > remaining {
		b.exceeded = true
		written, _ := b.Buffer.Write(p[:remaining])
		return written, io.ErrShortWrite
	}
	return b.Buffer.Write(p)
}

func chunkUploadSourceCount(sources []*audiocore.AudioSource) int {
	count := 0
	for _, source := range sources {
		if source != nil && source.Type == audiocore.SourceTypeChunkUpload {
			count++
		}
	}
	return count
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
