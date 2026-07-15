package analysis

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tphakala/birdnet-go/internal/audiocore"
	"github.com/tphakala/birdnet-go/internal/conf"
	"github.com/tphakala/birdnet-go/internal/conf/conftest"
)

func TestChunkUploadModelIDs(t *testing.T) {
	previous := conf.GetSettings()
	t.Cleanup(func() {
		conftest.SetTestSettings(previous)
	})

	settings := conftest.GetTestSettings()
	settings.Realtime.Audio.ChunkUpload.Models = []string{" birdnet ", "", "perch_v2"}
	conftest.SetTestSettings(settings)

	got := chunkUploadModelIDs()
	want := []string{"birdnet", "perch_v2"}
	assert.Equal(t, want, got)
}

func TestChunkUploadModelIDsEmptyUsesPrimaryFallback(t *testing.T) {
	previous := conf.GetSettings()
	t.Cleanup(func() {
		conftest.SetTestSettings(previous)
	})

	settings := conftest.GetTestSettings()
	settings.Realtime.Audio.ChunkUpload.Models = nil
	conftest.SetTestSettings(settings)

	assert.Nil(t, chunkUploadModelIDs())
}

func TestChunkUploadModelIDsBlankUsesPrimaryFallback(t *testing.T) {
	previous := conf.GetSettings()
	t.Cleanup(func() {
		conftest.SetTestSettings(previous)
	})

	settings := conftest.GetTestSettings()
	settings.Realtime.Audio.ChunkUpload.Models = []string{"", "   "}
	conftest.SetTestSettings(settings)

	assert.Nil(t, chunkUploadModelIDs())
}

func TestLimitedPCMBuffer(t *testing.T) {
	t.Parallel()

	buffer := newLimitedPCMBuffer(4)
	// WriteString would bypass limitedPCMBuffer.Write through the embedded buffer.
	written, err := buffer.Write([]byte("abcdef")) //nolint:gocritic
	require.ErrorIs(t, err, io.ErrShortWrite)
	assert.Equal(t, 4, written)
	assert.Equal(t, []byte("abcd"), buffer.Bytes())
	assert.True(t, buffer.exceeded)
}

func TestChunkUploadSourceCount(t *testing.T) {
	t.Parallel()

	sources := []*audiocore.AudioSource{
		{Type: audiocore.SourceTypeChunkUpload},
		{Type: audiocore.SourceTypeRTSP},
		nil,
		{Type: audiocore.SourceTypeChunkUpload},
	}
	assert.Equal(t, 2, chunkUploadSourceCount(sources))
}

func TestChunkUploadPacerRejectsFullQueue(t *testing.T) {
	t.Parallel()

	pacer := &chunkUploadPacer{
		ch:           make(chan chunkUploadPCM, 1),
		lastActivity: time.Now(),
		accepting:    true,
		stop:         make(chan struct{}),
	}
	pacer.ch <- chunkUploadPCM{sourceID: "chunk_existing"}
	err := pacer.enqueue(t.Context(), make(chan struct{}), chunkUploadPCM{sourceID: "chunk_new"})
	require.ErrorIs(t, err, audiocore.ErrChunkUploadQueueFull)
}

func TestChunkUploadPacerRetiresOnlyWhenIdle(t *testing.T) {
	t.Parallel()

	pacer := &chunkUploadPacer{
		ch:           make(chan chunkUploadPCM, 1),
		lastActivity: time.Now().Add(-chunkUploadIdleTimeout - time.Second),
		accepting:    true,
		stop:         make(chan struct{}),
	}
	assert.True(t, pacer.retireIfIdle(time.Now()))
	assert.False(t, pacer.retireIfIdle(time.Now()))
	select {
	case <-pacer.stop:
	default:
		t.Fatal("retired pacer stop channel was not closed")
	}
}
