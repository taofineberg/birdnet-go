package analysis

import (
	"testing"

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
	if len(got) != len(want) {
		t.Fatalf("chunkUploadModelIDs() length = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("chunkUploadModelIDs()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestChunkUploadModelIDsEmptyUsesPrimaryFallback(t *testing.T) {
	previous := conf.GetSettings()
	t.Cleanup(func() {
		conftest.SetTestSettings(previous)
	})

	settings := conftest.GetTestSettings()
	settings.Realtime.Audio.ChunkUpload.Models = nil
	conftest.SetTestSettings(settings)

	if got := chunkUploadModelIDs(); got != nil {
		t.Fatalf("chunkUploadModelIDs() = %v, want nil fallback", got)
	}
}

func TestChunkUploadModelIDsBlankUsesPrimaryFallback(t *testing.T) {
	previous := conf.GetSettings()
	t.Cleanup(func() {
		conftest.SetTestSettings(previous)
	})

	settings := conftest.GetTestSettings()
	settings.Realtime.Audio.ChunkUpload.Models = []string{"", "   "}
	conftest.SetTestSettings(settings)

	if got := chunkUploadModelIDs(); got != nil {
		t.Fatalf("chunkUploadModelIDs() = %v, want nil fallback", got)
	}
}
