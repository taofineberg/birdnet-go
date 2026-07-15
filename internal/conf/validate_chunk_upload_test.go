package conf

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateChunkUploadSettings(t *testing.T) {
	t.Parallel()

	valid := ChunkUploadSettings{
		Enabled: true, Token: "secret", Path: "chunks/inbox", Save: true,
		MaxBytes: 5 * 1024 * 1024, MaxSeconds: 15,
	}
	require.NoError(t, validateChunkUploadSettings(&valid))

	tests := []struct {
		name   string
		mutate func(*ChunkUploadSettings)
		match  string
	}{
		{name: "blank token", mutate: func(s *ChunkUploadSettings) { s.Token = " " }, match: "token"},
		{name: "small body limit", mutate: func(s *ChunkUploadSettings) { s.MaxBytes = 1 }, match: "maxBytes"},
		{name: "long duration", mutate: func(s *ChunkUploadSettings) { s.MaxSeconds = 301 }, match: "maxSeconds"},
		{name: "blank save path", mutate: func(s *ChunkUploadSettings) { s.Path = " " }, match: "path"},
		{name: "traversing save path", mutate: func(s *ChunkUploadSettings) { s.Path = "../outside" }, match: "path traversal"},
		{name: "null byte in save path", mutate: func(s *ChunkUploadSettings) { s.Path = "chunks\x00outside" }, match: "null bytes"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			settings := valid
			test.mutate(&settings)
			err := validateChunkUploadSettings(&settings)
			require.Error(t, err)
			assert.ErrorContains(t, err, test.match)
		})
	}
}

func TestValidateChunkUploadSettingsDisabledAllowsZeroValues(t *testing.T) {
	t.Parallel()
	require.NoError(t, validateChunkUploadSettings(&ChunkUploadSettings{}))
}
