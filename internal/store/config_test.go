package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetConfigDefaults(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	cfg, err := s.GetConfig(ctx)
	require.NoError(t, err)

	assert.NotNil(t, cfg)
	assert.Equal(t, "2", cfg["backoff-base"])
	assert.Equal(t, "3", cfg["max-retries"])
}

func TestSetConfigSuccess(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	err := s.SetConfig(ctx, "  max-retries  ", "10")
	require.NoError(t, err)

	cfg, err := s.GetConfig(ctx)
	require.NoError(t, err)
	assert.Equal(t, "10", cfg["max-retries"])
}

func TestSetConfigUnknownKey(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	err := s.SetConfig(ctx, "unknown_key", "10")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `unknown config key "unknown_key"`)

	cfg, err := s.GetConfig(ctx)
	require.NoError(t, err)
	_, exists := cfg["unknown_key"]
	assert.False(t, exists)
}

func TestSetConfigEmptyKey(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	err := s.SetConfig(ctx, "   ", "10")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "config key is required")
}

func TestSetConfigRejectsInvalidValue(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{"max retries non integer", "max-retries", "abc"},
		{"max retries spaces", "max-retries", " 10 "},
		{"max retries negative", "max-retries", "-1"},
		{"backoff base zero", "backoff-base", "0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := openTestStore(t)
			err := s.SetConfig(context.Background(), tt.key, tt.value)
			require.Error(t, err)
		})
	}
}

func TestConfigTableRejectsInvalidDirectWrites(t *testing.T) {
	s := openTestStore(t)

	_, err := s.db.Exec("UPDATE config SET value = ? WHERE key = ?", "bad", "max-retries")
	require.Error(t, err)

	_, err = s.db.Exec("INSERT INTO config (key, value, updated_at) VALUES (?, ?, ?)", "unknown", "1", int64(1))
	require.Error(t, err)
}
