package dashboard

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dask-58/queuectl/internal/store"
)

func testDBPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "queuectl.db")
}

func openTestStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(testDBPath(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

func TestHandleDashboard(t *testing.T) {
	s := openTestStore(t)
	handler := handleDashboard(s)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	handler(w, req)

	res := w.Result()
	defer res.Body.Close()

	assert.Equal(t, http.StatusOK, res.StatusCode)
	assert.Contains(t, w.Body.String(), "QueueCTL Dashboard")
}

func TestHandleStatus(t *testing.T) {
	s := openTestStore(t)
	handler := handleStatus(s)

	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	w := httptest.NewRecorder()

	handler(w, req)

	res := w.Result()
	defer res.Body.Close()

	assert.Equal(t, http.StatusOK, res.StatusCode)
	assert.Equal(t, "application/json", res.Header.Get("Content-Type"))

	var status map[string]int
	err := json.NewDecoder(res.Body).Decode(&status)
	require.NoError(t, err)

	assert.Equal(t, 0, status["pending"])
}

func TestHandleJobs(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	_, err := s.Enqueue(ctx, "job-1", "cmd")
	require.NoError(t, err)

	handler := handleJobs(s)

	req := httptest.NewRequest(http.MethodGet, "/api/jobs", nil)
	w := httptest.NewRecorder()

	handler(w, req)

	res := w.Result()
	defer res.Body.Close()

	assert.Equal(t, http.StatusOK, res.StatusCode)
	assert.Equal(t, "application/json", res.Header.Get("Content-Type"))

	var jobs []store.Job
	err = json.NewDecoder(res.Body).Decode(&jobs)
	require.NoError(t, err)

	require.Len(t, jobs, 1)
	assert.Equal(t, "job-1", jobs[0].ID)
}
