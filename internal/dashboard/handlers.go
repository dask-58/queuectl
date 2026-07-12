package dashboard

import (
	"encoding/json"
	"html/template"
	"net/http"

	"github.com/dask-58/queuectl/internal/store"
)

func handleDashboard(s *store.Store) http.HandlerFunc {
	tmpl, err := template.New("dashboard").Parse(dashboardHTML)

	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		if err != nil {
			http.Error(w, "Failed to load dashboard", http.StatusInternalServerError)
			return
		}

		ctx := r.Context()
		status, err := s.Status(ctx)
		if err != nil {
			http.Error(w, "Failed to load status", http.StatusInternalServerError)
			return
		}

		jobs, err := s.ListRecentJobs(ctx, 25)
		if err != nil {
			http.Error(w, "Failed to load recent jobs", http.StatusInternalServerError)
			return
		}

		data := struct {
			Status *store.Status
			Jobs   []store.Job
		}{
			Status: status,
			Jobs:   jobs,
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := tmpl.Execute(w, data); err != nil {
			// If headers are already written, we can't do much, but log/ignore.
			return
		}
	}
}

func handleStatus(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		status, err := s.Status(r.Context())
		if err != nil {
			respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}

		respondJSON(w, http.StatusOK, map[string]interface{}{
			"pending":    status.PendingJobs,
			"processing": status.ProcessingJobs,
			"completed":  status.CompletedJobs,
			"failed":     status.FailedJobs,
			"dead":       status.DeadJobs,
			"workers":    status.ActiveWorkers,
		})
	}
}

func handleJobs(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		jobs, err := s.ListRecentJobs(r.Context(), 25)
		if err != nil {
			respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		respondJSON(w, http.StatusOK, jobs)
	}
}

func respondJSON(w http.ResponseWriter, code int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(data)
}
