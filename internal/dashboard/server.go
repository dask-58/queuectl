package dashboard

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/dask-58/queuectl/internal/store"
)

// Serve starts the dashboard HTTP server and blocks until the context is canceled.
func Serve(ctx context.Context, addr string, s *store.Store) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", handleDashboard(s))
	mux.HandleFunc("/api/status", handleStatus(s))
	mux.HandleFunc("/api/jobs", handleJobs(s))

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- fmt.Errorf("listen and serve: %w", err)
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("server shutdown: %w", err)
		}
		return nil
	case err := <-errCh:
		return err
	}
}
