package api

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/oxia-io/okk/coordinator/internal/config"
	"github.com/oxia-io/okk/coordinator/internal/task"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Server struct {
	manager *task.Manager
	mux     *http.ServeMux
}

func NewServer(manager *task.Manager) *Server {
	s := &Server{
		manager: manager,
		mux:     http.NewServeMux(),
	}
	s.registerRoutes()
	return s
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("POST /testcases", s.createTestCase)
	s.mux.HandleFunc("GET /testcases", s.listTestCases)
	s.mux.HandleFunc("GET /testcases/{name}", s.getTestCase)
	s.mux.HandleFunc("DELETE /testcases/{name}", s.deleteTestCase)
	s.mux.HandleFunc("GET /healthz", s.healthz)
	s.mux.Handle("GET /metrics", promhttp.Handler())
}

func (s *Server) createTestCase(w http.ResponseWriter, r *http.Request) {
	var tc config.TestCaseConfig
	if err := json.NewDecoder(r.Body).Decode(&tc); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if tc.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	if tc.Type == "" {
		writeError(w, http.StatusBadRequest, "type is required")
		return
	}
	if tc.WorkerEndpoint == "" {
		writeError(w, http.StatusBadRequest, "workerEndpoint is required")
		return
	}

	if err := s.manager.CreateTask(&tc); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}

	slog.Info("Testcase created", "name", tc.Name, "type", tc.Type)
	writeJSON(w, http.StatusCreated, map[string]string{"status": "created", "name": tc.Name})
}

func (s *Server) listTestCases(w http.ResponseWriter, _ *http.Request) {
	statuses := s.manager.ListStatuses()
	writeJSON(w, http.StatusOK, map[string]any{"testcases": statuses})
}

func (s *Server) getTestCase(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	status, found := s.manager.GetStatus(name)
	if !found {
		writeError(w, http.StatusNotFound, "testcase not found: "+name)
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) deleteTestCase(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := s.manager.DeleteTask(name); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	slog.Info("Testcase deleted", "name", name)
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted", "name": name})
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "healthy"})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, message string) {
	writeJSON(w, code, map[string]string{"error": message})
}
