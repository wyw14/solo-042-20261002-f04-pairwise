package api

import (
	"log/slog"
	"net/http"

	"github.com/google/uuid"
)

type responseRecorder struct {
	http.ResponseWriter
	status int
}

func (r *responseRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

// Flush 透传给底层 ResponseWriter，保证流式处理器（NDJSON/SSE）
// 经过本包装器后仍能立即刷出数据。
func (r *responseRecorder) Flush() {
	if flusher, ok := r.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		id := request.Header.Get("X-Request-ID")
		if id == "" {
			id = uuid.NewString()
		}
		writer.Header().Set("X-Request-ID", id)
		next.ServeHTTP(writer, request)
	})
}

func recoverPanic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		recorder := &responseRecorder{ResponseWriter: writer, status: http.StatusOK}
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.Error("http handler panic", "path", request.URL.Path, "error", recovered)
				writeError(recorder, http.StatusInternalServerError, "internal error")
			}
		}()
		next.ServeHTTP(recorder, request)
	})
}
