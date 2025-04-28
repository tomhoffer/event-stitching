package logger

import (
	"log/slog"
	"os"
)

func NewLogger(level slog.Level) *slog.Logger {
	opts := &slog.HandlerOptions{
		Level: level,
	}
	handler := slog.NewTextHandler(os.Stdout, opts)
	return slog.New(handler)
}
