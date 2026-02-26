package harestore

import "context"

// Logger defines the interface for structured logging
type Logger interface {
	// DebugwCtx logs a message with structured key-value pairs at the Debug level.
	DebugwCtx(ctx context.Context, msg string, kvs ...any)
	// InfowCtx logs a message with structured key-value pairs at the Info level.
	InfowCtx(ctx context.Context, msg string, kvs ...any)
	// WarnwCtx logs a message with structured key-value pairs at the Warn level.
	WarnwCtx(ctx context.Context, msg string, kvs ...any)
	// ErrorwCtx logs a message with structured key-value pairs at the Error level.
	ErrorwCtx(ctx context.Context, msg string, kvs ...any)
	// FatalwCtx logs a message with structured key-value pairs at the Fatal level
	FatalwCtx(ctx context.Context, msg string, kvs ...any)
}

// nopLogger is a no-operation implementation of the Logger interface.
type nopLogger struct{}

func (*nopLogger) DebugwCtx(context.Context, string, ...any) {}
func (*nopLogger) InfowCtx(context.Context, string, ...any)  {}
func (*nopLogger) WarnwCtx(context.Context, string, ...any)  {}
func (*nopLogger) ErrorwCtx(context.Context, string, ...any) {}
func (*nopLogger) FatalwCtx(context.Context, string, ...any) {}
