package logging

import (
	"context"
	"net/http"

	"github.com/justinas/alice"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// based originally on https://github.com/cantor-systems/zapctx/blob/master/zapctx.go

func Middleware(logger *zap.Logger) alice.Constructor {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := ToContext(r.Context(), logger)
			r = r.WithContext(ctx)
			next.ServeHTTP(w, r)
		})
	}
}

type loggerKeyType struct{}

var loggerKey = loggerKeyType{}

func ToContext(ctx context.Context, logger *zap.Logger) context.Context {
	return context.WithValue(ctx, loggerKey, logger)
}

// WithFields returns a new context derived from ctx
// that has a logger that always logs the given fields.
func WithFields(ctx context.Context, fields ...zapcore.Field) context.Context {
	return ToContext(ctx, FromContext(ctx).With(fields...))
}

func FromContext(ctx context.Context) *zap.Logger {
	if logger, _ := ctx.Value(loggerKey).(*zap.Logger); logger != nil {
		return logger
	}
	return zap.NewNop()
}

func Debug(ctx context.Context, msg string, fields ...zapcore.Field) {
	FromContext(ctx).Debug(msg, fields...)
}

func Info(ctx context.Context, msg string, fields ...zapcore.Field) {
	FromContext(ctx).Info(msg, fields...)
}

func Warn(ctx context.Context, msg string, fields ...zapcore.Field) {
	FromContext(ctx).Warn(msg, fields...)
}

func Error(ctx context.Context, msg string, fields ...zapcore.Field) {
	FromContext(ctx).Error(msg, fields...)
}
