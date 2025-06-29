// Package logger provides a centralized and configurable logging utility
// based on the `go.uber.org/zap` library.
package logger

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// It sets up a production-ready logger with JSON encoding, ISO8601 timestamps,
// and includes service name and process ID as initial fields.
func New(service string, outputPaths ...string) *zap.SugaredLogger {
	encoderCfg := zap.NewProductionEncoderConfig()

	encoderCfg.TimeKey = "timestamp"
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	config := zap.Config{
		Development:       false,
		DisableCaller:     false,
		DisableStacktrace: false,
		Sampling:          nil,
		Encoding:          "json",
		EncoderConfig:     encoderCfg,
		OutputPaths:       []string{"stderr"},
		ErrorOutputPaths:  []string{"stderr"},
		Level:             zap.NewAtomicLevelAt(zap.InfoLevel),
		InitialFields:     map[string]any{"service": service, "pid": os.Getpid()},
	}

	if len(outputPaths) != 0 {
		config.OutputPaths = outputPaths
	}

	return zap.Must(config.Build()).Sugar()
}
