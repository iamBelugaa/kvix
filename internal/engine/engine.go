// Package engine provides the core database engine implementation for the Ignite storage system.
package engine

import (
	"context"
	stdErrors "errors"
	"fmt"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/iamNilotpal/ignite/internal/compaction"
	"github.com/iamNilotpal/ignite/internal/index"
	"github.com/iamNilotpal/ignite/internal/storage"
	"github.com/iamNilotpal/ignite/pkg/errors"
	"github.com/iamNilotpal/ignite/pkg/options"
)

var (
	ErrEngineClosed = stdErrors.New("operation failed: cannot access closed engine")
)

// Engine represents the main database engine that coordinates all subsystems.
type Engine struct {
	closed     atomic.Bool
	index      *index.Index
	storage    *storage.Storage
	options    *options.Options
	log        *zap.SugaredLogger
	compaction *compaction.Compaction
}

// New creates and initializes a new Engine instance with the provided configuration.
func New(ctx context.Context, log *zap.SugaredLogger, options *options.Options) (*Engine, error) {
	log.Infow("Initializing engine with multi-segment support")

	storage, err := storage.New(ctx, log, options)
	if err != nil {
		return nil, err
	}

	index, err := index.New(ctx, log, options.DataDir)
	if err != nil {
		return nil, err
	}

	compaction := compaction.New()

	return &Engine{
		log:        log,
		options:    options,
		index:      index,
		storage:    storage,
		compaction: compaction,
	}, nil
}

// Set stores a key-value pair in the storage system and creates the corresponding index entry.
func (e *Engine) Set(ctx context.Context, key, value []byte) error {
	if e.closed.Load() {
		return ErrEngineClosed
	}

	e.log.Infow("Starting Set operation", "keyLength", len(key), "valueLength", len(value))

	_, offset, err := e.storage.Set(ctx, key, value)
	if err != nil {
		return err
	}

	e.index.Set(string(key), &index.RecordPointer{
		ExpiresAt:        0,
		Offset:           offset,
		SegmentID:        e.storage.SegmentID(),
		SegmentTimestamp: e.storage.SegmentTimestamp(),
	})

	e.log.Infow("Set operation completed successfully", "key", string(key))
	return nil
}

// SetX stores a key-value pair with a time-to-live (TTL) expiration.
func (e *Engine) SetX(ctx context.Context, key, value []byte, ttl time.Duration) (*storage.Record, error) {
	if e.closed.Load() {
		return nil, ErrEngineClosed
	}

	e.log.Infow("Starting SetX operation", "ttl", ttl, "keyLength", len(key), "valueLength", len(value))

	record, offset, err := e.storage.Set(ctx, key, value)
	if err != nil {
		return nil, err
	}

	e.index.Set(string(key), &index.RecordPointer{
		Offset:           offset,
		SegmentID:        e.storage.SegmentID(),
		SegmentTimestamp: e.storage.SegmentTimestamp(),
		ExpiresAt:        time.Now().Add(ttl).UnixNano(),
	})

	e.log.Infow("SetX operation completed successfully", "key", string(key))
	return record, nil
}

// Get retrieves a record from the storage system using the index for location information.
func (e *Engine) Get(ctx context.Context, key []byte) (*storage.Record, error) {
	if e.closed.Load() {
		return nil, ErrEngineClosed
	}

	e.log.Infow("Starting Get operation", "key", string(key))

	pointer, ok := e.index.Get(string(key))
	if !ok {
		return nil, errors.NewIndexError(
			nil, errors.ErrIndexKeyNotFound, "Key not found in index",
		).WithKey(string(key))
	}

	e.log.Infow(
		"Index lookup successful",
		"key", string(key),
		"offset", pointer.Offset,
		"segmentID", pointer.SegmentID,
		"segmentTimestamp", pointer.SegmentTimestamp,
	)

	record, err := e.storage.Get(ctx, key, pointer.SegmentID, pointer.SegmentTimestamp, pointer.Offset)
	if err != nil {
		return nil, err
	}

	e.log.Infow("Get operation completed successfully", "key", string(key))
	return record, nil
}

// Delete removes a record from both the storage system and the index.
func (e *Engine) Delete(ctx context.Context, key []byte) (bool, error) {
	if e.closed.Load() {
		return false, ErrEngineClosed
	}

	e.log.Infow("Starting Delete operation", "key", string(key))

	deleted := e.index.Delete(string(key))
	if deleted {
		e.log.Infow("Delete operation completed successfully", "key", string(key))
	} else {
		e.log.Infow("Delete operation completed - key not found", "key", string(key))
	}

	return deleted, nil
}

// Exists checks if a key exists in the index without retrieving the full record.
func (e *Engine) Exists(ctx context.Context, key []byte) (bool, error) {
	if e.closed.Load() {
		return false, ErrEngineClosed
	}

	e.log.Infow("Checking key existence", "key", string(key))
	_, exists := e.index.Get(string(key))

	e.log.Infow("Key existence check completed", "key", string(key), "exists", exists)
	return exists, nil
}

// CleanupExpired removes all expired entries from the index.
func (e *Engine) CleanupExpired(ctx context.Context) error {
	if e.closed.Load() {
		return ErrEngineClosed
	}

	e.log.Infow("Starting expired entry cleanup")
	e.index.CleanupExpired()

	e.log.Infow("Expired entry cleanup completed")
	return nil
}

// Close gracefully shuts down the engine and releases all associated resources.
func (e *Engine) Close() error {
	if !e.closed.CompareAndSwap(false, true) {
		return ErrEngineClosed
	}

	var errors []error
	e.log.Infow("Closing engine with comprehensive resource cleanup")

	if err := e.index.Close(); err != nil {
		e.log.Errorw("Failed to close index subsystem", "error", err)
		errors = append(errors, fmt.Errorf("failed to close index: %w", err))
	}

	if err := e.storage.Close(); err != nil {
		e.log.Errorw("Failed to close storage subsystem", "error", err)
		errors = append(errors, fmt.Errorf("failed to close storage: %w", err))
	}

	if len(errors) > 0 {
		return fmt.Errorf("engine close encountered %d errors: %v", len(errors), errors)
	}

	e.log.Infow("Engine closed successfully")
	return nil
}
