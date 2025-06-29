package engine

import (
	"context"
	stdErrors "errors"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/iamBelugaa/kvix/internal/index"
	"github.com/iamBelugaa/kvix/internal/storage"
	"github.com/iamBelugaa/kvix/pkg/errors"
	"github.com/iamBelugaa/kvix/pkg/options"
)

var (
	ErrEngineClosed = stdErrors.New("operation failed: cannot access closed engine")
)

type Engine struct {
	closed  atomic.Bool
	index   *index.Index
	storage *storage.Storage
	options *options.Options
}

func New(ctx context.Context, log *zap.SugaredLogger, options *options.Options) (*Engine, error) {
	storage, err := storage.New(ctx, log, options)
	if err != nil {
		return nil, err
	}

	index, err := index.New(options.DataDir)
	if err != nil {
		return nil, err
	}

	return &Engine{
		options: options,
		index:   index,
		storage: storage,
	}, nil
}

func (e *Engine) Set(ctx context.Context, key, value []byte) error {
	if e.closed.Load() {
		return ErrEngineClosed
	}

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

	return nil
}

func (e *Engine) SetX(ctx context.Context, key, value []byte, ttl time.Duration) (*storage.Record, error) {
	if e.closed.Load() {
		return nil, ErrEngineClosed
	}

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

	return record, nil
}

func (e *Engine) Get(ctx context.Context, key []byte) (*storage.Record, error) {
	if e.closed.Load() {
		return nil, ErrEngineClosed
	}

	pointer, ok := e.index.Get(string(key))
	if !ok {
		return nil, errors.NewIndexError(
			nil, errors.ErrIndexKeyNotFound, "Key not found in index",
		).
			WithKey(string(key))
	}

	record, err := e.storage.Get(ctx, key, pointer.SegmentID, pointer.SegmentTimestamp, pointer.Offset)
	if err != nil {
		return nil, err
	}

	return record, nil
}

func (e *Engine) Delete(ctx context.Context, key []byte) (bool, error) {
	if e.closed.Load() {
		return false, ErrEngineClosed
	}
	return e.index.Delete(string(key)), nil
}

func (e *Engine) Exists(ctx context.Context, key []byte) (bool, error) {
	if e.closed.Load() {
		return false, ErrEngineClosed
	}
	_, exists := e.index.Get(string(key))
	return exists, nil
}

func (e *Engine) CleanupExpired(ctx context.Context) error {
	if e.closed.Load() {
		return ErrEngineClosed
	}
	e.index.CleanupExpired()
	return nil
}

func (e *Engine) Close() error {
	if !e.closed.CompareAndSwap(false, true) {
		return ErrEngineClosed
	}

	if err := e.index.Close(); err != nil {
		return err
	}

	if err := e.storage.Close(); err != nil {
		return err
	}

	return nil
}
