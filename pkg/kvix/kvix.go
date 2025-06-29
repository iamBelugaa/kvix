package kvix

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/iamBelugaa/kvix/internal/engine"
	"github.com/iamBelugaa/kvix/internal/storage"
	"github.com/iamBelugaa/kvix/pkg/errors"
	"github.com/iamBelugaa/kvix/pkg/logger"
	"github.com/iamBelugaa/kvix/pkg/options"
	"go.uber.org/zap"
)

type Instance struct {
	mu      sync.RWMutex
	engine  *engine.Engine
	options *options.Options
	log     *zap.SugaredLogger
}

func NewInstance(context context.Context, service string, opts ...options.OptionFunc) (*Instance, error) {
	log := logger.New(service)

	defaultOpts := options.DefaultOptions()
	if len(opts) > 0 {
		for _, opt := range opts {
			opt(&defaultOpts)
		}
	}

	eng, err := engine.New(context, log, &defaultOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize kvix: %w", err)
	}

	log.Infow(
		"Kvix database instance initialized successfully",
		"service", service,
		"dataDir", defaultOpts.DataDir,
		"maxSegmentSize", defaultOpts.SegmentOptions.Size,
	)

	return &Instance{engine: eng, options: &defaultOpts, log: log}, nil
}

func (i *Instance) Set(context context.Context, key []byte, value []byte) error {
	i.log.Infow("Set request received", "key", string(key))

	if err := isValidKey(key); err != nil {
		return err
	}

	if err := isValidValue(value); err != nil {
		return err
	}

	i.mu.Lock()
	defer i.mu.Unlock()
	return i.engine.Set(context, key, value)
}

func (i *Instance) SetX(context context.Context, key []byte, value []byte, ttl time.Duration) error {
	i.log.Infow("SetX request received", "key", string(key))

	if err := isValidKey(key); err != nil {
		return err
	}

	if err := isValidValue(value); err != nil {
		return err
	}

	if ttl <= 0 {
		return errors.NewValidationError(
			nil, errors.ErrValidationInvalidData, fmt.Sprintf("ttl must be positive, got %v", ttl),
		)
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	_, err := i.engine.SetX(context, key, value, ttl)
	return err
}

func (i *Instance) Get(context context.Context, key []byte) (*storage.Record, error) {
	i.log.Infow("Get request received", "key", string(key))

	if err := isValidKey(key); err != nil {
		return nil, err
	}

	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.engine.Get(context, key)
}

func (i *Instance) Exists(context context.Context, key []byte) (bool, error) {
	i.log.Infow("Exists request received", "key", string(key))

	if err := isValidKey(key); err != nil {
		return false, err
	}

	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.engine.Exists(context, key)
}

func (i *Instance) Delete(context context.Context, key []byte) (bool, error) {
	i.log.Infow("Delete request received", "key", string(key))

	if err := isValidKey(key); err != nil {
		return false, err
	}

	i.mu.Lock()
	defer i.mu.Unlock()
	return i.engine.Delete(context, key)
}

func (i *Instance) Close() error {
	i.log.Infow("Close request received")

	i.mu.Lock()
	defer i.mu.Unlock()
	return i.engine.Close()
}
