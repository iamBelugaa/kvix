// Package ignite provides a key-value data store designed for fast read and write operations.
package ignite

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/iamNilotpal/ignite/internal/engine"
	"github.com/iamNilotpal/ignite/internal/storage"
	"github.com/iamNilotpal/ignite/pkg/logger"
	"github.com/iamNilotpal/ignite/pkg/options"
)

// Instance represents a complete Ignite key-value database instance.
type Instance struct {
	mu      sync.RWMutex
	engine  *engine.Engine
	options *options.Options
}

// NewInstance creates and initializes a new Ignite database instance with the specified configuration.
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
		return nil, fmt.Errorf("failed to initialize database engine: %w", err)
	}

	log.Infow(
		"Ignite database instance initialized successfully",
		"service", service,
		"dataDir", defaultOpts.DataDir,
		"maxSegmentSize", defaultOpts.SegmentOptions.Size,
	)

	return &Instance{engine: eng, options: &defaultOpts}, nil
}

// Set stores a key-value pair in the database with immediate durability.
func (i *Instance) Set(context context.Context, key []byte, value []byte) error {
	if err := isValidKey(key); err != nil {
		return fmt.Errorf("invalid key: %w", err)
	}

	if err := isValidValue(value); err != nil {
		return fmt.Errorf("invalid value: %w", err)
	}

	i.mu.Lock()
	defer i.mu.Unlock()
	return i.engine.Set(context, key, value)
}

// SetX stores a key-value pair with automatic expiration after the specified duration.
func (i *Instance) SetX(context context.Context, key []byte, value []byte, ttl time.Duration) error {
	if err := isValidKey(key); err != nil {
		return fmt.Errorf("invalid key: %w", err)
	}

	if err := isValidValue(value); err != nil {
		return fmt.Errorf("invalid value: %w", err)
	}

	if ttl <= 0 {
		return fmt.Errorf("TTL must be positive, got %v", ttl)
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	_, err := i.engine.SetX(context, key, value, ttl)
	return err
}

// Get retrieves the value associated with the given key, if it exists and hasn't expired.
func (i *Instance) Get(context context.Context, key []byte) (*storage.Record, error) {
	if err := isValidKey(key); err != nil {
		return nil, fmt.Errorf("invalid key: %w", err)
	}

	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.engine.Get(context, key)
}

// Exists checks whether a key exists in the database without retrieving the full record.
func (i *Instance) Exists(context context.Context, key []byte) (bool, error) {
	if err := isValidKey(key); err != nil {
		return false, fmt.Errorf("invalid key: %w", err)
	}

	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.engine.Exists(context, key)
}

// Delete removes a key-value pair from the database.
func (i *Instance) Delete(context context.Context, key []byte) (bool, error) {
	if err := isValidKey(key); err != nil {
		return false, fmt.Errorf("invalid key: %w", err)
	}

	i.mu.Lock()
	defer i.mu.Unlock()
	return i.engine.Delete(context, key)
}

// Close gracefully shuts down the Ignite database instance.
func (i *Instance) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.engine.Close()
}
