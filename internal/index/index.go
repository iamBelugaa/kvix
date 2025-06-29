// Package index provides the in-memory hash table implementation for the ignite key-value store.
// The index enables O(1) key lookups through an in-memory hash table while keeping
// storage overhead minimal.
package index

import (
	"context"

	"go.uber.org/zap"
)

// New creates and initializes a new Index instance.
func New(ctx context.Context, log *zap.SugaredLogger, dataDir string) (*Index, error) {
	return &Index{
		log:           log,
		dataDir:       dataDir,
		recordPointer: make(map[string]*RecordPointer, 2046),
	}, nil
}

// Set stores a RecordPointer for the given key.
func (idx *Index) Set(key string, pointer *RecordPointer) {
	idx.log.Infow("Setting index entry", "key", key, "pointer", pointer)

	idx.mu.Lock()
	idx.recordPointer[key] = pointer
	idx.mu.Unlock()

	idx.log.Infow("Index entry set successfully", "key", key)
}

// Get retrieves a RecordPointer for the given key, checking for expiration.
func (idx *Index) Get(key string) (*RecordPointer, bool) {
	idx.log.Infow("Getting index entry", "key", key)

	pointer, ok := idx.recordPointer[key]
	if !ok {
		idx.log.Infow("Index entry not found", "key", key)
		return nil, false
	}

	if pointer.IsExpired() {
		idx.log.Infow("Index entry expired, removing automatically", "key", key, "record", pointer)
		{
			idx.mu.Lock()
			delete(idx.recordPointer, key)
			idx.mu.Unlock()
		}
		return nil, false
	}

	idx.log.Infow("Index entry retrieved successfully", "key", key, "pointer", pointer)
	return pointer, true
}

// Delete removes a RecordPointer for the given key.
func (idx *Index) Delete(key string) bool {
	idx.log.Infow("Deleting index entry", "key", key)

	_, ok := idx.recordPointer[key]
	if !ok {
		idx.log.Infow("Index entry not found for deletion", "key", key)
		return false
	}

	idx.mu.Lock()
	delete(idx.recordPointer, key)
	idx.mu.Unlock()

	idx.log.Infow("Index entry deleted successfully", "key", key)
	return true
}

// CleanupExpired removes all expired entries from the index in a batch operation.
func (idx *Index) CleanupExpired() {
	idx.log.Infow("Starting expired entry cleanup")

	idx.mu.Lock()
	defer idx.mu.Unlock()

	for key, rp := range idx.recordPointer {
		if rp.IsExpired() {
			delete(idx.recordPointer, key)
		}
	}

	idx.log.Infow("Expired entry cleanup completed", "remainingCount", len(idx.recordPointer))
}

// Close gracefully shuts down the Index, cleaning up resources.
func (idx *Index) Close() error {
	idx.log.Infow("Closing index system")

	idx.mu.Lock()
	defer idx.mu.Unlock()

	finalCount := len(idx.recordPointer)
	idx.log.Infow("Final index statistics before close", "totalEntries", finalCount)

	clear(idx.recordPointer)
	idx.recordPointer = nil

	idx.log.Infow("Index system closed successfully", "entriesCleared", finalCount)
	return nil
}
