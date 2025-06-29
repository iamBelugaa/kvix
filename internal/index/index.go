package index

func New(dataDir string) (*Index, error) {
	return &Index{
		dataDir:       dataDir,
		recordPointer: make(map[string]*RecordPointer),
	}, nil
}

func (idx *Index) Set(key string, pointer *RecordPointer) {
	idx.mu.Lock()
	idx.recordPointer[key] = pointer
	idx.mu.Unlock()
}

func (idx *Index) Get(key string) (*RecordPointer, bool) {
	pointer, ok := idx.recordPointer[key]
	if !ok {
		return nil, false
	}

	if pointer.IsExpired() {
		idx.mu.Lock()
		delete(idx.recordPointer, key)
		idx.mu.Unlock()
		return nil, false
	}

	return pointer, true
}

func (idx *Index) Delete(key string) bool {
	_, ok := idx.recordPointer[key]
	if !ok {
		return false
	}

	idx.mu.Lock()
	delete(idx.recordPointer, key)
	idx.mu.Unlock()

	return true
}

func (idx *Index) CleanupExpired() {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	for key, rp := range idx.recordPointer {
		if rp.IsExpired() {
			delete(idx.recordPointer, key)
		}
	}
}

func (idx *Index) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	clear(idx.recordPointer)
	idx.recordPointer = nil

	return nil
}
