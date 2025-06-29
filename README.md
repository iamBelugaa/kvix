# Kvix

A key-value storage engine inspired by Bitcask architecture, designed for
applications. Kvix combines an in-memory index with append-only file storage to
deliver predictable O(1) performance.

## Core Components

### Instance Management

The Instance type serves as the main entry point and manages the entire database
lifecycle.

```go
type Instance struct {
    options *options.Options  // Configuration parameters
    mu      sync.RWMutex      // Concurrent access protection
    engine  *engine.Engine    // Core database engine coordination
}
```

### Engine Coordination

The Engine acts as the central coordinator between the index, storage, and
compaction subsystems.

```go
type Engine struct {
    options    *options.Options
    log        *zap.SugaredLogger
    closed     atomic.Bool
    index      *index.Index
    storage    *storage.Storage
    compaction *compaction.Compaction
}
```

### Index Implementation

The index uses a memory-efficient design where each key maps to a RecordPointer
containing exactly the information needed to locate and retrieve data:

```go
type RecordPointer struct {
    ExpiresAt        int64   // 8 bytes: Unix nanoseconds for TTL
    Offset           int64   // 8 bytes: Exact position in segment file
    SegmentTimestamp int64   // 8 bytes: Creation time for filename reconstruction
    SegmentID        uint16  // 2 bytes: Segment identifier (0-65535)

    // 6 Bytes of padding added by Go for alignment = 32 bytes total
}
```

## Storage Engine Design

### Multi-Segment Architecture

The storage system implements sophisticated segment management with automatic
rotation when files reach configurable size limits.

**Segment File Structure:**

```
┌─────────────────────────────────────────────────────────────┐
│                    SEGMENT FILE                             │
├─────────────────────────────────────────────────────────────┤
│  RECORD 1: [Binary Header][Protobuf Payload]                │
│            [17 bytes]     [Variable length]                 │
│                                                             │
│  RECORD 2: [Binary Header][Protobuf Payload]                │
│            [17 bytes]     [Variable length]                 │
│                                                             │
│  RECORD 3: [Binary Header][Protobuf Payload]                │
│            [17 bytes]     [Variable length]                 │
│  ...                                                        │
└─────────────────────────────────────────────────────────────┘
```

Each record header contains essential metadata in a fixed binary structure:

```go
type RecordHeader struct {
    Checksum    uint32 // 4 bytes: CRC32 for data integrity
    PayloadSize uint32 // 4 bytes: Size of protobuf payload
    Version     uint8  // 1 byte: Schema version
    Timestamp   int64  // 8 bytes: Creation timestamp
}
```

### Core Operations

#### `Set`

```go
func (i *Instance) Set(ctx context.Context, key []byte, value []byte) error
```

Stores a key-value pair with immediate durability. The operation is atomic and
fully durable once it returns successfully.

#### `SetX`

```go
func (i *Instance) SetX(ctx context.Context, key []byte, value []byte, ttl time.Duration) error
```

Stores a key-value pair with automatic expiration after the specified duration.
Ideal for implementing caches, session stores, and time-sensitive data.

#### `Get`

```go
func (i *Instance) Get(ctx context.Context, key []byte) (*storage.Record, error)
```

Retrieves the complete record associated with the given key, if it exists and
hasn't expired. Uses O(1) index lookup followed by direct file access.

#### `Exists`

```go
func (i *Instance) Exists(ctx context.Context, key []byte) (bool, error)
```

Checks whether a key exists in the database without retrieving the full record.
Significantly faster than Get for large values since it only accesses the index.

#### `Delete`

```go
func (i *Instance) Delete(ctx context.Context, key []byte) (bool, error)
```

Removes a key-value pair from the database using logical deletion. The operation
immediately makes the key inaccessible while marking it for physical removal
during compaction.

#### `Close`

```go
func (i *Instance) Close() error
```

Gracefully shuts down the database instance, ensuring data durability and proper
resource cleanup.

## Configuration

### Functional Configuration Pattern

The system uses functional options for configuration, providing a clean and
extensible approach to customization:

```go
// Available configuration functions
func WithDataDir(directory string) OptionFunc
func WithSegmentSize(size uint64) OptionFunc
func WithSegmentPrefix(prefix string) OptionFunc
func WithSegmentDir(directory string) OptionFunc
func WithCompactInterval(interval time.Duration) OptionFunc
```

### Configuration Constraints

#### Segment Size Constraints

- **Minimum**: 512MB (prevents excessive file fragmentation)
- **Maximum**: 4GB (maintains manageable file sizes for backup/recovery)
- **Default**: 1GB (balanced performance and operational simplicity)

#### Directory Structure

- **Base data directory**: `/var/lib/kvix` (default)
- **Segment subdirectory**: Configurable within base directory
- **Filename format**: `{prefix}_{segmentID}_{timestamp}.seg`

#### Compaction Settings

- **Default interval**: 5 hours
- **Maximum interval**: 168 hours (1 week)
- **Minimum interval**: Default compaction interval

### Session Store Implementation

```go
package session

import (
    "context"
    "encoding/json"
    "fmt"
    "time"

    "github.com/iamBelugaa/kvix/pkg/kvix"
    "github.com/iamBelugaa/kvix/pkg/errors"
)

type SessionStore struct {
    db *kvix.Instance
}

type SessionData struct {
    UserID      string                 `json:"user_id"`
    Permissions []string               `json:"permissions"`
    Data        map[string]any         `json:"data"`
    CreatedAt   time.Time              `json:"created_at"`
    LastAccess  time.Time              `json:"last_access"`
}

func NewSessionStore(dataDir string) (*SessionStore, error) {
    db, err := kvix.NewInstance(context.Background(), "session-store",
        kvix.WithDataDir(dataDir),
        kvix.WithSegmentPrefix("sessions"),
        kvix.WithSegmentSize(1024*1024*1024), // 1GB segments
    )
    if err != nil {
        return nil, fmt.Errorf("failed to initialize session store: %w", err)
    }

    return &SessionStore{db: db}, nil
}

func (s *SessionStore) CreateSession(ctx context.Context, userID string, permissions []string) (string, error) {
    sessionID := generateSecureSessionID()

    sessionData := SessionData{
        UserID:      userID,
        Permissions: permissions,
        Data:        make(map[string]any),
        CreatedAt:   time.Now(),
        LastAccess:  time.Now(),
    }

    encoded, err := json.Marshal(sessionData)
    if err != nil {
        return "", fmt.Errorf("failed to encode session data: %w", err)
    }

    key := []byte("session:" + sessionID)
    err = s.db.SetX(ctx, key, encoded, 24*time.Hour)
    if err != nil {
        return "", fmt.Errorf("failed to store session: %w", err)
    }

    return sessionID, nil
}

func (s *SessionStore) GetSession(ctx context.Context, sessionID string) (*SessionData, error) {
    key := []byte("session:" + sessionID)

    record, err := s.db.Get(ctx, key)
    if err != nil {
        if errors.GetErrorCode(err) == errors.ErrIndexKeyNotFound {
            return nil, ErrSessionNotFound
        }
        return nil, fmt.Errorf("failed to retrieve session: %w", err)
    }

    var session SessionData
    if err := json.Unmarshal(record.Value, &session); err != nil {
        return nil, fmt.Errorf("failed to decode session data: %w", err)
    }

    session.LastAccess = time.Now()
    if err := s.updateSession(ctx, sessionID, &session); err != nil {
        log.Printf("Warning: failed to update session access time: %v", err)
    }

    return &session, nil
}

func (s *SessionStore) updateSession(ctx context.Context, sessionID string, session *SessionData) error {
    encoded, err := json.Marshal(session)
    if err != nil {
        return err
    }

    key := []byte("session:" + sessionID)
    return s.db.SetX(ctx, key, encoded, 24*time.Hour)
}

func (s *SessionStore) DeleteSession(ctx context.Context, sessionID string) error {
    key := []byte("session:" + sessionID)

    deleted, err := s.db.Delete(ctx, key)
    if err != nil {
        return fmt.Errorf("failed to delete session: %w", err)
    }

    if !deleted {
        return ErrSessionNotFound
    }

    return nil
}

func (s *SessionStore) Close() error {
    return s.db.Close()
}
```
