package options

import "time"

const (
	// Specifies the default base directory where IgniteDB will store its data files.
	DefaultDataDir string = "/var/lib/ignitedb"

	// Defines the default time duration between automatic compaction operations.
	DefaultCompactInterval = time.Hour * 5

	// Defines the max time duration between automatic compaction operations.
	MaxCompactInterval = 168 * time.Hour

	// Represents the minimum allowed size for a segment file in bytes (512MB).
	MinSegmentSize uint64 = 512 * 1024 * 1024

	// Represents the maximum allowed size for a segment file in bytes (4GB).
	MaxSegmentSize uint64 = 4 * 1024 * 1024 * 1024

	// Specifies the default target size for a new segment file in bytes (1GB).
	DefaultSegmentSize uint64 = 1 * 1024 * 1024 * 1024

	// Specifies the default subdirectory within the main data directory
	// where segment files will be stored.
	DefaultSegmentDirectory string = DefaultDataDir + "/segments"

	// Defines the default prefix for segment file names.
	// For example, a segment file might be named "segment-00001.db".
	DefaultSegmentPrefix string = "segment"

	// Maximum allowed size for a record key in bytes (64KB).
	MaxKeySize uint16 = 65535

	// Maximum allowed size for a record value in bytes (100MB).
	MaxValueSize uint32 = 100 * 1024 * 1024

	// Specifies the minimum supported version of IgniteDB required for compatibility.
	MinSchemaVersion uint8 = 1

	// Represents the current version of the IgniteDB format or schema.
	CurrentSchemaVersion uint8 = 1

	// Specifies the maximum supported version of IgniteDB.
	MaxSchemaVersion uint8 = 255
)

// Holds the default configuration settings for an IgniteDB instance.
var defaultOptions = Options{
	DataDir:         DefaultDataDir,
	CompactInterval: DefaultCompactInterval,
	SegmentOptions: &SegmentOptions{
		Size:      DefaultSegmentSize,
		Prefix:    DefaultSegmentPrefix,
		Directory: DefaultSegmentDirectory,
	},
}

func DefaultOptions() Options {
	return defaultOptions
}
