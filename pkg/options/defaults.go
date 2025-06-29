package options

import "time"

const (
	DefaultDataDir string = "/var/lib/kvix"

	DefaultCompactInterval = time.Hour * 5
	MaxCompactInterval     = 168 * time.Hour

	MinSegmentSize     uint64 = 512 * 1024 * 1024
	MaxSegmentSize     uint64 = 4 * 1024 * 1024 * 1024
	DefaultSegmentSize uint64 = 1 * 1024 * 1024 * 1024

	DefaultSegmentPrefix    string = "segment"
	DefaultSegmentDirectory string = DefaultDataDir + "/segments"

	MaxKeySize   uint16 = 65535
	MaxValueSize uint32 = 100 * 1024 * 1024

	MinSchemaVersion     uint8 = 1
	CurrentSchemaVersion uint8 = 1
	MaxSchemaVersion     uint8 = 255
)

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
