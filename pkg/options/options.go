// Package options provides data structures and functions for configuring the Ignite database.
package options

import (
	"fmt"
	"math"
	"strings"
	"time"
)

// Defines configurable parameters for each segment.
type SegmentOptions struct {
	// Defines the maximum size a segment can grow to before rotation.
	//
	//  - Default: 1GB
	//  - Maximum: 4GB
	//  - Minimum: 512MB
	Size uint64 `json:"maxSegmentSize"`

	// Specifies where segment files are stored.
	//
	// Default: "/var/lib/ignitedb/segments"
	Directory string `json:"directory"`

	// Defines the filename prefix for segment files.
	// Final filename will be: `prefix_segmentId_timestamp.seg`
	//
	// Default: "segment"
	//
	// Example: If Prefix is "mydata", a segment file might be "mydata_000001_20240525232100.seg".
	Prefix string `json:"prefix"`
}

// Defines the configuration parameters for Ignite DB.
type Options struct {
	// Specifies the base path where files will be stored.
	//
	// Default: "/var/lib/ignitedb"
	DataDir string `json:"dataDir"`

	// Defines how often the compaction process runs to  merge old segments.
	//
	// Default: 5h
	CompactInterval time.Duration `json:"compactInterval"`

	// Configures segment management including size limits and naming convention.
	SegmentOptions *SegmentOptions `json:"segmentOptions"`
}

type OptionFunc func(*Options)

// Applies a predefined set of default configuration values to the Options struct.
func WithDefaultOptions() OptionFunc {
	return func(o *Options) {
		opts := DefaultOptions()
		o.DataDir = opts.DataDir
		o.SegmentOptions = opts.SegmentOptions
		o.CompactInterval = opts.CompactInterval
	}
}

// Sets the primary data directory for Ignite.
func WithDataDir(directory string) OptionFunc {
	return func(o *Options) {
		directory = strings.TrimSpace(directory)
		if directory != "" {
			o.DataDir = directory
		}
	}
}

// Sets the interval at which Ignite performs compaction operations.
func WithCompactInterval(interval time.Duration) OptionFunc {
	return func(o *Options) {
		if interval > DefaultCompactInterval {
			o.CompactInterval = interval
		}
	}
}

// Sets the directory specifically for storing segment files.
func WithSegmentDir(directory string) OptionFunc {
	return func(o *Options) {
		directory = strings.TrimSpace(directory)
		if directory != "" {
			o.SegmentOptions.Directory = directory
		}
	}
}

// Sets the file name prefix for segment files.
func WithSegmentPrefix(prefix string) OptionFunc {
	return func(o *Options) {
		prefix = strings.TrimSpace(prefix)
		if prefix != "" {
			o.SegmentOptions.Prefix = prefix
		}
	}
}

// Sets the maximum size of individual segment files.
func WithSegmentSize(size uint64) OptionFunc {
	return func(o *Options) {
		if size > MinSegmentSize && size < MaxSegmentSize {
			o.SegmentOptions.Size = size
		}
	}
}

// FormatBytes converts byte count to human-readable format for error messages.
func FormatBytes(bytes uint64) string {
	const unit = 1024
	var units = []string{"B", "KB", "MB", "GB", "TB"}

	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}

	exp := 0
	value := float64(bytes)

	for value >= unit && exp < len(units)-1 {
		value /= unit
		exp++
	}

	if math.Abs(value-math.Round(value)) < 0.01 {
		return fmt.Sprintf("%.0f %s", math.Round(value), units[exp])
	}
	return fmt.Sprintf("%.2f %s", value, units[exp])
}
