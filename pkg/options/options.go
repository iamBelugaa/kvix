package options

import (
	"strings"
	"time"
)

type SegmentOptions struct {
	Size      uint64 `json:"maxSegmentSize"` // Default: 1GB - Maximum: 4GB - Minimum: 512MB
	Directory string `json:"directory"`      // Default: "/var/lib/kvix/segments"
	Prefix    string `json:"prefix"`         // Default: "segment"
}

type Options struct {
	SegmentOptions  *SegmentOptions `json:"segmentOptions"`
	DataDir         string          `json:"dataDir"`         // Default: "/var/lib/kvix"
	CompactInterval time.Duration   `json:"compactInterval"` // Default: 5h
}

type OptionFunc func(*Options)

func WithDefaultOptions() OptionFunc {
	return func(o *Options) {
		opts := DefaultOptions()
		o.DataDir = opts.DataDir
		o.SegmentOptions = opts.SegmentOptions
		o.CompactInterval = opts.CompactInterval
	}
}

func WithDataDir(directory string) OptionFunc {
	return func(o *Options) {
		directory = strings.TrimSpace(directory)
		if directory != "" {
			o.DataDir = directory
		}
	}
}

func WithCompactInterval(interval time.Duration) OptionFunc {
	return func(o *Options) {
		if interval > DefaultCompactInterval {
			o.CompactInterval = interval
		}
	}
}

func WithSegmentDir(directory string) OptionFunc {
	return func(o *Options) {
		directory = strings.TrimSpace(directory)
		if directory != "" {
			o.SegmentOptions.Directory = directory
		}
	}
}

func WithSegmentPrefix(prefix string) OptionFunc {
	return func(o *Options) {
		prefix = strings.TrimSpace(prefix)
		if prefix != "" {
			o.SegmentOptions.Prefix = prefix
		}
	}
}

func WithSegmentSize(size uint64) OptionFunc {
	return func(o *Options) {
		if size > MinSegmentSize && size < MaxSegmentSize {
			o.SegmentOptions.Size = size
		}
	}
}
