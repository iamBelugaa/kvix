package checksum

import (
	"hash/crc32"
)

type CRC32IEEE struct {
	table *crc32.Table
}

func NewCRC32IEEE() *CRC32IEEE {
	return &CRC32IEEE{table: crc32.MakeTable(crc32.IEEE)}
}

func (c *CRC32IEEE) Calculate(data []byte) uint32 {
	return crc32.Checksum(data, c.table)
}

func (c *CRC32IEEE) Verify(data []byte, expected uint32) bool {
	checksum := crc32.Checksum(data, c.table)
	return checksum == expected
}
