package checksum

import (
	"crypto/sha256"
	"encoding/hex"
)

func ComputeChecksum(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

func VerifyChecksum(data []byte, expected string) bool {
	return ComputeChecksum(data) == expected
}
