package tarchang

import (
	"crypto/sha256"
	"encoding/hex"
)

func BufToSha256String(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
