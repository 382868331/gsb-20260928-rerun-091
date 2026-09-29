package objectstore

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

func newUploadID() (string, error) {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("%d-%s", time.Now().UnixNano(), hex.EncodeToString(b[:])), nil
}
