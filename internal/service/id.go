package service

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

type IDGenerator interface {
	NewID(prefix string) string
}

type RandomIDGenerator struct{}

func (RandomIDGenerator) NewID(prefix string) string {
	var bytes [8]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return fmt.Sprintf("%s_%d", sanitizePrefix(prefix), time.Now().UnixNano())
	}
	return fmt.Sprintf("%s_%x_%s", sanitizePrefix(prefix), time.Now().UTC().UnixNano(), hex.EncodeToString(bytes[:]))
}

func sanitizePrefix(prefix string) string {
	prefix = strings.TrimSpace(strings.ToLower(prefix))
	if prefix == "" {
		return "id"
	}
	return prefix
}
