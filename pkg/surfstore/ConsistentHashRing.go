package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string // hashed serverAddr -> serverAddr
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	serverHashes := make([]string, 0, len(c.ServerMap))
	for k := range c.ServerMap {
		serverHashes = append(serverHashes, k)
	}
	sort.Strings(serverHashes)
	for _, serverHash := range serverHashes {
		if serverHash > blockId {
			return c.ServerMap[serverHash]
		}
	}
	return c.ServerMap[serverHashes[0]]
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))
}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	hashRing := ConsistentHashRing{}
	hashRing.ServerMap = make(map[string]string)
	for _, serverAddr := range serverAddrs {
		serverHash := hashRing.Hash("blockstore" + serverAddr)
		hashRing.ServerMap[serverHash] = serverAddr
	}
	return &hashRing
}
