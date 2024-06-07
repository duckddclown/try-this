package surfstore

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"sync"

	grpc "google.golang.org/grpc"
)

type RaftConfig struct {
	RaftAddrs  []string
	BlockAddrs []string
}

func LoadRaftConfigFile(filename string) (cfg RaftConfig) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	decoder := json.NewDecoder(configReader)

	if err := decoder.Decode(&cfg); err == io.EOF {
		return
	} else if err != nil {
		log.Fatal(err)
	}
	return
}

func NewRaftServer(id int64, config RaftConfig) (*RaftSurfstore, error) {
	// TODO Any initialization you need here

	isLeaderMutex := sync.RWMutex{}
	isCrashedMutex := sync.RWMutex{}

	server := RaftSurfstore{
		isLeader:       false,
		isLeaderMutex:  &isLeaderMutex,
		term:           0,
		metaStore:      NewMetaStore(config.BlockAddrs),
		log:            make([]*UpdateOperation, 0),
		isCrashed:      false,
		isCrashedMutex: &isCrashedMutex,

		id:           id,
		peers:        config.RaftAddrs,
		commitIndex:  -1,
		lastApplied:  -1,
		nextIndex:    make([]int64, len(config.RaftAddrs)),
		MatchedIndex: make([]int64, len(config.RaftAddrs)),
		// pendingCommits: make
	}

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
	// Create a grpc server
	grpcServer := grpc.NewServer()
	// // Register rpc services
	RegisterRaftSurfstoreServer(grpcServer, server)

	// Start listening on hostAddr
	listener, err := net.Listen("tcp", server.peers[server.id])
	log.Printf("RaftSurstore server - Listening on %v\n", server.peers[server.id])
	if err != nil {
		return err
	}

	return grpcServer.Serve(listener)
}
