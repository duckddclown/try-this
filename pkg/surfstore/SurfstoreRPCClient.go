package surfstore

import (
	context "context"
	"fmt"
	"log"
	"strings"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	res, err := c.PutBlock(ctx, &Block{BlockData: block.BlockData, BlockSize: block.BlockSize})
	if err != nil {
		conn.Close()
		return err
	}
	*succ = res.Flag

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	res, err := c.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = res.Hashes

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		// connect to the server
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		defer conn.Close()
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		res, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
		if err != nil {
			if CheckErrorIsCrashedOrLeader(err) {
				continue
			}
			return err
		}
		*serverFileInfoMap = res.FileInfoMap
		return nil

		// close the connection
		// return conn.Close()
	}
	return fmt.Errorf("all servers crashed/not leader: %v", surfClient.MetaStoreAddrs)
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		// connect to the server
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		defer conn.Close()

		c := NewRaftSurfstoreClient(conn)
		log.Println("SurfClient - UpdateFile - Preparing to call RaftSurfstoreClient to udpate file")
		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		res, err := c.UpdateFile(ctx, &FileMetaData{
			Filename:      fileMetaData.Filename,
			Version:       fileMetaData.Version,
			BlockHashList: fileMetaData.BlockHashList})
		log.Printf("SurfClient - UpdateFile - RaftSurfstoreClient.UpdateFile succeed, new version %v\n", res.Version)
		if err != nil {
			if CheckErrorIsCrashedOrLeader(err) {
				log.Printf("SurfClient - UpdateFile - RaftSurfstoreClient %v failed, try next server\n", addr)
				continue
			}
			log.Printf("SurfClient - UpdateFile - RaftSurfstoreClient %v failed with unexpected error: %v\n", addr, err.Error())
			return err
		}
		*latestVersion = res.Version
		return nil
	}
	return fmt.Errorf("all servers crashed/not leader: %v", surfClient.MetaStoreAddrs)
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	res, err := c.GetBlockHashes(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}

	*blockHashes = res.Hashes

	// close the connection
	return conn.Close()
}

func CheckErrorIsCrashedOrLeader(err error) bool {
	return strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) || strings.Contains(err.Error(), ERR_NOT_LEADER.Error())
}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		// connect to the server
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		defer conn.Close()
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		res, err := c.GetBlockStoreMap(ctx, &BlockHashes{
			Hashes: blockHashesIn,
		})
		if err != nil {
			log.Printf("GetBlockStoreMap - get from server %v failed: %v\n", addr, err.Error())
			if CheckErrorIsCrashedOrLeader(err) {
				continue
			}
			// conn.Close()
			return err
		}

		for server, hashes := range res.BlockStoreMap {
			(*blockStoreMap)[server] = hashes.Hashes
		}
		return nil
	}
	return fmt.Errorf("all servers crashed/not leader: %v", surfClient.MetaStoreAddrs)
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		// connect to the server
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		defer conn.Close()
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		res, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
		if err != nil {
			if CheckErrorIsCrashedOrLeader(err) {
				continue
			}
			// conn.Close()
			return err
		}

		*blockStoreAddrs = res.BlockStoreAddrs
		return nil
		// close the connection
		// return conn.Close()
	}
	return fmt.Errorf("all servers crashed/not leader: %v", surfClient.MetaStoreAddrs)
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
