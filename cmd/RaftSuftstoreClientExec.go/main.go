package main

import (
	"context"
	"cse224/proj5/pkg/surfstore"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

func main() {

	ctx, _ := context.WithTimeout(context.Background(), time.Minute)

	conn, err := grpc.Dial("localhost:9007", grpc.WithInsecure())
	// conn, err := grpc.Dial("localhost:9008", grpc.WithInsecure())
	// conn, err := grpc.Dial("localhost:9009", grpc.WithInsecure())
	if err != nil {
		log.Fatal("Error connecting to clients ", err)
	}

	r1 := surfstore.NewRaftSurfstoreClient(conn)

	// r1.SetLeader(ctx, &emptypb.Empty{})
	r1.SendHeartbeat(ctx, &emptypb.Empty{})
	// res, err := r1.GetInternalState(ctx, &emptypb.Empty{})
	// log.Println(res)
}
