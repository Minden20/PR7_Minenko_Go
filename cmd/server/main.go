package main

import (
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"media-service/internal/server"
	pb "media-service/proto"
)

const addr = ":50051"

func main() {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("[SERVER] Failed to listen on %s: %v", addr, err)
	}

	grpcServer := grpc.NewServer()
	media := server.NewMediaServer(server.NewJobStore(), 500*time.Millisecond)
	pb.RegisterMediaServiceServer(grpcServer, media)

	reflection.Register(grpcServer)

	log.Printf("[SERVER] Starting Media Processing gRPC service on %s...", addr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("[SERVER] Serve failed: %v", err)
	}
}
