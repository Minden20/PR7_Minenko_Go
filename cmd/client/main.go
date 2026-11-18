package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "media-service/proto"
)

const timeLayout = "2006-01-02 15:04:05"

func main() {
	addr := flag.String("addr", "localhost:50051", "адреса gRPC-сервера")
	filename := flag.String("file", "input_video.mov", "ім'я вхідного файлу")
	flag.Parse()

	fmt.Println("=== Media Processing Service CLI Client ===")

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[ERROR] Failed to create client: %v", err)
	}
	defer conn.Close()

	client := pb.NewMediaServiceClient(conn)

	fmt.Println("\n[Step 1] Sending SubmitJob request...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	job, err := client.SubmitJob(ctx, &pb.ProcessMediaRequest{
		ClientId:     "cli-client-01",
		Filename:     *filename,
		TargetFormat: pb.MediaFormat_MEDIA_FORMAT_MP4,
		QualityLevel: 85,
	})
	cancel()
	if err != nil {
		fail(err)
	}
	fmt.Println("[SUCCESS] Job created successfully:")
	fmt.Printf("  - Job ID:        %s\n", job.GetJobId())
	fmt.Printf("  - Initial State: %s\n", job.GetStatus())
	fmt.Printf("  - Submitted At:  %s\n", time.Unix(job.GetCreatedAt(), 0).Format(timeLayout))

	fmt.Println("\n[Step 2] Subscribing to Server Streaming: TrackProgress...")
	streamCtx, streamCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer streamCancel()

	stream, err := client.TrackProgress(streamCtx, &pb.GetJobStatusRequest{JobId: job.GetJobId()})
	if err != nil {
		fail(err)
	}
	for {
		update, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			fail(err)
		}
		suffix := "..."
		if update.GetIsCompleted() {
			suffix = "!"
		}
		fmt.Printf("  [%d%%] %s%s\n", update.GetProgressPercent(), update.GetCurrentStage(), suffix)
	}

	fmt.Println("\n[Step 3] Checking final job status via GetJobStatus RPC...")
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	st, err := client.GetJobStatus(ctx, &pb.GetJobStatusRequest{JobId: job.GetJobId()})
	if err != nil {
		fail(err)
	}
	fmt.Printf("  - Current Status: %s\n", st.GetStatus())
	fmt.Printf("  - Progress:       %d%%\n", st.GetProgressPercent())
	if st.GetCompletedAt() > 0 {
		fmt.Printf("  - Completed At:   %s\n", time.Unix(st.GetCompletedAt(), 0).Format(timeLayout))
	}

	fmt.Println("\n[Step 4] Requesting status of non-existent job...")
	_, err = client.GetJobStatus(ctx, &pb.GetJobStatusRequest{JobId: "00000000-0000-0000-0000-000000000000"})
	if s, ok := status.FromError(err); ok && err != nil {
		fmt.Printf("  [EXPECTED ERROR] code = %s, message = %s\n", s.Code(), s.Message())
	}

	fmt.Println("\n=== Processing pipeline finished successfully ===")
}

func fail(err error) {
	if s, ok := status.FromError(err); ok {
		fmt.Printf("[ERROR] Server responded with gRPC error: code = %s desc = %s\n", s.Code(), s.Message())
	} else {
		fmt.Printf("[ERROR] %v\n", err)
	}
	os.Exit(1)
}
