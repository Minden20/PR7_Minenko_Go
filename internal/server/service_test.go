package server

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	pb "media-service/proto"
)

func startServer(t *testing.T, delay time.Duration) (pb.MediaServiceClient, *JobStore) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	store := NewJobStore()
	srv := grpc.NewServer()
	pb.RegisterMediaServiceServer(srv, NewMediaServer(store, delay))
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewMediaServiceClient(conn), store
}

func validRequest() *pb.ProcessMediaRequest {
	return &pb.ProcessMediaRequest{
		ClientId: "test", Filename: "input.mov",
		TargetFormat: pb.MediaFormat_MEDIA_FORMAT_MP4, QualityLevel: 80,
	}
}

func TestSubmitAndGetStatus(t *testing.T) {
	client, _ := startServer(t, time.Millisecond)
	ctx := context.Background()

	job, err := client.SubmitJob(ctx, validRequest())
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	if job.GetJobId() == "" || job.GetStatus() != StatusQueued || job.GetCreatedAt() == 0 {
		t.Fatalf("unexpected response: %+v", job)
	}

	st, err := client.GetJobStatus(ctx, &pb.GetJobStatusRequest{JobId: job.GetJobId()})
	if err != nil {
		t.Fatalf("GetJobStatus: %v", err)
	}
	if st.GetStatus() != StatusQueued || st.GetProgressPercent() != 0 {
		t.Fatalf("unexpected status: %+v", st)
	}
}

func TestGetStatusNotFound(t *testing.T) {
	client, _ := startServer(t, time.Millisecond)
	_, err := client.GetJobStatus(context.Background(), &pb.GetJobStatusRequest{JobId: "missing"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("code = %v, want NotFound", status.Code(err))
	}
}

func TestSubmitValidation(t *testing.T) {
	client, _ := startServer(t, time.Millisecond)
	cases := map[string]func(*pb.ProcessMediaRequest){
		"empty filename":     func(r *pb.ProcessMediaRequest) { r.Filename = "" },
		"unspecified format": func(r *pb.ProcessMediaRequest) { r.TargetFormat = pb.MediaFormat_MEDIA_FORMAT_UNSPECIFIED },
		"quality too low":    func(r *pb.ProcessMediaRequest) { r.QualityLevel = 0 },
		"quality too high":   func(r *pb.ProcessMediaRequest) { r.QualityLevel = 101 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			req := validRequest()
			mutate(req)
			_, err := client.SubmitJob(context.Background(), req)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("code = %v, want InvalidArgument", status.Code(err))
			}
		})
	}
}

func TestTrackProgressCompletes(t *testing.T) {
	client, _ := startServer(t, 5*time.Millisecond)
	ctx := context.Background()
	job, _ := client.SubmitJob(ctx, validRequest())

	stream, err := client.TrackProgress(ctx, &pb.GetJobStatusRequest{JobId: job.GetJobId()})
	if err != nil {
		t.Fatalf("TrackProgress: %v", err)
	}
	var got []int32
	for {
		u, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		got = append(got, u.GetProgressPercent())
	}
	want := []int32{25, 50, 75, 100}
	if len(got) != len(want) {
		t.Fatalf("progress = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("progress = %v, want %v", got, want)
		}
	}

	st, _ := client.GetJobStatus(ctx, &pb.GetJobStatusRequest{JobId: job.GetJobId()})
	if st.GetStatus() != StatusCompleted || st.GetProgressPercent() != 100 || st.GetCompletedAt() == 0 {
		t.Fatalf("final status: %+v", st)
	}
}

func TestTrackProgressNotFound(t *testing.T) {
	client, _ := startServer(t, time.Millisecond)
	stream, err := client.TrackProgress(context.Background(), &pb.GetJobStatusRequest{JobId: "missing"})
	if err == nil {
		_, err = stream.Recv()
	}
	if status.Code(err) != codes.NotFound {
		t.Fatalf("code = %v, want NotFound", status.Code(err))
	}
}

func TestTrackProgressCanceledByClient(t *testing.T) {
	client, store := startServer(t, 200*time.Millisecond)
	job, _ := client.SubmitJob(context.Background(), validRequest())

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.TrackProgress(ctx, &pb.GetJobStatusRequest{JobId: job.GetJobId()})
	if err != nil {
		t.Fatalf("TrackProgress: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("Recv: %v", err)
	}
	cancel()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if j, _ := store.Get(job.GetJobId()); j.Status == StatusCanceled {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	j, _ := store.Get(job.GetJobId())
	t.Fatalf("status = %s, want %s", j.Status, StatusCanceled)
}
