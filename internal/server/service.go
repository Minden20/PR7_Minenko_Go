package server

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "media-service/proto"
)

type Stage struct {
	Name    string
	Percent int32
}

var DefaultStages = []Stage{
	{"Demuxing input stream", 25},
	{"Encoding video frames", 50},
	{"Remuxing audio and headers", 75},
	{"Process completed successfully", 100},
}

type MediaServer struct {
	pb.UnimplementedMediaServiceServer

	store     *JobStore
	stages    []Stage
	stepDelay time.Duration
}

func NewMediaServer(store *JobStore, stepDelay time.Duration) *MediaServer {
	return &MediaServer{store: store, stages: DefaultStages, stepDelay: stepDelay}
}

func (s *MediaServer) SubmitJob(_ context.Context, req *pb.ProcessMediaRequest) (*pb.JobResponse, error) {
	if strings.TrimSpace(req.GetFilename()) == "" {
		return nil, status.Error(codes.InvalidArgument, "filename is required")
	}
	if req.GetTargetFormat() == pb.MediaFormat_MEDIA_FORMAT_UNSPECIFIED {
		return nil, status.Error(codes.InvalidArgument, "target_format must be specified")
	}
	if _, ok := pb.MediaFormat_name[int32(req.GetTargetFormat())]; !ok {
		return nil, status.Errorf(codes.InvalidArgument, "unknown target_format: %d", req.GetTargetFormat())
	}
	if q := req.GetQualityLevel(); q < 1 || q > 100 {
		return nil, status.Errorf(codes.InvalidArgument, "quality_level must be between 1 and 100, got %d", q)
	}

	job := Job{
		ID:           uuid.NewString(),
		ClientID:     req.GetClientId(),
		Filename:     req.GetFilename(),
		TargetFormat: req.GetTargetFormat(),
		QualityLevel: req.GetQualityLevel(),
		Status:       StatusQueued,
		CreatedAt:    time.Now(),
	}
	s.store.Save(job)
	log.Printf("[STORAGE] Job registered: ID=%s Filename=%s TargetFormat=%s Status=%s",
		job.ID, job.Filename, job.TargetFormat, job.Status)

	return &pb.JobResponse{
		JobId:     job.ID,
		Status:    job.Status,
		CreatedAt: job.CreatedAt.Unix(),
	}, nil
}

func (s *MediaServer) GetJobStatus(_ context.Context, req *pb.GetJobStatusRequest) (*pb.JobStatusResponse, error) {
	job, ok := s.store.Get(req.GetJobId())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "job with ID '%s' not found", req.GetJobId())
	}

	resp := &pb.JobStatusResponse{
		JobId:           job.ID,
		Status:          job.Status,
		ProgressPercent: job.Progress,
	}
	if !job.CompletedAt.IsZero() {
		resp.CompletedAt = job.CompletedAt.Unix()
	}
	return resp, nil
}

func (s *MediaServer) TrackProgress(req *pb.GetJobStatusRequest, stream grpc.ServerStreamingServer[pb.ProgressUpdate]) error {
	id := req.GetJobId()
	job, ok := s.store.Get(id)
	if !ok {
		return status.Errorf(codes.NotFound, "job with ID '%s' not found", id)
	}
	log.Printf("[STREAM] Client connected to track progress for JobID=%s", id)

	if job.Status == StatusCompleted {
		last := s.stages[len(s.stages)-1]
		return stream.Send(&pb.ProgressUpdate{
			JobId: id, CurrentStage: last.Name, ProgressPercent: 100, IsCompleted: true,
		})
	}

	s.store.Update(id, func(j *Job) { j.Status = StatusProcessing })

	timer := time.NewTimer(s.stepDelay)
	defer timer.Stop()

	for _, stage := range s.stages {
		select {
		case <-stream.Context().Done():
			s.store.Update(id, func(j *Job) { j.Status = StatusCanceled })
			log.Printf("[STREAM] Client disconnected, processing canceled for JobID=%s", id)
			return status.Error(codes.Canceled, "client canceled tracking")
		case <-timer.C:
		}

		completed := stage.Percent >= 100
		s.store.Update(id, func(j *Job) {
			j.Progress = stage.Percent
			if completed {
				j.Status = StatusCompleted
				j.CompletedAt = time.Now()
			}
		})

		update := &pb.ProgressUpdate{
			JobId:           id,
			CurrentStage:    stage.Name,
			ProgressPercent: stage.Percent,
			IsCompleted:     completed,
		}
		if err := stream.Send(update); err != nil {
			s.store.Update(id, func(j *Job) {
				if j.Status != StatusCompleted {
					j.Status = StatusCanceled
				}
			})
			return err
		}
		log.Printf("[STREAM] JobID=%s -> Sent stage %q (%d%%)", id, stage.Name, stage.Percent)
		timer.Reset(s.stepDelay)
	}

	log.Printf("[STREAM] Progress tracking finished for JobID=%s", id)
	return nil
}
