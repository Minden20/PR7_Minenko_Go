package server

import (
	"sync"
	"time"

	pb "media-service/proto"
)

const (
	StatusQueued     = "QUEUED"
	StatusProcessing = "PROCESSING"
	StatusCompleted  = "COMPLETED"
	StatusCanceled   = "CANCELED"
)

type Job struct {
	ID           string
	ClientID     string
	Filename     string
	TargetFormat pb.MediaFormat
	QualityLevel int32
	Status       string
	Progress     int32
	CreatedAt    time.Time
	CompletedAt  time.Time
}

type JobStore struct {
	mu   sync.RWMutex
	jobs map[string]Job
}

func NewJobStore() *JobStore {
	return &JobStore{jobs: make(map[string]Job)}
}

func (s *JobStore) Save(job Job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[job.ID] = job
}

func (s *JobStore) Get(id string) (Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.jobs[id]
	return job, ok
}

func (s *JobStore) Update(id string, fn func(*Job)) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[id]
	if !ok {
		return false
	}
	fn(&job)
	s.jobs[id] = job
	return true
}
