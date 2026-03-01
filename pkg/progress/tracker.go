package progress

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

type Tracker struct {
	Year          int            `json:"year"`
	ProcessedDays map[string]bool `json:"processed_days"`
	FailedDays    map[string]string `json:"failed_days"` // date -> error message
	mu            sync.Mutex
	filePath      string
}

func NewTracker(year int, filePath string) *Tracker {
	return &Tracker{
		Year:          year,
		ProcessedDays: make(map[string]bool),
		FailedDays:    make(map[string]string),
		filePath:      filePath,
	}
}

func LoadTracker(filePath string) (*Tracker, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read progress file: %w", err)
	}

	var tracker Tracker
	if err := json.Unmarshal(data, &tracker); err != nil {
		return nil, fmt.Errorf("unmarshal progress: %w", err)
	}

	tracker.filePath = filePath
	if tracker.ProcessedDays == nil {
		tracker.ProcessedDays = make(map[string]bool)
	}
	if tracker.FailedDays == nil {
		tracker.FailedDays = make(map[string]string)
	}

	return &tracker, nil
}

func (t *Tracker) IsProcessed(date string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.ProcessedDays[date]
}

func (t *Tracker) MarkProcessed(date string) error {
	t.mu.Lock()
	t.ProcessedDays[date] = true
	delete(t.FailedDays, date) // 成功后清除失败记录
	t.mu.Unlock()
	return t.Save()
}

func (t *Tracker) MarkFailed(date string, errMsg string) error {
	t.mu.Lock()
	t.FailedDays[date] = errMsg
	t.mu.Unlock()
	return t.Save()
}

func (t *Tracker) Save() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	data, err := json.MarshalIndent(t, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal progress: %w", err)
	}

	if err := os.WriteFile(t.filePath, data, 0644); err != nil {
		return fmt.Errorf("write progress file: %w", err)
	}

	return nil
}

func (t *Tracker) GetStats() (total, processed, failed int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	processed = len(t.ProcessedDays)
	failed = len(t.FailedDays)
	total = processed + failed
	
	return
}
