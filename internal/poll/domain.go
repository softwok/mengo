package poll

import (
	"time"
)

type Request struct {
	Id        string    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
}
