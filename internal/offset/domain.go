package offset

import "time"

const CollectionName = "offsets"

type Offset struct {
	Id        string    `bson:"_id" json:"id"`
	Timestamp time.Time `json:"timestamp"`
}
