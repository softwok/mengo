package poll

type Request struct {
	Topic         string `json:"topic"`
	ConsumerGroup string `json:"consumerGroup"`
	Partition     uint8  `json:"partition"`
}
