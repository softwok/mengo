package subscribe

type Request struct {
	Topic         string `json:"topic"`
	ConsumerGroup string `json:"consumerGroup"`
}
