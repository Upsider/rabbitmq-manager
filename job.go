package rabbit_mq

import "time"

type Job struct {
	Args    []interface{}
	After   *time.Duration // execute after this amount in milliseconds
	JobName string
}

func NewJob(jobName string, args ...interface{}) Job {
	return Job{
		JobName: jobName,
		Args:    args,
	}
}

