package mr

type TaskType int
type Phase int
type Task struct {
	Input    string
	Id       int
	TaskType TaskType
}

const (
	MapTask TaskType = iota
	ReduceTask
	Wait
)

const (
	MapPhase Phase = iota
	ReducePhase
)
