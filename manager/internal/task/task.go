package task

import "io"

type Task interface {
	io.Closer

	Run() error
}

func NewTask() {

}
