package task

import "github.com/oxia-io/okk/internal/proto"

func IsEventually(assertion *proto.Assertion) bool {
	if assertion.EventuallyEmpty != nil && *assertion.EventuallyEmpty {
		return true
	}
	return false
}
