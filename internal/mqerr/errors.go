// Package mqerr defines the broker/client error taxonomy.
//
// Errors are divided into three distinguishable classes so callers can tell
// protocol violations, invalid broker/session states and connection failures
// apart without string matching.
package mqerr

import "fmt"

// Class identifies the high-level cause of an operation failure.
type Class int

const (
	// ClassProtocol: the peer sent (or asked to send) something forbidden or
	// malformed by the MQTT specification.
	ClassProtocol Class = iota
	// ClassState: the local broker/session/client is in a state where the
	// requested operation is not possible.
	ClassState
	// ClassConnection: the transport connection failed or was lost.
	ClassConnection
)

// Error is the structured error type used across broker and client.
type Error struct {
	Class  Class
	Code   Code
	Reason string
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Reason)
}

// New constructs an *Error.
func New(class Class, code Code, format string, args ...any) *Error {
	return &Error{Class: class, Code: code, Reason: fmt.Sprintf(format, args...)}
}

// Of reports whether err is an *Error with the given code.
func Of(err error, code Code) bool {
	e, ok := err.(*Error)
	return ok && e.Code == code
}

// IsClass reports whether err belongs to the given class.
func IsClass(err error, class Class) bool {
	e, ok := err.(*Error)
	return ok && e.Class == class
}
