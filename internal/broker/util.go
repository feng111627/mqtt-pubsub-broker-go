package broker

import (
	"strconv"
	"sync/atomic"
	"time"
)

func itoa(v uint64) string { return strconv.FormatUint(v, 10) }

var suffixCounter uint64

// randomSuffix returns a process-unique, time-seeded opaque token. It is not a
// cryptographic id; collision resistance only matters within one broker run.
func randomSuffix() string {
	n := atomic.AddUint64(&suffixCounter, 1)
	return strconv.FormatInt(time.Now().UnixNano(), 36) + "-" + strconv.FormatUint(n, 36)
}
