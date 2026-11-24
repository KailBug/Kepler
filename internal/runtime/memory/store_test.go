package memory_test

import (
	"testing"
	"time"

	"github.com/KailBug/Kepler/internal/runtime"
	"github.com/KailBug/Kepler/internal/runtime/memory"
	"github.com/KailBug/Kepler/internal/runtime/storetest"
)

func TestMemoryStore(t *testing.T) {
	storetest.Run(t, func(t *testing.T) (runtime.Store, func(time.Duration)) {
		now := time.Now()
		s := memory.New().WithClock(func() time.Time { return now })
		return s, func(d time.Duration) { now = now.Add(d) }
	})
}
