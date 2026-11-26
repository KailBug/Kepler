package runtime

import "sync"

// Broadcaster fans out lightweight wake-up signals per session so SSE / Connect-RPC
// stream subscribers immediately pull newly committed events from the Store.
type Broadcaster struct {
	mu   sync.RWMutex
	subs map[string]map[chan struct{}]struct{}
}

func NewBroadcaster() *Broadcaster {
	return &Broadcaster{
		subs: make(map[string]map[chan struct{}]struct{}),
	}
}

// Subscribe registers a buffered wake-up channel for sessionID and returns an unsubscribe func.
func (b *Broadcaster) Subscribe(sessionID string) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	b.mu.Lock()
	set := b.subs[sessionID]
	if set == nil {
		set = make(map[chan struct{}]struct{})
		b.subs[sessionID] = set
	}
	set[ch] = struct{}{}
	b.mu.Unlock()

	unsub := func() {
		b.mu.Lock()
		if set := b.subs[sessionID]; set != nil {
			delete(set, ch)
			if len(set) == 0 {
				delete(b.subs, sessionID)
			}
		}
		b.mu.Unlock()
	}
	return ch, unsub
}

// Notify wakes up all subscribers watching sessionID without blocking.
func (b *Broadcaster) Notify(sessionID string) {
	b.mu.RLock()
	set := b.subs[sessionID]
	for ch := range set {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	b.mu.RUnlock()
}
