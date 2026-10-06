package broker

import (
	"sort"
	"strings"
	"sync"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
)

// topicTrie is the deterministic wildcard routing structure.
//
// Each node owns exact children keyed by level text, at most one "+" child and
// at most one "#" terminal on itself. Terminal nodes carry the set of
// subscriptions ending there (keyed by session UID). Routing a published
// topic walks the levels following exact, "+" and ancestor "#" branches;
// every hit therefore has an explicit explanation (which filter matched).
type topicTrie struct {
	mu   sync.RWMutex
	root *trieNode
}

type trieNode struct {
	children map[string]*trieNode
	plus     *trieNode
	hash     *trieNode // node reached through a "#" level from this node's parent
	subs     map[uint64]*subscription
}

func newTrieNode() *trieNode {
	return &trieNode{subs: make(map[uint64]*subscription)}
}

func newTopicTrie() *topicTrie {
	return &topicTrie{root: newTrieNode()}
}

// add registers a session's subscription for a filter. Re-subscribing the
// same filter replaces the stored options (MQTT spec: new SUBSCRIBE overrides).
func (t *topicTrie) add(uid uint64, sub *subscription) {
	t.mu.Lock()
	defer t.mu.Unlock()
	node := t.root
	levels := packet.SplitTopic(sub.Filter)
	for i, lv := range levels {
		if lv == "#" && i == len(levels)-1 {
			if node.hash == nil {
				node.hash = newTrieNode()
			}
			node = node.hash
			break
		}
		if lv == "+" {
			if node.plus == nil {
				node.plus = newTrieNode()
			}
			node = node.plus
			continue
		}
		next := node.children[lv]
		if next == nil {
			if node.children == nil {
				node.children = make(map[string]*trieNode)
			}
			next = newTrieNode()
			node.children[lv] = next
		}
		node = next
	}
	node.subs[uid] = sub
}

// remove unregisters one filter for a session. Passing filter == "" removes
// every subscription owned by the uid (used on session destruction).
// Empty intermediate nodes are pruned so the trie does not leak.
func (t *topicTrie) remove(uid uint64, filter string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if filter == "" {
		t.removeUID(t.root, uid)
		return
	}
	path := []*trieNode{t.root}
	edges := []string{}
	node := t.root
	levels := packet.SplitTopic(filter)
	for i, lv := range levels {
		var next *trieNode
		if lv == "#" && i == len(levels)-1 {
			next = node.hash
		} else if lv == "+" {
			next = node.plus
		} else {
			next = node.children[lv]
		}
		if next == nil {
			return
		}
		path = append(path, next)
		edges = append(edges, lv)
		node = next
	}
	delete(node.subs, uid)
	t.prune(path, edges)
}

func (t *topicTrie) removeUID(node *trieNode, uid uint64) {
	delete(node.subs, uid)
	for _, c := range node.children {
		t.removeUID(c, uid)
	}
	if node.plus != nil {
		t.removeUID(node.plus, uid)
	}
	if node.hash != nil {
		t.removeUID(node.hash, uid)
	}
}

// prune removes trailing nodes (and edges) that became completely empty.
func (t *topicTrie) prune(path []*trieNode, edges []string) {
	for i := len(path) - 1; i > 0; i-- {
		node := path[i]
		if len(node.subs) != 0 || len(node.children) != 0 || node.plus != nil || node.hash != nil {
			return
		}
		parent := path[i-1]
		edge := edges[i-1]
		if edge == "+" {
			parent.plus = nil
		} else if edge == "#" {
			parent.hash = nil
		} else {
			delete(parent.children, edge)
		}
	}
}

// matchHit explains one routing decision: the winning filter and its owner.
type matchHit struct {
	Filter    string
	SessionID uint64
	QoS       byte
	NoLocal   bool
}

// match finds every subscription hit by a concrete topic name.
//
// Determinism rules:
//   - one session never receives the same published message twice even when
//     several of its filters match (the UID keyed set dedupes), and the
//     effective QoS is the minimum of publisher QoS and matching filters;
//   - results are sorted by (session id, filter) so callers/tests see a
//     stable, explainable order.
func (t *topicTrie) match(topic string) []matchHit {
	t.mu.RLock()
	defer t.mu.RUnlock()
	levels := packet.SplitTopic(topic)
	bySession := map[uint64]*matchHit{}
	walk(t.root, levels, 0, bySession)
	hits := make([]matchHit, 0, len(bySession))
	for _, h := range bySession {
		hits = append(hits, *h)
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].SessionID != hits[j].SessionID {
			return hits[i].SessionID < hits[j].SessionID
		}
		return hits[i].Filter < hits[j].Filter
	})
	return hits
}

func walk(node *trieNode, levels []string, i int, hits map[uint64]*matchHit) {
	// A "#" terminal here matches everything at this depth and below,
	// including the zero-levels-left case ("a/#" matches "a").
	if node.hash != nil {
		collect(node.hash, hits)
	}
	if i == len(levels) {
		collect(node, hits)
		return
	}
	lv := levels[i]
	if c := node.children[lv]; c != nil {
		walk(c, levels, i+1, hits)
	}
	if node.plus != nil {
		walk(node.plus, levels, i+1, hits)
	}
}

// collect folds all subscriptions at a terminal node into the dedup set.
func collect(node *trieNode, hits map[uint64]*matchHit) {
	for uid, sub := range node.subs {
		existing, ok := hits[uid]
		if !ok {
			hits[uid] = &matchHit{
				Filter:    sub.Filter,
				SessionID: uid,
				QoS:       sub.QoS,
				NoLocal:   sub.NoLocal,
			}
			continue
		}
		// Multiple filters of one session match: minimum QoS wins.
		if sub.QoS < existing.QoS {
			existing.QoS = sub.QoS
		}
		// Prefer the more specific (non-wildcard) filter when reporting which
		// filter explained the hit, for stable diagnostics.
		if specificity(sub.Filter) > specificity(existing.Filter) {
			existing.Filter = sub.Filter
		}
		existing.NoLocal = existing.NoLocal && sub.NoLocal
	}
}

// specificity ranks filters for diagnostic reporting: exact > + > #.
func specificity(filter string) int {
	switch {
	case strings.Contains(filter, "#"):
		return 0
	case strings.Contains(filter, "+"):
		return 1
	default:
		return 2
	}
}
