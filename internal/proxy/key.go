package proxy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"
)

// Conversation keys. A request is hashed as a chain: a header link (dialect,
// system prompt, tools, the settings that change the rendered prompt), then
// one link per message. A later turn repeats the earlier turn's messages, so
// its keys start with the earlier turn's keys, and the earlier request's last
// key names the state its turn left in the engine. Keys are guesses: Strata
// matches tokens itself, so a wrong guess costs speed, never a wrong answer.

// extends reports whether keys continue (or repeat) the turn whose keys were turn.
func extends(keys, turn []string) bool {
	return len(turn) > 0 && len(turn) <= len(keys) && keys[len(turn)-1] == turn[len(turn)-1]
}

// keyer hashes requests. Each message's canonical hash is memoized by the
// hash of its raw bytes, so a long conversation is parsed once, not per turn.
type keyer struct {
	mu   sync.Mutex
	memo map[[32]byte][32]byte
}

const memoSize = 16384

// keys returns a request's prefix keys, or false when it can't be keyed: not
// JSON, no messages, or one that depends on server-side state.
func (k *keyer) keys(path string, raw []byte) ([]string, bool) {
	var b struct {
		Messages, Input, System, Instructions, Tools json.RawMessage
		ReasoningEffort                              json.RawMessage `json:"reasoning_effort"`
		Thinking                                     json.RawMessage
		ESP                                          json.RawMessage `json:"experimental_speed_projection"`
		TemplateKwargs                               json.RawMessage `json:"chat_template_kwargs"`
		PreviousResponseID                           json.RawMessage `json:"previous_response_id"`
	}
	if json.Unmarshal(raw, &b) != nil || isSet(b.PreviousResponseID) {
		return nil, false
	}
	list := b.Messages
	if path == "/v1/responses" {
		list = b.Input
	}
	var msgs []json.RawMessage
	if path == "/v1/responses" && len(list) > 0 && list[0] == '"' { // "input": "text"
		msgs = []json.RawMessage{list}
	} else if json.Unmarshal(list, &msgs) != nil || len(msgs) == 0 {
		return nil, false
	}

	h := sha256.New()
	h.Write([]byte("strata-diskcache v1\x00" + path))
	for i, f := range []json.RawMessage{b.System, b.Instructions, b.Tools, b.ReasoningEffort, b.Thinking, b.ESP, b.TemplateKwargs} {
		if isSet(f) {
			c := k.canon(f)
			h.Write([]byte{byte(i)})
			h.Write(c[:])
		}
	}
	var cur [32]byte
	h.Sum(cur[:0])
	keys := make([]string, len(msgs))
	for i, m := range msgs {
		c := k.canon(m)
		cur = sha256.Sum256(append(cur[:], c[:]...))
		keys[i] = hex.EncodeToString(cur[:])
	}
	return keys, true
}

func isSet(raw json.RawMessage) bool { return len(raw) > 0 && !bytes.Equal(raw, []byte("null")) }

// canon hashes v's canonical form: keys sorted, whitespace gone, and every
// "cache_control" dropped (clients move those markers between turns; they
// never reach the prompt).
func (k *keyer) canon(raw json.RawMessage) [32]byte {
	rh := sha256.Sum256(raw)
	k.mu.Lock()
	c, ok := k.memo[rh]
	k.mu.Unlock()
	if ok {
		return c
	}
	c = rh
	var v any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if d.Decode(&v) == nil {
		if enc, err := json.Marshal(strip(v)); err == nil {
			c = sha256.Sum256(enc)
		}
	}
	k.mu.Lock()
	if k.memo == nil || len(k.memo) >= memoSize {
		k.memo = map[[32]byte][32]byte{}
	}
	k.memo[rh] = c
	k.mu.Unlock()
	return c
}

func strip(v any) any {
	switch t := v.(type) {
	case map[string]any:
		delete(t, "cache_control")
		for key, x := range t {
			t[key] = strip(x)
		}
	case []any:
		for i, x := range t {
			t[i] = strip(x)
		}
	}
	return v
}
