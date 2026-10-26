// Package jsonrpc holds the few JSON-RPC 2.0 types the gateway needs.
package jsonrpc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

// Standard JSON-RPC error codes, plus -32005 which most EVM providers use for rate limits.
const (
	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeInternalError  = -32603
	CodeLimitExceeded  = -32005
)

type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
	ID      json.RawMessage `json:"id,omitempty"`
}

type Error struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

type Response struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *Error          `json:"error,omitempty"`
	ID      json.RawMessage `json:"id"`
}

var ErrEmpty = errors.New("empty request")

// Parse accepts either a single request object or a batch array and reports which it was.
func Parse(body []byte) (reqs []Request, batch bool, err error) {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return nil, false, ErrEmpty
	}

	if body[0] == '[' {
		if err := json.Unmarshal(body, &reqs); err != nil {
			return nil, true, err
		}
		if len(reqs) == 0 {
			return nil, true, ErrEmpty
		}
		batch = true
	} else {
		var r Request
		if err := json.Unmarshal(body, &r); err != nil {
			return nil, false, err
		}
		reqs = []Request{r}
	}

	for i, r := range reqs {
		if r.Method == "" {
			return nil, batch, fmt.Errorf("request %d has no method", i)
		}
	}
	return reqs, batch, nil
}

// ErrorBody builds an error response. A missing id is sent back as null, per the spec.
func ErrorBody(id json.RawMessage, code int, message string) []byte {
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	b, _ := json.Marshal(Response{JSONRPC: "2.0", Error: &Error{Code: code, Message: message}, ID: id})
	return b
}

// ResultBody wraps an already-encoded result for the given request id.
func ResultBody(id json.RawMessage, result json.RawMessage) []byte {
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	b, _ := json.Marshal(Response{JSONRPC: "2.0", Result: result, ID: id})
	return b
}

// IsNull reports whether a result is JSON null (e.g. a receipt that doesn't exist yet).
func IsNull(raw json.RawMessage) bool {
	return len(raw) == 0 || string(bytes.TrimSpace(raw)) == "null"
}
