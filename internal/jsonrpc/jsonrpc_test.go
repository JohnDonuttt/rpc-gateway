package jsonrpc

import (
	"encoding/json"
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		wantCount int
		wantBatch bool
		wantErr   bool
	}{
		{"single", `{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}`, 1, false, false},
		{"batch", `[{"id":1,"method":"a"},{"id":2,"method":"b"}]`, 2, true, false},
		{"whitespace around", "  \n{\"id\":1,\"method\":\"a\"}\n", 1, false, false},
		{"empty body", ``, 0, false, true},
		{"empty batch", `[]`, 0, true, true},
		{"not json", `hello`, 0, false, true},
		{"missing method", `{"id":1}`, 0, false, true},
		{"batch with a bad entry", `[{"id":1,"method":"a"},{"id":2}]`, 0, true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reqs, batch, err := Parse([]byte(tt.body))
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if len(reqs) != tt.wantCount || batch != tt.wantBatch {
				t.Fatalf("got %d reqs batch=%v, want %d batch=%v", len(reqs), batch, tt.wantCount, tt.wantBatch)
			}
		})
	}
}

func TestParsePreservesIDType(t *testing.T) {
	for _, id := range []string{`1`, `"abc"`, `null`} {
		reqs, _, err := Parse([]byte(`{"id":` + id + `,"method":"m"}`))
		if err != nil {
			t.Fatal(err)
		}
		if string(reqs[0].ID) != id {
			t.Errorf("id = %s, want %s", reqs[0].ID, id)
		}
	}
}

func TestErrorBodyDefaultsIDToNull(t *testing.T) {
	var resp Response
	if err := json.Unmarshal(ErrorBody(nil, CodeParseError, "bad"), &resp); err != nil {
		t.Fatal(err)
	}
	if string(resp.ID) != "null" || resp.Error.Code != CodeParseError {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestResultBodyKeepsIDAndResult(t *testing.T) {
	body := ResultBody(json.RawMessage(`"abc"`), json.RawMessage(`"0x1"`))
	if string(body) != `{"jsonrpc":"2.0","result":"0x1","id":"abc"}` {
		t.Fatalf("got %s", body)
	}
}

func TestIsNull(t *testing.T) {
	for raw, want := range map[string]bool{"null": true, " null ": true, "": true, `"0x1"`: false, "{}": false} {
		if got := IsNull(json.RawMessage(raw)); got != want {
			t.Errorf("IsNull(%q) = %v, want %v", raw, got, want)
		}
	}
}
