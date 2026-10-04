package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/google/uuid"
)

func (c client) placeHold(from, to string, amount int) (int, map[string]any) {
	c.t.Helper()
	body, _ := json.Marshal(map[string]any{"account": from, "to_account": to, "amount_minor": amount, "currency": "EUR", "expires_in": 600})
	return c.do("POST", "/v1/holds", string(body))
}

func TestHoldPlaceCaptureAndRelease(t *testing.T) {
	c := newClient(t, 10_000)
	alice, shop := c.openAccount(), c.openAccount()
	c.transfer(c.settlement.String(), alice, 1_000)

	status, h := c.placeHold(alice, shop, 300)
	if status != http.StatusCreated || h["status"] != "active" || h["to_account"] != shop {
		t.Fatalf("place: %d %v", status, h)
	}
	_, bal := c.do("GET", "/v1/accounts/"+alice+"/balance", "")
	if bal["held_minor"] != 300.0 || bal["available_minor"] != 700.0 {
		t.Fatalf("balance with the hold: %v", bal)
	}
	id := h["id"].(string)
	status, captured := c.do("POST", "/v1/holds/"+id+"/capture", `{"amount_minor":250}`)
	if status != http.StatusOK || captured["status"] != "captured" || captured["captured_minor"] != 250.0 || captured["transaction_id"] == nil {
		t.Fatalf("capture: %d %v", status, captured)
	}
	_, bal = c.do("GET", "/v1/accounts/"+alice+"/balance", "")
	if bal["posted_minor"] != 750.0 || bal["held_minor"] != 0.0 {
		t.Fatalf("balance after capture: %v", bal)
	}
	if status, got := c.do("GET", "/v1/holds/"+id, ""); status != http.StatusOK || got["status"] != "captured" {
		t.Fatalf("get: %d %v", status, got)
	}

	_, h2 := c.placeHold(alice, shop, 100)
	id2 := h2["id"].(string)
	if resp := c.send("POST", "/v1/holds/"+id2+"/release", "", uuid.NewString()); resp.status != http.StatusOK || !bytes.Contains(resp.body, []byte(`"released"`)) {
		t.Fatalf("release with an empty body: %d %s", resp.status, resp.body)
	}
}

func TestHoldErrors(t *testing.T) {
	c := newClient(t, 10_000)
	other := newClient(t, 0)
	alice, shop := c.openAccount(), c.openAccount()
	c.transfer(c.settlement.String(), alice, 100)
	_, h := c.placeHold(alice, shop, 60)
	id := h["id"].(string)

	tests := []struct {
		name               string
		client             client
		method, path, body string
		status             int
		code               string
	}{
		{"more than available", c, "POST", "/v1/holds", `{"account":"` + alice + `","to_account":"` + shop + `","amount_minor":41,"currency":"EUR","expires_in":60}`, 422, "insufficient_funds"},
		{"no expiry", c, "POST", "/v1/holds", `{"account":"` + alice + `","to_account":"` + shop + `","amount_minor":1,"currency":"EUR"}`, 400, "validation_failed"},
		{"capture more than held", c, "POST", "/v1/holds/" + id + "/capture", `{"amount_minor":61}`, 422, "capture_exceeds_hold"},
		{"another partner's hold", other, "POST", "/v1/holds/" + id + "/capture", `{"amount_minor":1}`, 404, "not_found"},
		{"another partner reads it", other, "GET", "/v1/holds/" + id, "", 404, "not_found"},
		{"release with fields", c, "POST", "/v1/holds/" + id + "/release", `{"why":"x"}`, 400, "malformed_json"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.client.t = t
			status, body := tt.client.do(tt.method, tt.path, tt.body)
			if status != tt.status || body["code"] != tt.code {
				t.Fatalf("%d %v, want %d %s", status, body, tt.status, tt.code)
			}
		})
	}

	if status, _ := c.do("POST", "/v1/holds/"+id+"/release", "{}"); status != http.StatusOK {
		t.Fatalf("release: %d", status)
	}
	if status, body := c.do("POST", "/v1/holds/"+id+"/capture", `{"amount_minor":1}`); status != http.StatusConflict || body["code"] != "hold_not_active" {
		t.Fatalf("capture after release: %d %v", status, body)
	}
}

// A capture retried with the same key replays the stored answer and never posts twice.
func TestCaptureIsIdempotent(t *testing.T) {
	c := newClient(t, 10_000)
	alice, shop := c.openAccount(), c.openAccount()
	c.transfer(c.settlement.String(), alice, 1_000)
	_, h := c.placeHold(alice, shop, 300)
	key, path := uuid.NewString(), "/v1/holds/"+h["id"].(string)+"/capture"
	first := c.send("POST", path, `{"amount_minor":100}`, key)
	retry := c.send("POST", path, `{"amount_minor":100}`, key)
	if first.status != http.StatusOK || retry.status != first.status || !bytes.Equal(first.body, retry.body) {
		t.Fatalf("first %d %s, retry %d %s", first.status, first.body, retry.status, retry.body)
	}
	var n int
	if err := tdb.Owner.QueryRow(context.Background(), "SELECT count(*) FROM postings WHERE account_id = $1", shop).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("%d postings to the shop, want 1", n)
	}
}
