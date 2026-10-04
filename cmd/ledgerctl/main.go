// Command ledgerctl administers the ledger and talks to its API.
//
// Admin commands connect to the database as the owner role. Credentials come from the environment only
// (DATABASE_URL), never from flags, so they don't end up in shell history or process listings:
//
//	ledgerctl partner create --name NAME --funding-limit MINOR [--currencies EUR,USD]
//	ledgerctl partner rotate-key --partner ID
//	ledgerctl outbox status
//
// "partner create" and "rotate-key" print the new API key once, on stdout. Only its SHA-256 is stored; it can't
// be shown again.
//
// API commands use LEDGER_URL (default http://localhost:8080) and LEDGER_API_KEY, and print the JSON answer. Writes
// send a fresh Idempotency-Key unless --idempotency-key is given (give the same one to retry safely):
//
//	ledgerctl account open --currency EUR [--customer-ref REF]
//	ledgerctl account show|balance|statement --account ID
//	ledgerctl transfer --from ID --to ID --amount MINOR --currency EUR [--reference TEXT]
//	ledgerctl hold place --account ID --to ID --amount MINOR --currency EUR --expires-in SECONDS
//	ledgerctl hold capture --hold ID --amount MINOR
//	ledgerctl hold release --hold ID
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tochinicky/go-payments-ledger/internal/ledger"
	"github.com/tochinicky/go-payments-ledger/internal/store"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "ledgerctl:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) < 1 {
		return errors.New("usage: ledgerctl partner|outbox|account|transfer|hold ... (see the package documentation)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd, rest := args[0], args[1:]
	sub := ""
	if len(rest) > 0 && !strings.HasPrefix(rest[0], "-") {
		sub, rest = rest[0], rest[1:]
	}
	switch cmd + " " + sub {
	case "partner create", "partner rotate-key", "outbox status":
		return admin(ctx, cmd+" "+sub, rest)
	default:
		return client(ctx, cmd, sub, rest)
	}
}

func admin(ctx context.Context, cmd string, args []string) error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL (the owner role) is not set")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	st := store.New(pool, store.NewID)
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	switch cmd {
	case "partner create":
		name := fs.String("name", "", "partner name")
		limit := fs.Int64("funding-limit", 0, "how far the settlement accounts may go below zero, in minor units")
		currencies := fs.String("currencies", "EUR", "comma-separated currencies to open settlement accounts in")
		if err := fs.Parse(args); err != nil {
			return err
		}
		if *name == "" {
			return errors.New("--name is required")
		}
		var cs []ledger.Currency
		for _, c := range strings.Split(*currencies, ",") {
			cur, err := ledger.ParseCurrency(strings.TrimSpace(c))
			if err != nil {
				return err
			}
			cs = append(cs, cur)
		}
		key, hash, err := store.NewAPIKey()
		if err != nil {
			return err
		}
		p, err := st.CreatePartner(ctx, *name, hash, *limit, cs)
		if err != nil {
			return err
		}
		return printJSON(map[string]any{"partner_id": p.ID, "api_key": key, "note": "store the key now: it is shown only once"})
	case "partner rotate-key":
		id := fs.String("partner", "", "partner id")
		if err := fs.Parse(args); err != nil {
			return err
		}
		partner, err := uuid.Parse(*id)
		if err != nil {
			return fmt.Errorf("--partner: %w", err)
		}
		key, hash, err := store.NewAPIKey()
		if err != nil {
			return err
		}
		if err := st.RotatePartnerKey(ctx, partner, hash); err != nil {
			return err
		}
		return printJSON(map[string]any{"partner_id": partner, "api_key": key, "note": "the old key no longer works; store this one now"})
	default: // outbox status
		s, err := st.Outbox(ctx)
		if err != nil {
			return err
		}
		return printJSON(map[string]any{"events": s.Total, "unpublished": s.Unpublished, "oldest_unpublished_seconds": s.OldestUnpublishedSeconds})
	}
}

func client(ctx context.Context, cmd, sub string, args []string) error {
	base := os.Getenv("LEDGER_URL")
	if base == "" {
		base = "http://localhost:8080"
	}
	key := os.Getenv("LEDGER_API_KEY")
	if key == "" {
		return errors.New("LEDGER_API_KEY is not set")
	}
	fs := flag.NewFlagSet(cmd+" "+sub, flag.ContinueOnError)
	idem := fs.String("idempotency-key", "", "reuse this key to retry a write safely (default: a new one)")
	account := fs.String("account", "", "account id")
	to := fs.String("to", "", "destination account id")
	from := fs.String("from", "", "source account id")
	hold := fs.String("hold", "", "hold id")
	amount := fs.Int64("amount", 0, "amount in minor units")
	currency := fs.String("currency", "EUR", "currency")
	reference := fs.String("reference", "", "free-text reference")
	customerRef := fs.String("customer-ref", "", "your customer's reference")
	expiresIn := fs.Int64("expires-in", 3600, "hold lifetime in seconds")
	if err := fs.Parse(args); err != nil {
		return err
	}
	opt := func(s string) *string {
		if s == "" {
			return nil
		}
		return &s
	}
	var method, path string
	var body any
	switch cmd + " " + sub {
	case "account open":
		method, path, body = "POST", "/v1/accounts", map[string]any{"currency": *currency, "customer_ref": opt(*customerRef)}
	case "account show":
		method, path = "GET", "/v1/accounts/"+*account
	case "account balance":
		method, path = "GET", "/v1/accounts/"+*account+"/balance"
	case "account statement":
		method, path = "GET", "/v1/accounts/"+*account+"/statement"
	case "transfer ":
		method, path, body = "POST", "/v1/transfers", map[string]any{
			"from": *from, "to": *to, "amount_minor": *amount, "currency": *currency, "reference": opt(*reference)}
	case "hold place":
		method, path, body = "POST", "/v1/holds", map[string]any{
			"account": *account, "to_account": *to, "amount_minor": *amount, "currency": *currency, "expires_in": *expiresIn}
	case "hold capture":
		method, path, body = "POST", "/v1/holds/"+*hold+"/capture", map[string]any{"amount_minor": *amount}
	case "hold release":
		method, path, body = "POST", "/v1/holds/"+*hold+"/release", map[string]any{}
	default:
		return fmt.Errorf("unknown command %q", strings.TrimSpace(cmd+" "+sub))
	}
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = strings.NewReader(string(b))
	}
	// The URL is the operator's own LEDGER_URL: a CLI calling where it's told, not a server following user input.
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(base, "/")+path, reader) //nolint:gosec // see above
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+key)
	if method == "POST" {
		if *idem == "" {
			*idem = uuid.NewString()
		}
		req.Header.Set("Idempotency-Key", *idem)
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req) //nolint:gosec // G704: LEDGER_URL is the operator's own setting
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "%d %s (Idempotency-Key %s)\n", resp.StatusCode, http.StatusText(resp.StatusCode), *idem)
	_, _ = os.Stdout.Write(out)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("the API answered %d", resp.StatusCode)
	}
	return nil
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
