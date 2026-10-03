package ledger

import (
	"errors"
	"math"
	"testing"
)

func TestParseCurrency(t *testing.T) {
	if c, err := ParseCurrency("EUR"); err != nil || c != "EUR" {
		t.Fatalf("EUR: got %q, %v", c, err)
	}
	for _, bad := range []string{"", "eur", "EURO", "XXX"} {
		if _, err := ParseCurrency(bad); !errors.Is(err, ErrInvalidCurrency) {
			t.Errorf("ParseCurrency(%q) = %v, want ErrInvalidCurrency", bad, err)
		}
	}
}

func TestAddChecksCurrencyAndOverflow(t *testing.T) {
	eur := func(n int64) Money { return Money{Amount: n, Currency: "EUR"} }

	if sum, err := eur(150).Add(eur(-50)); err != nil || sum != eur(100) {
		t.Errorf("150 + −50 = %v, %v", sum, err)
	}
	if _, err := eur(1).Add(Money{Amount: 1, Currency: "USD"}); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("EUR + USD: %v, want ErrCurrencyMismatch", err)
	}
	if _, err := eur(math.MaxInt64).Add(eur(1)); !errors.Is(err, ErrOverflow) {
		t.Errorf("MaxInt64 + 1: %v, want ErrOverflow (not a silent wrap to a negative balance)", err)
	}
	if _, err := eur(math.MinInt64).Add(eur(-1)); !errors.Is(err, ErrOverflow) {
		t.Errorf("MinInt64 − 1: %v, want ErrOverflow", err)
	}
}

func TestNegOfTheMostNegativeAmountOverflows(t *testing.T) {
	// −math.MinInt64 doesn't fit in an int64: in plain Go it silently stays negative.
	if _, err := (Money{Amount: math.MinInt64, Currency: "EUR"}).Neg(); !errors.Is(err, ErrOverflow) {
		t.Errorf("Neg(MinInt64): %v, want ErrOverflow", err)
	}
	if _, err := (Money{Amount: 5, Currency: "EUR"}).Sub(Money{Amount: math.MinInt64, Currency: "EUR"}); !errors.Is(err, ErrOverflow) {
		t.Errorf("5 − MinInt64: %v, want ErrOverflow", err)
	}
}

func TestParseMoney(t *testing.T) {
	tests := []struct {
		in       string
		currency Currency
		want     int64
	}{
		{"12.34", "EUR", 1234},
		{"12.3", "EUR", 1230},
		{"12", "EUR", 1200},
		{"0.01", "EUR", 1},
		{"-0.5", "EUR", -50},
		{"1000", "JPY", 1000},
		{"92233720368547758.07", "EUR", math.MaxInt64},
		{"-92233720368547758.08", "EUR", math.MinInt64},
	}
	for _, tt := range tests {
		got, err := ParseMoney(tt.in, tt.currency)
		if err != nil || got.Amount != tt.want || got.Currency != tt.currency {
			t.Errorf("ParseMoney(%q, %s) = %v, %v; want %d", tt.in, tt.currency, got, err, tt.want)
		}
	}
}

func TestParseMoneyRejectsAnythingAmbiguous(t *testing.T) {
	for _, in := range []string{"", "-", ".5", "5.", "1.234", "1,00", "+1", " 1", "1e3", "0x10", "--1", "1.2.3", "１"} {
		if _, err := ParseMoney(in, "EUR"); err == nil {
			t.Errorf("ParseMoney(%q) accepted, want an error", in)
		}
	}
	if _, err := ParseMoney("1.5", "JPY"); err == nil {
		t.Error("JPY has no minor units: 1.5 must be rejected")
	}
	if _, err := ParseMoney("92233720368547758.08", "EUR"); !errors.Is(err, ErrOverflow) {
		t.Errorf("one cent above the maximum: %v, want ErrOverflow", err)
	}
}

func TestFormat(t *testing.T) {
	tests := []struct {
		m    Money
		want string
	}{
		{Money{1234, "EUR"}, "12.34 EUR"},
		{Money{-5, "EUR"}, "-0.05 EUR"},
		{Money{0, "EUR"}, "0.00 EUR"},
		{Money{1000, "JPY"}, "1000 JPY"},
		{Money{math.MinInt64, "EUR"}, "-92233720368547758.08 EUR"},
	}
	for _, tt := range tests {
		if got := tt.m.String(); got != tt.want {
			t.Errorf("%#v.String() = %q, want %q", tt.m, got, tt.want)
		}
	}
}
