package ledger

import (
	"math"
	"math/big"
	"testing"
)

// FuzzParseMoney: whatever the input, parsing never panics, and anything it accepts formats back to a string that
// parses to the same amount (no value is read one way and written another).
func FuzzParseMoney(f *testing.F) {
	for _, seed := range []string{"12.34", "-0.5", "0", "92233720368547758.07", "-92233720368547758.08", "1.234", "", "-", "1e9"} {
		f.Add(seed, true)
		f.Add(seed, false)
	}
	f.Fuzz(func(t *testing.T, in string, eur bool) {
		currency := Currency("JPY")
		if eur {
			currency = "EUR"
		}
		m, err := ParseMoney(in, currency)
		if err != nil {
			return
		}
		again, err := ParseMoney(FormatAmount(m.Amount, currency.MinorUnits()), currency)
		if err != nil || again != m {
			t.Fatalf("%q parsed to %v, which formats and re-parses to %v (%v)", in, m, again, err)
		}
	})
}

// FuzzAdd: checked addition agrees with exact (big integer) arithmetic: it succeeds exactly when the true sum
// fits in an int64, and then gives the true sum.
func FuzzAdd(f *testing.F) {
	f.Add(int64(1), int64(2))
	f.Add(int64(math.MaxInt64), int64(1))
	f.Add(int64(math.MinInt64), int64(-1))
	f.Add(int64(math.MinInt64), int64(math.MaxInt64))
	f.Fuzz(func(t *testing.T, a, b int64) {
		exact := new(big.Int).Add(big.NewInt(a), big.NewInt(b))
		fits := exact.IsInt64()

		sum, err := Money{a, "EUR"}.Add(Money{b, "EUR"})

		if fits != (err == nil) {
			t.Fatalf("%d + %d: exact %s, fits=%v, but err=%v", a, b, exact, fits, err)
		}
		if fits && sum.Amount != exact.Int64() {
			t.Fatalf("%d + %d = %d, want %s", a, b, sum.Amount, exact)
		}
	})
}
