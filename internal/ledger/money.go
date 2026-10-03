package ledger

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Currency is an ISO 4217 code, e.g. "EUR".
type Currency string

// minorUnits is how many decimal places each supported currency has: 1 EUR = 100 cents, 1 JPY = 1 yen.
// The ledger stores amounts as an integer count of these minor units. Floats are never used for money:
// 0.1 + 0.2 != 0.3 in binary floating point, and a ledger must add up exactly.
var minorUnits = map[Currency]int{"EUR": 2, "USD": 2, "GBP": 2, "CHF": 2, "JPY": 0}

// ParseCurrency validates a currency code.
func ParseCurrency(code string) (Currency, error) {
	c := Currency(code)
	if _, ok := minorUnits[c]; !ok {
		return "", newError(ErrInvalidCurrency, fmt.Sprintf("currency %q is not supported", code))
	}
	return c, nil
}

// MinorUnits returns the number of decimal places of c (2 for EUR).
func (c Currency) MinorUnits() int { return minorUnits[c] }

// Money is an amount in minor units (cents for EUR) of one currency. Arithmetic is checked: an operation that would
// overflow int64 returns an error instead of silently wrapping round to a negative number.
type Money struct {
	Amount   int64
	Currency Currency
}

// New returns money in minor units of a supported currency.
func New(amount int64, currency Currency) (Money, error) {
	if _, ok := minorUnits[currency]; !ok {
		return Money{}, newError(ErrInvalidCurrency, fmt.Sprintf("currency %q is not supported", currency))
	}
	return Money{Amount: amount, Currency: currency}, nil
}

// Add returns m + other. Both must be the same currency.
func (m Money) Add(other Money) (Money, error) {
	if m.Currency != other.Currency {
		return Money{}, newError(ErrCurrencyMismatch, fmt.Sprintf("can't add %s to %s", other.Currency, m.Currency))
	}
	sum, ok := addInt64(m.Amount, other.Amount)
	if !ok {
		return Money{}, ErrOverflow
	}
	return Money{Amount: sum, Currency: m.Currency}, nil
}

// Sub returns m − other. Both must be the same currency.
func (m Money) Sub(other Money) (Money, error) {
	neg, err := other.Neg()
	if err != nil {
		return Money{}, err
	}
	return m.Add(neg)
}

// Neg returns −m. It fails only for the one int64 that has no positive counterpart (math.MinInt64).
func (m Money) Neg() (Money, error) {
	if m.Amount == math.MinInt64 {
		return Money{}, ErrOverflow
	}
	return Money{Amount: -m.Amount, Currency: m.Currency}, nil
}

// IsPositive reports whether the amount is greater than zero.
func (m Money) IsPositive() bool { return m.Amount > 0 }

// String formats as "12.34 EUR" (decimal point, no thousands separators).
func (m Money) String() string {
	return FormatAmount(m.Amount, m.Currency.MinorUnits()) + " " + string(m.Currency)
}

// FormatAmount writes minor units as a decimal string with the given number of decimal places: 1234, 2 → "12.34".
// It works on the decimal digits as text, so even math.MinInt64 (which has no positive int64) formats correctly.
func FormatAmount(amount int64, decimals int) string {
	digits := strconv.FormatInt(amount, 10)
	sign := ""
	if strings.HasPrefix(digits, "-") {
		sign, digits = "-", digits[1:]
	}
	if len(digits) <= decimals { // pad so there's at least one digit before the point: 5 → "0.05"
		digits = strings.Repeat("0", decimals-len(digits)+1) + digits
	}
	if decimals == 0 {
		return sign + digits
	}
	return sign + digits[:len(digits)-decimals] + "." + digits[len(digits)-decimals:]
}

// ParseMoney reads a decimal amount like "12.34" or "-0.5" into minor units of currency. It accepts an optional
// leading "-", digits, and at most the currency's number of decimal places. No "+", spaces, exponents or
// thousands separators: the API takes amount_minor as an integer, so this is for the CLI and tests, and strictness
// is safer than guessing.
func ParseMoney(s string, currency Currency) (Money, error) {
	decimals, ok := minorUnits[currency]
	if !ok {
		return Money{}, newError(ErrInvalidCurrency, fmt.Sprintf("currency %q is not supported", currency))
	}
	invalid := func() (Money, error) {
		return Money{}, newError(ErrInvalidAmount, fmt.Sprintf("%q is not a valid %s amount", s, currency))
	}

	negative := strings.HasPrefix(s, "-")
	body := strings.TrimPrefix(s, "-")
	whole, frac, hasPoint := strings.Cut(body, ".")
	if whole == "" || !allDigits(whole) || (hasPoint && (frac == "" || !allDigits(frac))) || len(frac) > decimals {
		return invalid()
	}
	frac += strings.Repeat("0", decimals-len(frac))

	// Accumulate as a negative number: int64 has one more negative value than positive, so this also accepts
	// the most negative amount without overflowing on the way.
	var amount int64
	for _, r := range whole + frac {
		next, ok := mulInt64(amount, 10)
		if !ok {
			return Money{}, ErrOverflow
		}
		next, ok = addInt64(next, -int64(r-'0'))
		if !ok {
			return Money{}, ErrOverflow
		}
		amount = next
	}
	if !negative {
		if amount == math.MinInt64 {
			return Money{}, ErrOverflow
		}
		amount = -amount
	}
	return Money{Amount: amount, Currency: currency}, nil
}

func allDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return s != ""
}

// addInt64 adds with overflow detection: ok is false if the true sum doesn't fit in an int64.
func addInt64(a, b int64) (int64, bool) {
	if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
		return 0, false
	}
	return a + b, true
}

func mulInt64(a, b int64) (int64, bool) {
	if a == 0 || b == 0 {
		return 0, true
	}
	product := a * b
	if product/b != a || (a == -1 && b == math.MinInt64) || (b == -1 && a == math.MinInt64) {
		return 0, false
	}
	return product, true
}
