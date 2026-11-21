package main

import "testing"

// TestTruncate pins byte-boundary truncation: the result never splits a
// multi-byte rune and never exceeds max bytes.
func TestTruncate(t *testing.T) {
	cases := []struct {
		name string
		in   string
		max  int
		want string
	}{
		{"empty", "", 10, ""},
		{"within limit", "abc", 10, "abc"},
		{"exact limit", "abcdef", 6, "abcdef"},
		{"ascii cut", "abcdef", 3, "abc"},
		{"chinese exact", "你好世界", 6, "你好"},
		{"chinese split rune", "你好世界", 5, "你"},
		{"chinese one rune", "你好世界", 2, ""},
		{"emoji split", "🙂🙂", 5, "🙂"},
		{"emoji after ascii", "a🙂", 4, "a"},
		{"emoji full", "a🙂", 5, "a🙂"},
		{"zero max", "abc", 0, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := truncate(c.in, c.max); got != c.want {
				t.Fatalf("truncate(%q, %d) = %q, want %q", c.in, c.max, got, c.want)
			}
		})
	}
}
