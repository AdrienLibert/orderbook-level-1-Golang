package main

import "testing"

func TestGetenvFallbackWhenUnset(t *testing.T) {
	t.Setenv("NUM_TRADERS", "")

	value := getenv("NUM_TRADERS", "10")
	if value != "10" {
		t.Fatalf("expected fallback value 10, got %q", value)
	}
}

func TestGetenvReturnsEnvValueWhenSet(t *testing.T) {
	t.Setenv("NUM_TRADERS", "42")

	value := getenv("NUM_TRADERS", "10")
	if value != "42" {
		t.Fatalf("expected env value 42, got %q", value)
	}
}
