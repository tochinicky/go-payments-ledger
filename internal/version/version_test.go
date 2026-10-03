package version

import "testing"

func TestStringWithoutBuildFlags(t *testing.T) {
	if got, want := String(), "dev (unknown)"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestStringUsesBuildValues(t *testing.T) {
	// Restore the package variables after the test, so other tests see the defaults.
	oldVersion, oldCommit := Version, Commit
	t.Cleanup(func() { Version, Commit = oldVersion, oldCommit })

	Version, Commit = "v0.1.0", "a1b2c3d"

	if got, want := String(), "v0.1.0 (a1b2c3d)"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}
