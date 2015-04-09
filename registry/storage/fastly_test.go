package storage

import (
	"testing"
	"time"
)

func TestFastlyToken(t *testing.T) {
	key := "Bh5ubZSwcNTtXpXAeZ+xXoCkl9SUcUVqjzNDoaXIWhA="
	expires := time.Unix(1428365698, 0)

	path := "/path/to/data"
	actual := makeFastlyToken(path, key, expires)
	expected := "55232182_0x99cdab1d7175e5f6441a4478bcae5853c440071c"
	if expected != actual {
		t.Errorf("Token mismatch: %q != %q", actual, expected)
	}

}
