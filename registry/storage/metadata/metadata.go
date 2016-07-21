package metadata

import (
	"fmt"
	"github.com/docker/distribution/context"
	"github.com/docker/distribution/reference"
)

type ErrKeyNotFound struct {
	Key string
}

func (err ErrKeyNotFound) Error() string {
	return fmt.Sprintf("metadata key not found: %s", err.Key)
}

// Metadatable signifies metadata support
type Metadatable interface {
	MetadataService() MetadataService
}

type MetadataUpdateRecord struct {
	Actual   interface{}
	Expected interface{}
}

// IterFunc is called by Iterate.  A non-nil error stops iteration
type IterFunc func(string, interface{}) error

// MetadataService defines the operations of a metadata service
type MetadataService interface {
	// Put puts a value under a key
	Put(context.Context, string, interface{}) error

	// BatchPut performs a transactional put of multiple key value pairs
	BatchPut(context.Context, map[string]MetadataUpdateRecord) error

	// Get gets a value for a given key
	Get(context.Context, string) (interface{}, error)

	// Delete removes a value for a given key.  Returns an error if the key doesn't exist
	Delete(context.Context, string) error

	// Exists returns true if the key exists
	Exists(context.Context, string) (bool, error)

	// Iterate metadata starting at a key, calling IterFunc on each item
	Iterate(context.Context, IterateParameters, IterFunc) error

	// Return an instance of this metadata service scoped to a repository
	RepositoryScoped(MetadataService, reference.Named) MetadataService
}

// IterateParameters specify the type of metdata to be iterated and
// which lexicographical position to start from
type IterateParameters struct {
	IterType iterable
	From     string
}
