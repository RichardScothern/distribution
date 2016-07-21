package metadata

import (
	"fmt"
	"github.com/docker/distribution"
	"github.com/docker/distribution/context"
	"github.com/docker/distribution/digest"
	"github.com/docker/distribution/reference"
)

func NewTagService(repo reference.Named, md MetadataService) distribution.TagService {
	return TagService{
		store: md,
		repo:  repo,
	}
}

var _ distribution.TagService = &TagService{}

type TagService struct {
	repo  reference.Named
	store MetadataService
}

func (ts TagService) Get(ctx context.Context, tag string) (distribution.Descriptor, error) {
	key := TagKey{Tag: tag}.String()
	val, err := ts.store.Get(ctx, key)
	if err != nil {
		return distribution.Descriptor{}, err
	}
	if val == nil {
		return distribution.Descriptor{}, distribution.ErrTagUnknown{Tag: tag}
	}
	if desc, ok := val.(distribution.Descriptor); ok {
		return desc, nil
	}

	return distribution.Descriptor{}, fmt.Errorf("incorrect type for tag get: %s (%T)", val, val)
}

// Tag associates the tag with the provided descriptor, updating the
// current association, if needed.
func (ts TagService) Tag(ctx context.Context, tag string, desc distribution.Descriptor) error {
	_, err := digest.ParseDigest(string(desc.Digest))
	if err != nil {
		return err
	}
	key := TagKey{Tag: tag}.String()
	tx := GetTx(ctx)
	if tx == nil {
		return ts.store.Put(ctx, key, desc)
	}

	err = tx.Update(ctx, key, desc)
	if err != nil {
		return err
	}
	return nil
}

// Untag removes the given tag association
func (ts TagService) Untag(ctx context.Context, tag string) error {
	key := TagKey{Tag: tag}.String()
	tx := GetTx(ctx)
	if tx == nil {
		return ts.store.Delete(ctx, key)
	}

	err := tx.Update(ctx, key, nil)
	if err != nil {
		return err
	}

	return nil
}

// All returns the set of tags managed by this tag service
func (ts TagService) All(ctx context.Context) ([]string, error) {
	var tags []string

	params := IterateParameters{IterType: TagKey{}}
	err := ts.store.Iterate(ctx, params, func(key string, val interface{}) error {
		tags = append(tags, key)
		return nil
	})

	return tags, err
}

// Lookup returns the set of tags referencing the given digest.
func (ts TagService) Lookup(ctx context.Context, desc distribution.Descriptor) ([]string, error) {
	var tags []string
	params := IterateParameters{IterType: TagKey{}}
	err := ts.store.Iterate(ctx, params, func(key string, val interface{}) error {
		if d, ok := val.(distribution.Descriptor); ok {
			if desc.Digest == d.Digest {
				tags = append(tags, key)
			}
			return nil
		}
		return fmt.Errorf("incorrect type %T for key %s", val, val)
	})
	switch err.(type) {
	case distribution.ErrRepositoryUnknown:
		// The tag service has been initialized but not yet populated
		break
	case nil:
		break
	default:
		return nil, err
	}
	return tags, nil
}
