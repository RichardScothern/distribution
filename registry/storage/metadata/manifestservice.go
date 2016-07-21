package metadata

import (
	"github.com/docker/distribution"
	"github.com/docker/distribution/context"
	"github.com/docker/distribution/digest"
	"github.com/docker/distribution/reference"
)

type manifestService struct {
	ctx           context.Context
	repo          reference.Named
	store         MetadataService
	manifestStore distribution.ManifestService
	deleteEnabled bool
}

var _ distribution.ManifestService = &manifestService{}

func NewManifestService(ctx context.Context, repo reference.Named, md MetadataService, ms distribution.ManifestService, deleteEnabled bool) (distribution.ManifestService, error) {
	return &manifestService{
		ctx:           ctx,
		repo:          repo,
		store:         md,
		manifestStore: ms,
		deleteEnabled: deleteEnabled,
	}, nil
}

// Exists returns true if the manifest exists.
func (t manifestService) Exists(ctx context.Context, dgst digest.Digest) (bool, error) {
	key := ManifestDigestKey{Dgst: dgst}.String()
	return t.store.Exists(ctx, key)
}

// Get retrieves the manifest specified by the given digest
func (t manifestService) Get(ctx context.Context, dgst digest.Digest, options ...distribution.ManifestServiceOption) (distribution.Manifest, error) {
	exists, err := t.Exists(ctx, dgst)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, distribution.ErrManifestUnknownRevision{
			Name:     t.repo.Name(),
			Revision: dgst,
		}
	}
	return t.manifestStore.Get(ctx, dgst, options...)
}

// Put creates or updates the given manifest returning the manifest digest
func (t manifestService) Put(ctx context.Context, manifest distribution.Manifest, options ...distribution.ManifestServiceOption) (digest.Digest, error) {
	dgst, err := t.manifestStore.Put(ctx, manifest, options...)
	if err != nil {
		return "", err
	}

	key := ManifestDigestKey{Dgst: dgst}.String()
	tx := GetTx(ctx)
	if tx == nil {
		return dgst, t.store.Put(ctx, key, dgst)
	}

	_, payload, err := manifest.Payload()
	if err != nil {
		return "", err
	}

	err = tx.Update(ctx, key, payload)
	if err != nil {
		return dgst, err
	}
	return dgst, nil
}

// Delete removes the manifest specified by the given digest. Deleting
// a manifest that doesn't exist will return ErrManifestNotFound
func (t manifestService) Delete(ctx context.Context, dgst digest.Digest) error {
	if !t.deleteEnabled {
		return distribution.ErrUnsupported
	}

	exists, err := t.Exists(ctx, dgst)
	if err != nil {
		return err
	}
	if !exists {
		return distribution.ErrBlobUnknown
	}
	key := ManifestDigestKey{Dgst: dgst}.String()
	tx := GetTx(ctx)
	if tx == nil {
		return t.store.Delete(ctx, key)
	}

	err = tx.Update(ctx, key, nil)
	if err != nil {
		return err
	}

	return nil
}

func (t manifestService) Enumerate(ctx context.Context, f func(dgst digest.Digest) error) error {
	params := IterateParameters{IterType: ManifestDigestKey{}}
	err := t.store.Iterate(ctx, params, func(key string, val interface{}) error {
		dgst, err := digest.ParseDigest(key)
		if err != nil {
			return err
		}

		return f(dgst)

	})
	return err
}
