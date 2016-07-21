package metadata

import (
	"github.com/docker/distribution"
	"github.com/docker/distribution/context"
	"github.com/docker/distribution/digest"
	"github.com/docker/distribution/reference"
)

type metadataBlobStatter struct {
	metadataService MetadataService
	repository      reference.Named
	statter         distribution.BlobStatter
}

var _ distribution.BlobDescriptorService = &metadataBlobStatter{}

func NewBlobStatter(ms MetadataService, repository reference.Named, statter distribution.BlobStatter) distribution.BlobDescriptorService {
	return &metadataBlobStatter{
		metadataService: ms,
		repository:      repository,
		statter:         statter,
	}
}

func (mbs *metadataBlobStatter) Stat(ctx context.Context, dgst digest.Digest) (distribution.Descriptor, error) {
	dgst, err := digest.ParseDigest(string(dgst))
	if err != nil {
		return distribution.Descriptor{}, err
	}

	key := BlobKey{Dgst: dgst}.String()
	val, err := mbs.metadataService.Get(ctx, key)
	if err != nil {
		return distribution.Descriptor{}, err
	}

	if val == nil {
		return distribution.Descriptor{}, distribution.ErrBlobUnknown
	}

	return mbs.statter.Stat(ctx, dgst)
}

func (mbs *metadataBlobStatter) Clear(ctx context.Context, dgst digest.Digest) error {
	key := BlobKey{Dgst: dgst}.String()
	return mbs.metadataService.Delete(ctx, key)
}

func (mbs *metadataBlobStatter) SetDescriptor(ctx context.Context, dgst digest.Digest, desc distribution.Descriptor) error {
	key := BlobKey{Dgst: dgst}.String()
	err := mbs.metadataService.Put(ctx, key, desc)
	if err != nil {
		return err
	}
	return nil
}
