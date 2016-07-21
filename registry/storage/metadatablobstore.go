package storage

import (
	"fmt"
	"time"

	"github.com/docker/distribution"
	"github.com/docker/distribution/context"
	"github.com/docker/distribution/digest"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/storage/metadata"
	"github.com/docker/distribution/uuid"
)

var _ distribution.BlobStore = &metadataBlobStore{}

// metadataBlobStore provides a blob service that namespaces blobs to a repository.
// The embedded linkedBlobstore provides base functionality with metadata specfic overrides
// where necessary
type metadataBlobStore struct {
	*linkedBlobStore
	metadataService metadata.MetadataService
}

func (mbs *metadataBlobStore) Put(ctx context.Context, mediaType string, p []byte) (distribution.Descriptor, error) {
	dgst := digest.FromBytes(p)

	desc, err := mbs.blobStore.Put(ctx, mediaType, p)
	if err != nil {
		context.GetLogger(ctx).Errorf("error putting into main store: %v", err)
		return distribution.Descriptor{}, err
	}

	if err := mbs.blobAccessController.SetDescriptor(ctx, dgst, desc); err != nil {
		return distribution.Descriptor{}, err
	}
	return desc, nil
}

func (mbs *metadataBlobStore) Create(ctx context.Context, options ...distribution.BlobCreateOption) (distribution.BlobWriter, error) {
	var opts distribution.CreateOptions

	for _, option := range options {
		err := option.Apply(&opts)
		if err != nil {
			return nil, err
		}
	}

	if opts.Mount.ShouldMount {
		desc, err := mbs.mount(ctx, opts.Mount.From, opts.Mount.From.Digest())

		if err == nil {
			// Mount successful, no need to initiate an upload session
			return nil, distribution.ErrBlobMounted{From: opts.Mount.From, Descriptor: desc}
		}
	}

	uuid := uuid.Generate().String()
	startedAt := time.Now().UTC()

	startedAtKey := metadata.UploadStartedAtKey{ID: uuid}.String()
	err := mbs.metadataService.Put(ctx, startedAtKey, startedAt.Format(time.RFC3339))
	if err != nil {
		return nil, err
	}

	uploadPath, err := pathFor(uploadDataPathSpec{
		name: mbs.repository.Named().Name(),
		id:   uuid,
	})
	if err != nil {
		return nil, err
	}

	uploadKey := metadata.UploadPathKey{ID: uuid}.String()
	err = mbs.metadataService.Put(ctx, uploadKey, uploadPath)
	if err != nil {
		return nil, err
	}

	return mbs.newBlobUpload(ctx, uuid, uploadPath, startedAt, false)
}

func (mbs *metadataBlobStore) mount(ctx context.Context, source reference.Named, dgst digest.Digest) (distribution.Descriptor, error) {
	sourceNamed, err := reference.ParseNamed(source.Name())
	if err != nil {
		return distribution.Descriptor{}, err
	}

	sourceRepo, err := mbs.registry.Repository(ctx, sourceNamed)
	if err != nil {
		return distribution.Descriptor{}, err
	}

	stat, err := sourceRepo.Blobs(ctx).Stat(ctx, dgst)
	if err != nil {
		return distribution.Descriptor{}, err
	}

	desc := distribution.Descriptor{
		Size:      stat.Size,
		MediaType: "application/octet-stream",
		Digest:    dgst,
	}
	return desc, mbs.blobAccessController.SetDescriptor(ctx, dgst, desc)
}

func (mbs *metadataBlobStore) Resume(ctx context.Context, id string) (distribution.BlobWriter, error) {
	startedAtKey := metadata.UploadStartedAtKey{ID: id}.String()
	startedAtVal, err := mbs.metadataService.Get(ctx, startedAtKey)
	if startedAtVal == nil {
		return nil, distribution.ErrBlobUploadUnknown
	}
	if err != nil {
		return nil, err
	}

	startedAt, err := time.Parse(time.RFC3339, fmt.Sprint(startedAtVal))
	if err != nil {
		return nil, err
	}

	pathKey := metadata.UploadPathKey{ID: id}.String()
	uploadPath, err := mbs.metadataService.Get(ctx, pathKey)
	if err != nil {
		return nil, err
	}

	return mbs.newBlobUpload(ctx, id, fmt.Sprint(uploadPath), startedAt, true)
}

// newBlobUpload allocates a new upload controller with the given state.
func (mbs *metadataBlobStore) newBlobUpload(ctx context.Context, uuid, path string, startedAt time.Time, append bool) (distribution.BlobWriter, error) {
	fw, err := mbs.driver.Writer(ctx, path, append)
	if err != nil {
		return nil, err
	}

	bw := &blobWriter{
		ctx:        ctx,
		blobStore:  mbs.linkedBlobStore,
		id:         uuid,
		startedAt:  startedAt,
		digester:   digest.Canonical.New(),
		fileWriter: fw,
		driver:     mbs.driver,
		path:       path,
		resumableDigestEnabled: mbs.resumableDigestEnabled,
	}
	bw.cleanupFunc = func(ctx context.Context) error {
		err := bw.cleanup(ctx)
		if err != nil {
			return err
		}
		return metadata.Update(ctx, mbs.repository, func(ctx context.Context) error {
			startedAtKey := metadata.UploadStartedAtKey{ID: uuid}.String()
			err = mbs.metadataService.Delete(ctx, startedAtKey)
			if err != nil {
				return err
			}

			uploadKey := metadata.UploadPathKey{ID: uuid}.String()
			err := mbs.metadataService.Delete(ctx, uploadKey)
			if err != nil {
				return err
			}
			return nil
		})
	}

	bw.commitFunc = func(ctx context.Context, desc distribution.Descriptor) (distribution.Descriptor, error) {
		if err := bw.fileWriter.Commit(); err != nil {
			return distribution.Descriptor{}, err
		}

		bw.Close()
		desc.Size = bw.Size()

		canonical, err := bw.validateBlob(ctx, desc)
		if err != nil {
			return distribution.Descriptor{}, err
		}

		if err := bw.moveBlob(ctx, canonical); err != nil {
			return distribution.Descriptor{}, err
		}

		if err := bw.cleanupFunc(ctx); err != nil {
			return distribution.Descriptor{}, err
		}

		err = bw.blobStore.blobAccessController.SetDescriptor(ctx, canonical.Digest, canonical)
		if err != nil {
			return distribution.Descriptor{}, err
		}

		bw.committed = true
		return canonical, nil
	}

	return bw, nil
}
