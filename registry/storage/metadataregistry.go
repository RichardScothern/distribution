package storage

import (
	"github.com/docker/distribution"
	"github.com/docker/distribution/context"
	"github.com/docker/distribution/reference"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/metadata"
	//	"github.com/docker/distribution/registry/storage/metadata/inmemory"
)

// registry is the top-level implementation of Registry for use in the storage
// package. All instances should descend from this object.
type metadataRegistry struct {
	embedded        *registry
	metadataService metadata.MetadataService
}

// NewRegistry creates a new registry instance from the provided driver. The
// resulting registry may be shared by multiple goroutines but is cheap to
// allocate. If the Redirect option is specified, the backend blob server will
// attempt to use (StorageDriver).URLFor to serve all blobs.
func NewMetadataRegistry(ctx context.Context, driver storagedriver.StorageDriver, md metadata.MetadataService, options ...RegistryOption) (distribution.Namespace, error) {
	embedded, err := NewRegistry(ctx, driver, options...)
	if err != nil {
		return nil, err
	}

	registry := &metadataRegistry{
		embedded:        embedded.(*registry),
		metadataService: md,
	}

	return registry, nil
}

// Scope returns the namespace scope for a registry. The registry
// will only serve repositories contained within this scope.
func (reg *metadataRegistry) Scope() distribution.Scope {
	return reg.embedded.Scope()
}

// Repository returns an instance of the repository tied to the registry.
// Instances should not be shared between goroutines but are cheap to
// allocate. In general, they should be request scoped.
func (reg *metadataRegistry) Repository(ctx context.Context, canonicalName reference.Named) (distribution.Repository, error) {
	return &metadataRepository{
		ctx:  ctx,
		reg:  reg,
		name: canonicalName,
	}, nil
}

func (reg *metadataRegistry) Blobs() distribution.BlobEnumerator {
	return reg.embedded.Blobs()
}

func (reg *metadataRegistry) BlobStatter() distribution.BlobStatter {
	return reg.embedded.BlobStatter()
}

func (reg *metadataRegistry) MetadataService() metadata.MetadataService {
	return reg.metadataService
}

// repository provides name-scoped access to various services.
type metadataRepository struct {
	reg  *metadataRegistry
	ctx  context.Context
	name reference.Named
}

func (repo *metadataRepository) MetadataService() metadata.MetadataService {
	return repo.reg.metadataService.RepositoryScoped(repo.reg.metadataService, repo.Named())
}

// Name returns the name of the repository.
func (repo *metadataRepository) Named() reference.Named {
	return repo.name
}

func (repo *metadataRepository) Tags(ctx context.Context) distribution.TagService {
	return metadata.NewTagService(repo.name, repo.MetadataService())
}

// Manifests returns an instance of ManifestService. Instantiation is cheap and
// may be context sensitive in the future. The instance should be used similar
// to a request local
func (repo *metadataRepository) Manifests(ctx context.Context, options ...distribution.ManifestServiceOption) (distribution.ManifestService, error) {
	linkedBlobStore := &linkedBlobStore{
		ctx:                  ctx,
		registry:             repo.reg,
		blobStore:            repo.reg.embedded.blobStore,
		repository:           repo,
		deleteEnabled:        repo.reg.embedded.deleteEnabled,
		blobAccessController: metadata.NewBlobStatter(repo.MetadataService(), repo.name, repo.reg.embedded.BlobStatter()),
	}

	blobStore := &metadataBlobStore{
		linkedBlobStore,
		repo.MetadataService(),
	}

	// manifestStore is used here for writing manifests to a blobStore
	// as well as verifying.  Most operations are delegated to handlers.
	// the blobstore used has a metadata aware statter and linking disabled
	store := &manifestStore{
		ctx:        ctx,
		repository: repo,
		blobStore:  blobStore,
		schema1Handler: &signedManifestHandler{
			ctx:               ctx,
			schema1SigningKey: repo.reg.embedded.schema1SigningKey,
			repository:        repo,
			blobStore:         blobStore,
		},
		schema2Handler: &schema2ManifestHandler{
			ctx:        ctx,
			repository: repo,
			blobStore:  blobStore,
		},
		manifestListHandler: &manifestListHandler{
			repository: repo,
			ctx:        ctx,
			blobStore:  blobStore,
		},
	}

	ms, err := metadata.NewManifestService(ctx, repo.name, repo.MetadataService(), store, repo.reg.embedded.deleteEnabled)
	if err != nil {
		return nil, err
	}

	// Apply options
	for _, option := range options {
		err := option.Apply(store)
		if err != nil {
			return nil, err
		}
	}

	return ms, nil
}

// Blobs returns an instance of the BlobStore. Instantiation is cheap and
// may be context sensitive in the future. The instance should be used similar
// to a request local.
func (repo *metadataRepository) Blobs(ctx context.Context) distribution.BlobStore {
	linkedBlobStore := &linkedBlobStore{
		registry:             repo.reg,
		blobStore:            repo.reg.embedded.blobStore,
		blobServer:           repo.reg.embedded.blobServer,
		blobAccessController: metadata.NewBlobStatter(repo.MetadataService(), repo.name, repo.reg.embedded.BlobStatter()),
		repository:           repo,
		ctx:                  ctx,

		deleteEnabled:          repo.reg.embedded.deleteEnabled,
		resumableDigestEnabled: repo.reg.embedded.resumableDigestEnabled,
	}

	return &metadataBlobStore{
		linkedBlobStore,
		repo.MetadataService(),
	}
}
