package inmemory

import (
	"fmt"
	"reflect"
	"sort"
	"sync"

	"github.com/docker/distribution"
	"github.com/docker/distribution/context"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/storage/metadata"
)

var _ metadata.MetadataService = &inMemoryMetadataStore{}

type repository map[string]interface{}

func NewMetadataService() metadata.MetadataService {
	return &inMemoryMetadataStore{
		state:   make(map[reference.Named]repository),
		RWMutex: &sync.RWMutex{},
	}
}

func (m *inMemoryMetadataStore) RepositoryScoped(to metadata.MetadataService, repo reference.Named) metadata.MetadataService {
	global := to.(*inMemoryMetadataStore)
	reposcoped := &inMemoryMetadataStore{
		RWMutex: global.RWMutex,
		state:   global.state,
		repo:    repo,
	}
	m.Lock()
	defer m.Unlock()

	if _, ok := reposcoped.state[repo]; !ok {
		reposcoped.state[repo] = make(repository, 0)
	}
	return reposcoped
}

type inMemoryMetadataStore struct {
	*sync.RWMutex
	state map[reference.Named]repository
	repo  reference.Named
}

func (m *inMemoryMetadataStore) Put(ctx context.Context, key string, value interface{}) error {
	m.Lock()
	defer m.Unlock()

	repository := m.state[m.repo]
	repository[key] = value

	return nil
}

func (m *inMemoryMetadataStore) BatchPut(ctx context.Context, updates map[string]metadata.MetadataUpdateRecord) error {
	m.Lock()
	defer m.Unlock()

	for k, v := range updates {
		valNow := m.getOrNil(ctx, k)
		if !reflect.DeepEqual(valNow, v.Expected) {
			fmt.Printf("tx failed on key %s\n actual:   %v != \n expected: %v\n", k, valNow, v.Expected)
			return metadata.ErrTransactionCanRetry
		}
	}

	repository := m.state[m.repo]
	for k, v := range updates {
		if v.Actual == nil {
			delete(repository, k)
		} else {
			repository[k] = v.Actual
		}
	}

	return nil
}

func (m *inMemoryMetadataStore) getOrNil(ctx context.Context, key string) interface{} {
	repository := m.state[m.repo]
	if val, ok := repository[key]; ok {
		return val
	}
	return nil
}

func (m *inMemoryMetadataStore) Get(ctx context.Context, key string) (interface{}, error) {
	m.RLock()
	defer m.RUnlock()
	return m.getOrNil(ctx, key), nil
}

func (m *inMemoryMetadataStore) Delete(ctx context.Context, key string) error {
	m.Lock()
	defer m.Unlock()

	repository := m.state[m.repo]

	if _, ok := repository[key]; !ok {
		return metadata.ErrKeyNotFound{Key: key}
	}

	delete(repository, key)
	return nil
}

func (m *inMemoryMetadataStore) Iterate(ctx context.Context, params metadata.IterateParameters, iterFunc metadata.IterFunc) error {
	m.RLock()
	defer m.RUnlock()

	if params.IterType == (metadata.RepoKey{}) {
		return m.iterateRepositories(ctx, params, iterFunc)
	}

	repository := m.state[m.repo]
	return m.iterateRepository(ctx, params, iterFunc, repository)

}
func (m *inMemoryMetadataStore) iterateRepositories(ctx context.Context, params metadata.IterateParameters, iterFunc metadata.IterFunc) error {
	var repos []string
	for k, _ := range m.state {
		repos = append(repos, k.Name())
	}

	sort.Strings(repos)
	for _, r := range repos {
		if params.From != "" && params.From >= r {
			continue
		}
		err := iterFunc(r, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *inMemoryMetadataStore) iterateRepository(ctx context.Context, params metadata.IterateParameters, iterFunc metadata.IterFunc, repository map[string]interface{}) error {
	var keys []string
	for k, _ := range repository {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	if len(keys) == 0 {
		return distribution.ErrRepositoryUnknown{Name: m.repo.Name()}
	}

	for _, k := range keys {
		v := repository[k]

		var err error
		generic := metadata.KeyFromString(k)

		switch key := generic.(type) {
		case metadata.ManifestDigestKey:
			eff := metadata.ManifestDigestKey{}
			if params.IterType == eff {
				err = iterFunc(string(key.Dgst), v)
			}
		case metadata.TagKey:
			eff := metadata.TagKey{}
			if params.IterType == eff {
				err = iterFunc(key.Tag, v)
			}
		default:
			continue
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *inMemoryMetadataStore) Exists(ctx context.Context, key string) (bool, error) {
	m.RLock()
	defer m.RUnlock()

	repository := m.state[m.repo]
	if _, ok := repository[key]; !ok {
		//m.dump()
		return false, nil
	}

	return true, nil
}

func (m *inMemoryMetadataStore) dump() {
	fmt.Printf("Dumping.  Repo=%s\n", m.repo)
	for repoName, repo := range m.state {
		for k, v := range repo {
			fmt.Printf("%q: %q=%v+\n", repoName, k, v)
		}
	}
}
