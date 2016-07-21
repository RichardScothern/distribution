package storage

import (
	"errors"
	"io"
	"path"
	"strings"

	"github.com/docker/distribution"
	"github.com/docker/distribution/context"
	"github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/metadata"
)

// ErrFinishedWalk is used when the called walk function no longer wants
// to accept any more values.  This is used for pagination when the
// required number of repos have been found.
var ErrFinishedWalk = errors.New("finished walk")

// Returns a list, or partial list, of repositories in the registry.
// Because it's a quite expensive operation, it should only be used when building up
// an initial set of repositories.
func (reg *registry) Repositories(ctx context.Context, repos []string, last string) (n int, err error) {
	var foundRepos []string

	if len(repos) == 0 {
		return 0, errors.New("no space in slice")
	}

	root, err := pathFor(repositoriesRootPathSpec{})
	if err != nil {
		return 0, err
	}

	err = Walk(ctx, reg.blobStore.driver, root, func(fileInfo driver.FileInfo) error {
		filePath := fileInfo.Path()

		// lop the base path off
		repoPath := filePath[len(root)+1:]

		_, file := path.Split(repoPath)
		if file == "_layers" {
			repoPath = strings.TrimSuffix(repoPath, "/_layers")
			if repoPath > last {
				foundRepos = append(foundRepos, repoPath)
			}
			return ErrSkipDir
		} else if strings.HasPrefix(file, "_") {
			return ErrSkipDir
		}

		// if we've filled our array, no need to walk any further
		if len(foundRepos) == len(repos) {
			return ErrFinishedWalk
		}

		return nil
	})

	n = copy(repos, foundRepos)

	// Signal that we have no more entries by setting EOF
	if len(foundRepos) <= len(repos) && (err == nil || err == ErrSkipDir) {
		err = io.EOF
	}

	return n, err
}

// Enumerate applies ingester to each repository
func (reg *registry) Enumerate(ctx context.Context, ingester func(string) error) error {
	return foreachRepository(ctx, reg, ingester)
}

func foreachRepository(ctx context.Context, reg distribution.Namespace, ingester func(repo string) error) error {
	repoNameBuffer := make([]string, 100)
	var last string
	for {
		n, err := reg.Repositories(ctx, repoNameBuffer, last)
		if err != nil && err != io.EOF {
			return err
		}

		if n == 0 {
			break
		}

		last = repoNameBuffer[n-1]
		for i := 0; i < n; i++ {
			repoName := repoNameBuffer[i]
			err = ingester(repoName)
			if err != nil {
				return err
			}
		}

		if err == io.EOF {
			break
		}
	}
	return nil
}

// todo(richardscothern): find a better place for this
func (reg *metadataRegistry) Repositories(ctx context.Context, repos []string, lastRepo string) (n int, errVal error) {
	if len(repos) == 0 {
		return 0, errors.New("no space in slice")
	}

	var items []string

	params := metadata.IterateParameters{From: lastRepo, IterType: metadata.RepoKey{}}
	err := reg.metadataService.Iterate(ctx, params, func(key string, val interface{}) error {
		items = append(items, key)
		if len(items) >= len(repos) {
			// filled the give slice
			return ErrFinishedWalk
		}
		return nil
	})

	if err != nil && err != ErrFinishedWalk {
		return 0, err
	}

	n = copy(repos, items)
	if len(items) < len(repos) {
		// didn't fill the repos array, no more to return
		errVal = io.EOF
	}
	return n, errVal
}

func (reg *metadataRegistry) Enumerate(ctx context.Context, ingester func(repo string) error) error {
	return foreachRepository(ctx, reg, ingester)
}
