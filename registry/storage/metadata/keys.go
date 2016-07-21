package metadata

import (
	"fmt"
	"regexp"

	"github.com/docker/distribution/digest"
	"github.com/docker/distribution/reference"
)

// todo: verify key on create

// Keys provides a method for generating, serializing and unserializing keys.  The
// string representation of a key can be converted back into a strong type.  Key
// string representations should not clash.
func KeyFromString(keyStr string) iterable {
	re := regexp.MustCompile("^manifest::(" + digest.DigestRegexp.String() + ")$")
	matches := re.FindStringSubmatch(keyStr)
	if matches != nil {
		return ManifestDigestKey{Dgst: digest.Digest(matches[1])}
	}

	re = regexp.MustCompile("^tag::(" + reference.TagRegexp.String() + ")$")
	matches = re.FindStringSubmatch(keyStr)
	if matches != nil {
		return TagKey{Tag: matches[1]}
	}

	re = regexp.MustCompile("^blob::(" + digest.DigestRegexp.String() + ")$")
	matches = re.FindStringSubmatch(keyStr)
	if matches != nil {
		return BlobKey{Dgst: digest.Digest(matches[1])}
	}

	re = regexp.MustCompile("^repo::([a-z0-9]+(?:(?:(?:[._]|__|[-]*)[a-z0-9]+)+)?)")
	matches = re.FindStringSubmatch(keyStr)
	if matches != nil {
		return RepoKey{Repo: matches[1]}
	}

	re = regexp.MustCompile("^upload::(.*)$")
	matches = re.FindStringSubmatch(keyStr)
	if matches != nil {
		//		return UploadStartedAtKey{ID: matches[1]}
		return nil
	}

	panic(fmt.Sprintf("error on key match: ", keyStr))
	return nil
}

// keySpec marks structs as key specifiers
type keySpec interface {
	keySpec()
}

// iterable marks key structs as iterable types
type iterable interface {
	iter()
}

// ManifestDigestKey describes manifests in a repository
type ManifestDigestKey struct {
	Dgst digest.Digest
}

func (k ManifestDigestKey) keySpec() {}
func (k ManifestDigestKey) iter()    {}
func (k ManifestDigestKey) String() string {
	return fmt.Sprintf("manifest::%s", k.Dgst)
}

// tagKey describes tags in a repository
type TagKey struct {
	Tag string
}

func (k TagKey) keySpec() {}
func (k TagKey) iter()    {}
func (k TagKey) String() string {
	return fmt.Sprintf("tag::%s", k.Tag)
}

// RepoKey describes a repository. todo(richard): what is this for?
type RepoKey struct {
	Repo string
}

func (k RepoKey) keySpec() {}
func (k RepoKey) iter()    {}
func (k RepoKey) String() string {
	return fmt.Sprintf("repo::%s", k.Repo)
}

// blobKey describes a blob digest in a repository
type BlobKey struct {
	Dgst digest.Digest
}

func (k BlobKey) keySpec() {}
func (k BlobKey) iter()    {}
func (k BlobKey) String() string {
	return fmt.Sprintf("blob::%s", k.Dgst)
}

// UploadStartedAtKey describes an upload start time in a repository.
type UploadStartedAtKey struct {
	ID string
}

func (k UploadStartedAtKey) keySpec() {}
func (k UploadStartedAtKey) String() string {
	return fmt.Sprintf("upload::%s::startedat", k.ID)
}

// UploadPathKey describes an upload path in a repository.
type UploadPathKey struct {
	ID string
}

func (k UploadPathKey) keySpec() {}
func (k UploadPathKey) String() string {
	return fmt.Sprintf("upload::%s::path", k.ID)
}
