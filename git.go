package borges

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"gopkg.in/src-d/core-retrieval.v0/model"
	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/util"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/config"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
	"gopkg.in/src-d/go-git.v4/plumbing/transport"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
	"gopkg.in/src-d/go-git.v4/storage/memory"
)

const (
	FetchRefSpecStr = "+refs/*:refs/remotes/%s/*"
	FetchHEADStr    = "+HEAD:refs/remotes/%s/HEAD"
)

func FetchRefSpec(id string) config.RefSpec {
	return config.RefSpec(fmt.Sprintf(FetchRefSpecStr, id))
}

func FetchHEADSpec(id string) config.RefSpec {
	return config.RefSpec(fmt.Sprintf(FetchHEADStr, id))
}

type Filesystemer interface {
	Filesystem() billy.Filesystem
}

type TemporaryRepository interface {
	Copy(ctx context.Context, fs billy.Filesystem) error
	Init(ctx context.Context) (plumbing.Hash, error)
}

type TemporaryCloner interface {
	Clone(ctx context.Context, id, url string) (TemporaryRepository, error)
}

func NewTemporaryCloner(tmpFs billy.Filesystem) TemporaryCloner {
	return &temporaryRepositoryCloner{tmpFs}
}

type temporaryRepositoryCloner struct {
	TempFilesystem billy.Filesystem
}

type temporaryRepository struct {
	ID             string
	Repository     *git.Repository
	TempFilesystem billy.Filesystem
	TempPath       string
}

func CreateRemote(r *git.Repository, id, endpoint string) (*git.Remote, error) {
	rCfg := &config.RemoteConfig{
		Name: id,
		URLs: []string{endpoint},
		Fetch: []config.RefSpec{
			FetchHEADSpec(id),
			FetchRefSpec(id),
		}}

	remote, err := r.Remote(id)
	if err == nil {
		if remote.Config() == rCfg {
			return remote, nil
		}

		cfg, err := r.Config()
		if err != nil {
			return nil, err
		}

		cfg.Remotes[id] = rCfg
		err = r.Storer.SetConfig(cfg)
		if err != nil {
			return nil, err
		}

		return r.Remote(id)
	}

	return r.CreateRemote(rCfg)
}

func (b *temporaryRepositoryCloner) Clone(
	ctx context.Context,
	id, endpoint string,
) (TemporaryRepository, error) {
	dir := filepath.Join(
		"local_repos",
		fmt.Sprintf("%s_%s",
			id,
			strconv.FormatInt(time.Now().UnixNano(), 10)))

	tmpFs, err := b.TempFilesystem.Chroot(dir)
	if err != nil {
		return nil, err
	}

	s := filesystem.NewStorage(tmpFs, cache.NewObjectLRUDefault())
	if err != nil {
		return nil, err
	}

	r, err := git.Init(s, nil)
	if err != nil {
		_ = util.RemoveAll(b.TempFilesystem, dir)
		return nil, err
	}

	// rCfg := &config.RemoteConfig{
	// 	Name: id,
	// 	URLs: []string{endpoint},
	// 	Fetch: []config.RefSpec{
	// 		FetchHEADSpec(id),
	// 		FetchRefSpec(id),
	// 	}}

	// remote, err := r.CreateRemote(rCfg)
	remote, err := CreateRemote(r, id, endpoint)
	if err != nil {
		println("error create remote")
		_ = util.RemoveAll(b.TempFilesystem, dir)
		return nil, err
	}

	o := &git.FetchOptions{
		RefSpecs: []config.RefSpec{FetchHEADSpec(id)},
		Force:    true,
		Tags:     git.NoTags,
	}
	err = remote.FetchContext(ctx, o)

	if err == git.NoErrAlreadyUpToDate || err == transport.ErrEmptyRemoteRepository {
		r, err = git.Init(memory.NewStorage(), nil)
	}

	if err != nil {
		println("error fetch")
		_ = util.RemoveAll(b.TempFilesystem, dir)
		return nil, err
	}

	return &temporaryRepository{
		ID:             id,
		Repository:     r,
		TempFilesystem: b.TempFilesystem,
		TempPath:       dir,
	}, nil
}

func (t *temporaryRepository) Copy(ctx context.Context, fs billy.Filesystem) error {
	return RecursiveCopy("/", fs, t.TempPath, t.TempFilesystem)
}

func (t *temporaryRepository) Init(ctx context.Context) (plumbing.Hash, error) {
	refName := plumbing.NewRemoteHEADReferenceName(t.ID)
	ref, err := t.Repository.Reference(refName, true)
	if err != nil {
		return plumbing.ZeroHash, err
	}

	var seenRoots = make(map[plumbing.Hash][]model.SHA1)

	c, err := ResolveCommit(t.Repository, ref.Hash())
	if err != nil {
		return plumbing.ZeroHash, err
	}

	roots, err := rootCommits(t.Repository, c, seenRoots)
	if err != nil {
		return plumbing.ZeroHash, err
	}

	return plumbing.Hash(roots[0]), nil
}

// RecursiveCopy copies a directory to a destination path. It creates all
// needed directories if destination path does not exist.
func RecursiveCopy(
	dst string,
	dstFS billy.Filesystem,
	src string,
	srcFS billy.Filesystem,
) error {
	stat, err := srcFS.Stat(src)
	if err != nil {
		return err
	}

	if stat.IsDir() {
		err = dstFS.MkdirAll(dst, stat.Mode())
		if err != nil {
			return err
		}

		files, err := srcFS.ReadDir(src)
		if err != nil {
			return err
		}

		for _, file := range files {
			srcPath := filepath.Join(src, file.Name())
			dstPath := filepath.Join(dst, file.Name())

			err = RecursiveCopy(dstPath, dstFS, srcPath, srcFS)
			if err != nil {
				return err
			}
		}
	} else {
		err = CopyFile(dst, dstFS, src, srcFS, stat.Mode())
		if err != nil {
			return err
		}
	}

	return nil
}

// CopyFile makes a file copy with the specified permission.
func CopyFile(
	dst string,
	dstFS billy.Filesystem,
	src string,
	srcFS billy.Filesystem,
	mode os.FileMode,
) error {
	_, err := srcFS.Stat(src)
	if err != nil {
		return err
	}

	fo, err := srcFS.Open(src)
	if err != nil {
		return err
	}
	defer fo.Close()

	fd, err := dstFS.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	defer fd.Close()

	_, err = io.Copy(fd, fo)
	if err != nil {
		fd.Close()
		dstFS.Remove(dst)
		return err
	}

	return nil
}
