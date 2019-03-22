package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jfontan/borges"

	goborges "github.com/src-d/go-borges"
	"github.com/src-d/go-borges/siva"
	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/osfs"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

func main() {
	tmpRepos := osfs.New("./tmpRepos")
	tmpBorges := osfs.New("./tmpBorges")
	fs := osfs.New("./sivas")

	if len(os.Args) != 2 {
		panic("Usage: borges <list of repositories>")
	}
	file := os.Args[1]

	var list []string

	f, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		list = append(list, scanner.Text())
	}

	library, err := siva.NewLibrary(
		"siva",
		fs,
		siva.LibraryOptions{
			Transactional: true,
			TempFS:        tmpBorges,
		})
	if err != nil {
		panic(err)
	}

	for _, r := range list {
		id := strings.TrimLeft(r, "https://")
		println("downloading", id)
		start := time.Now()
		err = DownloadRepo(library, tmpRepos, id, r)
		if err != nil {
			panic(err)
		}
		println("finished", id, time.Since(start).String())
	}
}

func DownloadRepo(lib *siva.Library, tmp billy.Filesystem, id, url string) error {
	cloner := borges.NewTemporaryCloner(tmp)

	start := time.Now()
	tr, err := cloner.Clone(context.Background(), id, url)
	if err != nil {
		return err
	}
	cloneTime := time.Since(start).String()

	init, err := tr.Init(context.Background())
	if err != nil {
		return err
	}

	println("init", id, init.String())

	start = time.Now()

	var repo goborges.Repository
	loc, err := lib.Location(goborges.LocationID(init.String()))
	if err != nil {
		if !goborges.ErrLocationNotExists.Is(err) {
			return err
		}
		repo, err = newLocation(lib, id, url, init, tr)
	} else {
		repo, err = addRemote(loc, id, url)
	}
	if err != nil {
		return err
	}

	copyTime := time.Since(start).String()

	start = time.Now()
	r := repo.R()
	err = r.Fetch(&git.FetchOptions{
		RemoteName: id,
		Tags:       git.NoTags,
	})
	if err != nil {
		if err != git.NoErrAlreadyUpToDate {
			return err
		}
	}

	fetchTime := time.Since(start)

	start = time.Now()
	err = repo.Commit()
	commitTime := time.Since(start).String()

	fmt.Printf("clone: %s, copy: %s, fetch: %s, commit: %s\n",
		cloneTime, copyTime, fetchTime, commitTime)

	return err
}

func newLocation(
	lib *siva.Library,
	id string,
	url string,
	init plumbing.Hash,
	tr borges.TemporaryRepository,
) (goborges.Repository, error) {
	loc, err := lib.AddLocation(goborges.LocationID(init.String()))
	if err != nil {
		return nil, err
	}

	repo, err := loc.Init(goborges.RepositoryID(id))
	if err != nil {
		return nil, err
	}

	r := repo.R()
	fs, ok := r.Storer.(borges.Filesystemer)
	if ok {
		err = tr.Copy(context.Background(), fs.Filesystem())
	} else {
		_, err = borges.CreateRemote(r, id, url)
	}

	if err != nil {
		return nil, err
	}

	return repo, nil
}

func addRemote(
	loc goborges.Location,
	id, url string,
) (goborges.Repository, error) {
	repo, err := loc.Get(goborges.RepositoryID(id), goborges.RWMode)
	if err == nil {
		return repo, nil
	}

	repo, err = loc.Init(goborges.RepositoryID(id))
	if err != nil {
		return nil, err
	}

	r := repo.R()
	_, err = borges.CreateRemote(r, id, url)
	if err != nil {
		println("err create remote")
		return nil, err
	}

	return repo, nil
}
