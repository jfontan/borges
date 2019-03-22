package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/src-d/borges"
	"github.com/src-d/go-borges/siva"

	goborges "github.com/src-d/go-borges"
	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/osfs"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/config"
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

func mainDownload() {
	fs := osfs.New("./sivas")
	// library := plain.NewLibrary("lib")

	// loc, err := plain.NewLocation("borges", fs, &plain.LocationOptions{})
	// if err != nil {
	// 	panic(err)
	// }

	repo := os.Args[1]
	file := os.Args[2]

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
			// Transactional: false,
		})
	if err != nil {
		panic(err)
	}

	loc, err := library.AddLocation("siva")
	if err != nil {
		panic(err)
	}

	// repo, err := loc.Init("src-d")
	// if err != nil {
	// 	panic(err)
	// }

	// r := repo.R()

	// err = SetupRemote(r, "src-d", "https://github.com/src-d/borges")
	// if err != nil {
	// 	panic(err)
	// }

	// err = Fetch(r, "src-d")
	// if err != nil {
	// 	panic(err)
	// }

	// err = repo.Commit()
	// if err != nil {
	// 	panic(err)
	// }

	println("start head")
	start := time.Now()
	url := fmt.Sprintf("https://github.com/%s/%s", list[0], repo)
	err = Download(loc, "src-d", url, true)
	if err != nil {
		panic(err)
	}
	println("end head", time.Since(start).String())

	// println("start jfontan")
	// start = time.Now()
	// err = Download(loc, "jfontan", "https://github.com/jfontan/borges")
	// if err != nil {
	// 	panic(err)
	// }
	// println("end jfontan", time.Since(start).String())

	// forks := []string{
	// 	"afcarl", "ajnavarro", "bzz", "carlosms", "erizocosmico", "jfontan",
	// 	"juanjux", "JustForkin", "kuba--", "mcarmonaa", "mcuadros", "realdoug",
	// 	"smola",
	// }

	for _, f := range list {
		fmt.Printf("start %s\n", f)
		start := time.Now()
		url := fmt.Sprintf("https://github.com/%s/%s", f, repo)
		err = Download(loc, f, url, false)
		if err != nil {
			panic(err)
		}
		fmt.Printf("end %s %s\n", f, time.Since(start).String())
	}
}

func Download(loc goborges.Location, name, url string, head bool) error {
	repo, err := loc.Init(goborges.RepositoryID(name))
	if goborges.ErrRepositoryExists.Is(err) {
		println("already")
		repo, err = loc.Get(goborges.RepositoryID(name), goborges.RWMode)
	}
	if err != nil {
		println("error", err.Error())
		return err
	}

	r := repo.R()
	err = SetupRemote(r, name, url)
	if err != nil {
		return err
	}

	err = Fetch(r, name)
	if err != nil {
		return err
	}

	// return repo.Commit()
	return repo.Close()
}

func SetupRemote(r *git.Repository, name, url string) error {
	cfg, err := r.Config()
	if err != nil {
		return err
	}

	remo := cfg.Remotes[name]
	if remo == nil {
		remo = &config.RemoteConfig{
			Name: name,
		}
		cfg.Remotes[name] = remo
	}

	remo.URLs = []string{url}
	remo.Fetch = []config.RefSpec{
		// borges.FetchRefSpec(name),
		borges.FetchHEADSpec(name),
	}

	return r.Storer.SetConfig(cfg)
}

func Fetch(r *git.Repository, name string) error {
	// remote, err := r.Remote(name)
	// if err != nil {
	// 	return err
	// }

	return r.Fetch(&git.FetchOptions{
		RemoteName: name,
		// RefSpecs: []config.RefSpec{
		// 	borges.FetchRefSpec(name),
		// 	borges.FetchHEADSpec(name),
		// },
		Tags: git.NoTags,
	})
}
