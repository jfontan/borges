package borges

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/src-d/borges/lock"
	"github.com/src-d/borges/storage"

	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gopkg.in/src-d/core-retrieval.v0/model"
	"gopkg.in/src-d/core-retrieval.v0/repository"
	"gopkg.in/src-d/core-retrieval.v0/test"
	sivafs "gopkg.in/src-d/go-billy-siva.v4"
	billy "gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/osfs"
	fixtures "gopkg.in/src-d/go-git-fixtures.v3"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
	"gopkg.in/src-d/go-git.v4/storage/memory"
	kallax "gopkg.in/src-d/go-kallax.v1"
)

func TestArchiver(t *testing.T) {
	suite.Run(t, &ArchiverSuite{bucket: 0})
	suite.Run(t, &ArchiverSuite{bucket: 2})
}

type ArchiverSuite struct {
	test.Suite

	rawStore *model.RepositoryStore
	store    RepositoryStore
	tmpPath  string
	tx       repository.RootedTransactioner
	copier   *repository.Copier
	txFs     billy.Filesystem
	tmpFs    billy.Filesystem
	rootedFs billy.Filesystem
	a        *Archiver
	bucket   int
}

const defaultTimeout = 1 * time.Minute

func (s *ArchiverSuite) SetupTest() {
	fixtures.Init()
	s.Suite.Setup()

	s.rawStore = model.NewRepositoryStore(s.DB)
	s.store = storage.FromDatabase(s.DB)

	var err error
	s.tmpPath, err = ioutil.TempDir(os.TempDir(),
		fmt.Sprintf("borges-tests%d", rand.Uint32()))
	s.NoError(err)

	fs := osfs.New(s.tmpPath)

	s.rootedFs, err = fs.Chroot("rooted")
	s.NoError(err)
	s.txFs, err = fs.Chroot("tx")
	s.NoError(err)
	s.tmpFs, err = fs.Chroot("tmp")
	s.NoError(err)

	s.copier = repository.NewCopier(
		s.txFs,
		repository.NewLocalFs(s.rootedFs),
		s.bucket)
	s.tx = repository.NewSivaRootedTransactioner(s.copier)

	ls, err := lock.NewLocal().NewSession(&lock.SessionConfig{
		Timeout: defaultTimeout,
	})
	s.NoError(err)

	s.a = NewArchiver(s.store, s.tx, NewTemporaryCloner(s.tmpFs),
		ls, defaultTimeout, s.copier)
}

func (s *ArchiverSuite) TearDownTest() {
	s.NoError(os.RemoveAll(s.tmpPath))

	s.Suite.TearDown()
	fixtures.Clean()
}

func (s *ArchiverSuite) TestCheckTimeout() {
	const smallTimeout = 1 * time.Nanosecond
	s.a.Timeout = smallTimeout
	defer func() { s.a.Timeout = defaultTimeout }()
	for _, ct := range ChangesFixtures {
		if ct.OldReferences == nil {
			continue
		}

		s.T().Run(ct.TestName, func(t *testing.T) {
			require := require.New(t)

			var rid kallax.ULID
			r, err := ct.OldRepository()
			require.NoError(err)
			var hash model.SHA1
			err = withInProcRepository(hash, r, func(url string) error {
				rid = s.newRepositoryModel(url)
				return s.a.Do(context.TODO(), &Job{RepositoryID: uuid.UUID(rid)})
			})

			require.Error(err)
			require.Contains(err.Error(), context.DeadlineExceeded.Error())

			_, err = s.rawStore.FindOne(model.NewRepositoryQuery().FindByID(rid).FindByStatus(model.Pending))
			require.NoError(err)
		})
	}
}

func (s *ArchiverSuite) TestLockTimeout() {
	assert := s.Assert()

	l, err := lock.New("local:")
	assert.NoError(err)

	cfg := &lock.SessionConfig{
		Timeout: time.Second,
	}
	id := "borges/b029517f6300c2da0f4b651b8642506cd6aaf45d"

	session, err := l.NewSession(cfg)
	assert.NoError(err)
	locker := session.NewLocker(id)

	_, err = locker.Lock()
	assert.NoError(err)

	repo := fixtures.ByTag("worktree").One()
	path := repo.Worktree().Root()
	repoUUID := s.newRepositoryModel(path)

	start := time.Now()

	job := &Job{RepositoryID: uuid.UUID(repoUUID)}
	a := NewArchiver(s.store, s.tx, NewTemporaryCloner(s.tmpFs),
		session, 10*time.Second, s.copier)

	ctx := context.TODO()
	err = a.Do(ctx, job)
	assert.Error(err)

	// I'm not able to get the reason why the job failed from the error so here
	// is checked that it waits at least 1 second that is trying to acquire
	// the lock but less than 10 seconds that is the job timeout.
	assert.True(time.Since(start) > time.Second)
	assert.True(time.Since(start) < 10*time.Second)
}

func (s *ArchiverSuite) TestReferenceUpdate() {
	for _, ct := range ChangesFixtures {
		s.T().Run(ct.TestName, func(t *testing.T) {
			obtainedRefs := ct.OldReferences
			for ic, cs := range ct.Changes { // emulate pushChangesToRootedRepositories() behaviour
				obtainedRefs = updateRepositoryReferences(obtainedRefs, cs, ic)
			}

			s.Equal(len(ct.NewReferences), len(obtainedRefs))
		})
	}
}

func (s *ArchiverSuite) getFileNames(p string) ([]string, error) {
	var files []string

	dirents, err := s.rootedFs.ReadDir(p)
	if err != nil {
		return nil, err
	}

	for _, file := range dirents {
		if file.IsDir() {
			f, err := s.getFileNames(path.Join(p, file.Name()))
			if err != nil {
				return nil, err
			}

			files = append(files, f...)
		} else {
			files = append(files, file.Name())
		}
	}

	return files, nil
}

func (s *ArchiverSuite) TestFixtures() {
	for _, ct := range ChangesFixtures {
		s.T().Run(ct.TestName, func(t *testing.T) {
			require := require.New(t)
			var hash model.SHA1

			or, err := ct.OldRepository()
			require.NoError(err)

			var rid kallax.ULID
			// emulate initial status of a repository
			err = withInProcRepository(hash, or, func(url string) error {
				rid = s.newRepositoryModel(url)
				return s.a.Do(context.TODO(), &Job{RepositoryID: uuid.UUID(rid)})
			})
			require.NoError(err)

			nr, err := ct.NewRepository()
			require.NoError(err)

			err = withInProcRepository(hash, nr, func(url string) error {
				mr, err := s.rawStore.FindOne(model.NewRepositoryQuery().FindByID(rid))
				require.NoError(err)
				mr.Endpoints = nil
				mr.Endpoints = append(mr.Endpoints, url)
				updated, err := s.rawStore.Save(mr)
				require.NoError(err)
				require.True(updated, err)
				return s.a.Do(context.TODO(), &Job{RepositoryID: uuid.UUID(mr.ID)})
			})
			require.NoError(err)

			checkNoFiles(t, s.txFs)
			checkNoFiles(t, s.tmpFs)

			checkReferences(t, nr, ct.NewReferences)

			// check references in database
			mr, err := s.rawStore.FindOne(model.NewRepositoryQuery().FindByID(rid).WithReferences(nil))
			require.NoError(err)
			if len(mr.References) > 0 {
				require.NotNil(mr.LastCommitAt)
				require.NotEqual(new(time.Time), mr.LastCommitAt)
			}
			checkReferencesInDB(t, mr, ct.NewReferences)

			type references map[plumbing.ReferenceName]bool

			fis, err := s.getFileNames(".")
			if len(ct.NewReferences) != 0 {
				require.NoError(err)
				initHashesInStorage := make(map[string]references)

				for _, fi := range fis {
					hashStr := strings.Replace(fi, ".siva", "", -1)
					hash := plumbing.NewHash(hashStr)

					// get siva file and open it
					tx, err := s.a.RootedTransactioner.Begin(context.TODO(), hash)
					require.NoError(err)

					repo, err := git.Open(tx.Storer(), nil)
					require.NoError(err)

					// gather references for later check
					it, err := repo.References()
					require.NoError(err)

					refs := make(references)
					err = it.ForEach(func(r *plumbing.Reference) error {
						refs[r.Name()] = true
						return nil
					})
					require.NoError(err)

					initHashesInStorage[hashStr] = refs

					remotes, err := repo.Remotes()
					require.NoError(err)

					// check that "origin" remote does not exist and that
					// the one downloaded is configured
					var found bool
					for _, r := range remotes {
						require.NotEqual("origin", r.Config().Name)
						if r.Config().Name == rid.String() {
							found = true
						}
					}
					require.True(found)

					// delete previously copied siva file
					err = tx.Rollback()
					require.NoError(err)
				}

				// check that all the references that we have into the
				// database exists as a rooted repository
				for _, ref := range mr.References {
					r, ok := initHashesInStorage[ref.Init.String()]
					require.True(ok)

					name := rootedRefName(ref.GitReference().Name(), mr.ID)
					_, ok = r[name]
					require.True(ok)
				}

			}
		})
	}
}

func (s *ArchiverSuite) TestNotExistingRepository() {
	rid := s.newRepositoryModel("file:///this/repository/does/not/exists")
	err := s.a.Do(context.TODO(), &Job{RepositoryID: uuid.UUID(rid)})
	s.NoError(err)

	mr, err := s.rawStore.FindOne(model.NewRepositoryQuery().FindByID(rid))
	s.NoError(err)

	s.Equal(model.NotFound, mr.Status)
}

func (s *ArchiverSuite) TestPrivateRepository() {
	rid := s.newRepositoryModel("https://github.com/src-d/company")
	err := s.a.Do(context.TODO(), &Job{RepositoryID: uuid.UUID(rid)})
	s.NoError(err)

	mr, err := s.rawStore.FindOne(model.NewRepositoryQuery().FindByID(rid))
	s.NoError(err)

	s.Equal(model.AuthRequired, mr.Status)
}

func (s *ArchiverSuite) TestProcessingRepository() {
	rid := s.newRepositoryModel("git://foo.bar.baz")
	repo, err := s.rawStore.FindOne(model.NewRepositoryQuery().FindByID(rid))
	s.NoError(err)
	repo.Status = model.Fetching
	_, err = s.rawStore.Save(repo)
	s.NoError(err)

	err = s.a.Do(context.TODO(), &Job{RepositoryID: uuid.UUID(rid)})
	s.True(ErrAlreadyFetching.Is(err))

	mr, err := s.rawStore.FindOne(model.NewRepositoryQuery().FindByID(rid))
	s.NoError(err)

	s.Equal(model.Pending, mr.Status)
}

func (s *ArchiverSuite) newRepositoryModel(endpoint string) kallax.ULID {
	mr := model.NewRepository()
	mr.Endpoints = append(mr.Endpoints, endpoint)
	updated, err := s.rawStore.Save(mr)
	s.NoError(err)
	s.False(updated)

	return mr.ID
}

func checkReferences(t *testing.T, obtained *git.Repository, refs []*model.Reference) {
	require := require.New(t)
	obtainedRefs := repoToMemRefs(t, obtained)
	expectedRefs := modelToMemRefs(t, refs)
	require.Equal(expectedRefs, obtainedRefs)
}

func checkReferencesInDB(t *testing.T, obtained *model.Repository, refs []*model.Reference) {
	require := require.New(t)
	obtainedRefs := modelToMemRefs(t, obtained.References)
	expectedRefs := modelToMemRefs(t, refs)
	require.Equal(expectedRefs, obtainedRefs)
}

func modelToMemRefs(t *testing.T, refs []*model.Reference) memory.ReferenceStorage {
	require := require.New(t)
	m := memory.ReferenceStorage{}
	for _, ref := range refs {
		// skip HEAD, since we added it for avoiding go-git errors
		if ref.Name == "refs/heads/HEAD" {
			continue
		}

		err := m.SetReference(ref.GitReference())
		require.NoError(err)
	}

	return m
}

func repoToMemRefs(t *testing.T, r *git.Repository) memory.ReferenceStorage {
	require := require.New(t)
	refr := NewGitReferencer(r)
	refs, err := refr.References()
	require.NoError(err)
	return modelToMemRefs(t, refs)
}

func checkNoFiles(t *testing.T, fs billy.Filesystem) {
	require := require.New(t)

	fis, err := fs.ReadDir("")
	if !os.IsNotExist(err) {
		require.NoError(err)
	}

	for _, fi := range fis {
		require.True(fi.IsDir(), "unexpected file: %s", fi.Name())

		fsr, err := fs.Chroot(fi.Name())
		require.NoError(err)
		checkNoFiles(t, fsr)
	}
}

func (s *ArchiverSuite) TestIsProcessableRepository() {
	const endpoint = "git@github.com:rick/morty.git"
	var (
		now       = time.Now()
		endpoints = []string{endpoint}
		isFork    = false
	)

	_, err := RepositoryID(endpoints, &isFork, s.store)
	s.NoError(err)

	modelRepos, err := s.store.GetByEndpoints(endpoint)
	s.NoError(err)
	s.Assertions.True(len(modelRepos) == 1)

	modelRepo := modelRepos[0]
	s.Assertions.True(modelRepo.Status == model.Pending)

	// simulate a wrong status in the main queue
	s.NoError(s.store.SetStatus(modelRepo, model.Fetching))

	// the repo can't be processed
	s.Error(s.a.isProcessableRepository(modelRepo, &now))

	// the status after the error must be 'pending'
	s.Assertions.True(modelRepo.Status == model.Pending)
}

func (s *ArchiverSuite) TestSymbolicReference() {
	for _, ct := range ChangesFixtures {
		if ct.OldReferences == nil {
			continue
		}

		s.T().Run(ct.TestName, func(t *testing.T) {
			require := require.New(t)

			var rid kallax.ULID
			// r, err := ct.OldRepository()
			r, err := ct.NewRepository()
			require.NoError(err)
			var hash model.SHA1
			err = withInProcRepository(hash, r, func(url string) error {
				rid = s.newRepositoryModel(url)

				err := r.Storer.SetReference(
					plumbing.NewSymbolicReference("refs/heads/symbolic", "refs/heads/master"))
				if err != nil {
					return err
				}

				master, err := r.Reference("refs/heads/master", false)
				require.NoError(err)

				r.Storer.SetReference(
					plumbing.NewHashReference("refs/heads/merda", master.Hash()))

				return s.a.Do(context.TODO(), &Job{RepositoryID: uuid.UUID(rid)})
			})
			require.NoError(err)

			name := plumbing.ReferenceName(
				fmt.Sprintf("refs/heads/symbolic/%s", rid.String()))
			target := plumbing.ReferenceName(
				fmt.Sprintf("refs/heads/master/%s", rid.String()))

			m, err := s.store.Get(rid)
			require.NoError(err)

			inits := make(map[plumbing.Hash]bool)
			for _, ref := range m.References {
				inits[plumbing.Hash(ref.Init)] = true
			}

			var found = false
			for init := range inits {
				tx, err := s.a.RootedTransactioner.Begin(context.TODO(), init)
				require.NoError(err)
				defer tx.Rollback()

				repo, err := git.Open(tx.Storer(), nil)
				require.NoError(err)

				refs, _ := repo.References()
				refs.ForEach(func(r *plumbing.Reference) error {
					println("ref", r.Name())
					return nil
				})

				ref, err := repo.Reference(name, false)
				if err == nil {
					require.Equal(target, ref.Target())
					found = true
					break
				}
			}

			require.True(found, "symbolic reference not found")

		})
	}
}

type cArchiver struct {
	m *model.Repository
	a *Archiver
	s RepositoryStore
	r *git.Repository
}

func customArchiver(
	t *testing.T,
	rootedFs, txFs, tmpFs billy.Filesystem,
) cArchiver {
	t.Helper()
	require := require.New(t)

	var suite test.Suite
	suite.SetT(t)
	suite.Setup()

	rawStore := model.NewRepositoryStore(suite.DB)
	store := storage.FromDatabase(suite.DB)

	bucket := 0

	// This copier uses a rooted fs that fails writing.
	copier := repository.NewCopier(
		txFs,
		repository.NewLocalFs(rootedFs),
		bucket)
	tx := repository.NewSivaRootedTransactioner(copier)

	ls, err := lock.NewLocal().NewSession(&lock.SessionConfig{
		Timeout: defaultTimeout,
	})
	require.NoError(err)

	a := NewArchiver(store, tx, NewTemporaryCloner(tmpFs),
		ls, defaultTimeout, copier)

	repoFixture := fixtures.ByTag("worktree").One()
	repoFS := repoFixture.Worktree()
	repoPath := repoFS.Root()
	repoURL := fmt.Sprintf("file://%s", repoPath)
	storage := filesystem.NewStorage(repoFS, nil)
	repo, err := git.Open(storage, repoFS)
	require.NoError(err)

	mr := model.NewRepository()
	mr.Endpoints = append(mr.Endpoints, repoURL)
	updated, err := rawStore.Save(mr)
	require.NoError(err)
	require.False(updated)

	return cArchiver{m: mr, a: a, s: store, r: repo}
}

func TestDeleteTmpOnError(t *testing.T) {
	require := require.New(t)
	fixtures.Init()
	defer fixtures.Clean()

	tmpPath, err := ioutil.TempDir(os.TempDir(),
		fmt.Sprintf("borges-tests%d", rand.Uint32()))
	require.NoError(err)

	defer os.RemoveAll(tmpPath)

	fs := osfs.New(tmpPath)

	rootedFs, err := fs.Chroot("rooted")
	require.NoError(err)
	txFs, err := fs.Chroot("tx")
	require.NoError(err)
	tmpFs, err := fs.Chroot("tmp")
	require.NoError(err)

	bfs := NewBrokenFS(txFs)
	a := customArchiver(t, rootedFs, bfs, tmpFs)

	// first time error because of BrokenFS, uses fastpath
	err = a.a.Do(context.TODO(), &Job{RepositoryID: uuid.UUID(a.m.ID)})
	require.Error(err)

	// deactivate BrokenFS to let it push changes
	bfs.MaxCount = -1
	err = a.a.Do(context.TODO(), &Job{RepositoryID: uuid.UUID(a.m.ID)})
	require.NoError(err)

	// delete references so it needs to push
	err = deleteReferences(
		rootedFs,
		a.m.ID.String(),
		"b029517f6300c2da0f4b651b8642506cd6aaf45d")
	require.NoError(err)

	// delete one reference from database so push is done
	r, err := a.s.Get(a.m.ID)
	require.NoError(err)
	require.Len(a.m.References, 2)

	r.References = r.References[1:]
	err = a.s.UpdateFetched(r, time.Now())
	require.NoError(err)

	// activate BrokenFS, test push
	bfs.MaxCount = 4
	err = a.a.Do(context.TODO(), &Job{RepositoryID: uuid.UUID(a.m.ID)})
	require.Error(err)

	// After an error the temporary repository should be deleted.
	files, err := tmpFs.ReadDir("/local_repos")
	require.NoError(err)
	require.Len(files, 0)
}

func deleteReferences(fs billy.Filesystem, id, root string) error {
	path := fmt.Sprintf("%s.siva", root)
	siva, err := sivafs.NewFilesystem(fs, path, nil)
	if err != nil {
		return err
	}

	storage := filesystem.NewStorage(siva, nil)
	repo, err := git.Open(storage, nil)
	if err != nil {
		return err
	}

	it, err := repo.References()
	if err != nil {
		return err
	}
	defer it.Close()

	err = it.ForEach(func(r *plumbing.Reference) error {
		if strings.HasSuffix(r.Name().String(), id) {
			return repo.Storer.RemoveReference(r.Name())
		}

		return nil
	})

	return err
}

func TestMissingSivaFile(t *testing.T) {
	require := require.New(t)
	fixtures.Init()
	defer fixtures.Clean()

	tmpPath, err := ioutil.TempDir(os.TempDir(),
		fmt.Sprintf("borges-tests%d", rand.Uint32()))
	require.NoError(err)

	defer os.RemoveAll(tmpPath)

	fs := osfs.New(tmpPath)

	rootedFs, err := fs.Chroot("rooted")
	require.NoError(err)
	txFs, err := fs.Chroot("tx")
	require.NoError(err)
	tmpFs, err := fs.Chroot("tmp")
	require.NoError(err)

	// r, a, store, _ := customArchiver(t, rootedFs, txFs, tmpFs)
	a := customArchiver(t, rootedFs, txFs, tmpFs)

	err = a.a.Do(context.TODO(), &Job{RepositoryID: uuid.UUID(a.m.ID)})
	require.NoError(err)

	// rename siva file to *.tmp so copier is not able to find it
	d, err := rootedFs.ReadDir(".")
	require.NoError(err)
	for _, f := range d {
		rootedFs.Rename(f.Name(), fmt.Sprintf("%s.tmp", f.Name()))
	}

	// add reference with its init commit
	ref := model.NewReference()
	ref.Init = model.NewSHA1("b029517f6300c2da0f4b651b8642506cd6aaf45d")
	a.m.References = []*model.Reference{ref}
	err = a.s.UpdateFetched(a.m, time.Now())
	require.NoError(err)

	err = a.a.Do(context.TODO(), &Job{RepositoryID: uuid.UUID(a.m.ID)})
	require.Error(err)

	// the error that caused the job to fail is lost in
	// pushChangesToRootedRepositories so it is not possible to check for
	// ErrEmptySiva. We restore again the siva file to make sure that the
	// missing siva file was the problem. We also add a new commit
	// so push does not fail because there are no changes.

	d, err = rootedFs.ReadDir(".")
	require.NoError(err)
	for _, f := range d {
		n := strings.TrimSuffix(f.Name(), ".tmp")
		if f.Name() != n {
			rootedFs.Rename(f.Name(), n)
		}
	}

	url := a.m.Endpoints[0]
	url = strings.TrimPrefix(url, "file://")
	g, err := git.PlainOpen(url)
	require.NoError(err)

	wt, err := g.Worktree()
	require.NoError(err)

	testFile := "test_file"
	wtFs := wt.Filesystem
	f, err := wtFs.Create(testFile)
	require.NoError(err)
	_, err = f.Write([]byte("test data"))
	require.NoError(err)
	require.NoError(f.Close())

	_, err = wt.Add(testFile)
	require.NoError(err)
	_, err = wt.Commit("test commit", &git.CommitOptions{
		Author: &object.Signature{
			Name:  "name",
			Email: "name@example.com",
			When:  time.Now(),
		},
	})
	require.NoError(err)

	err = a.a.Do(context.TODO(), &Job{RepositoryID: uuid.UUID(a.m.ID)})
	require.NoError(err)
}

func TestSymbolicReference(t *testing.T) {

}

func NewBrokenFS(fs billy.Filesystem) *BrokenFS {
	return &BrokenFS{
		Filesystem: fs,
		MaxCount:   2,
	}
}

type BrokenFS struct {
	billy.Filesystem
	MaxCount int
}

func (fs *BrokenFS) OpenFile(
	name string,
	flag int,
	perm os.FileMode,
) (billy.File, error) {
	file, err := fs.Filesystem.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}

	return &BrokenFile{
		File:     file,
		MaxCount: fs.MaxCount,
	}, nil
}

type BrokenFile struct {
	billy.File
	count    int
	MaxCount int
}

func (fs *BrokenFile) Write(p []byte) (int, error) {
	if fs.MaxCount >= 0 && fs.count >= fs.MaxCount {
		return 0, fmt.Errorf("could not write to broken file")
	}

	fs.count++

	return fs.File.Write(p)
}
