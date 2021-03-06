package borges

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/inconshreveable/log15"
	"github.com/satori/go.uuid"
	"github.com/src-d/borges/storage"
	"github.com/src-d/go-git-fixtures"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gopkg.in/src-d/core-retrieval.v0/model"
	rrepository "gopkg.in/src-d/core-retrieval.v0/repository"
	"gopkg.in/src-d/core-retrieval.v0/test"
	"gopkg.in/src-d/framework.v0/lock"
	"gopkg.in/src-d/go-billy.v3"
	"gopkg.in/src-d/go-billy.v3/osfs"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/storage/memory"
	"gopkg.in/src-d/go-kallax.v1"
)

func TestArchiver(t *testing.T) {
	suite.Run(t, new(ArchiverSuite))
}

type ArchiverSuite struct {
	test.Suite

	rawStore *model.RepositoryStore
	store    storage.RepoStore
	tmpPath  string
	tx       rrepository.RootedTransactioner
	txFs     billy.Filesystem
	tmpFs    billy.Filesystem
	rootedFs billy.Filesystem
	a        *Archiver
}

const defaultTimeout = 1 * time.Second

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

	s.tx = rrepository.NewSivaRootedTransactioner(rrepository.NewLocalCopier(s.rootedFs), s.txFs)

	ls, err := lock.NewLocal().NewSession(&lock.SessionConfig{
		Timeout: time.Second * 1,
	})
	s.NoError(err)

	s.a = NewArchiver(log15.New(), s.store, s.tx, NewTemporaryCloner(s.tmpFs), ls, defaultTimeout)
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
			err = WithInProcRepository(r, func(url string) error {
				rid = s.newRepositoryModel(url)
				return s.a.Do(&Job{RepositoryID: uuid.UUID(rid)})
			})

			require.Error(err)
			require.Contains(err.Error(), context.DeadlineExceeded.Error())

			_, err = s.rawStore.FindOne(model.NewRepositoryQuery().FindByID(rid).FindByStatus(model.Pending))
			require.NoError(err)
		})
	}
}

func (s *ArchiverSuite) TestReferenceUpdate() {
	for _, ct := range ChangesFixtures {
		s.T().Run(ct.TestName, func(t *testing.T) {
			var obtainedRefs []*model.Reference = ct.OldReferences
			for ic, cs := range ct.Changes { // emulate pushChangesToRootedRepositories() behaviour
				obtainedRefs = updateRepositoryReferences(obtainedRefs, cs, ic)
			}

			s.Equal(len(ct.NewReferences), len(obtainedRefs))
		})
	}
}

func (s *ArchiverSuite) TestFixtures() {
	for _, ct := range ChangesFixtures {
		s.T().Run(ct.TestName, func(t *testing.T) {
			require := require.New(t)

			or, err := ct.OldRepository()
			var rid kallax.ULID
			// emulate initial status of a repository
			err = WithInProcRepository(or, func(url string) error {
				rid = s.newRepositoryModel(url)
				return s.a.Do(&Job{RepositoryID: uuid.UUID(rid)})
			})
			require.NoError(err)

			nr, err := ct.NewRepository()
			require.NoError(err)

			err = WithInProcRepository(nr, func(url string) error {
				mr, err := s.rawStore.FindOne(model.NewRepositoryQuery().FindByID(rid))
				require.NoError(err)
				mr.Endpoints = nil
				mr.Endpoints = append(mr.Endpoints, url)
				updated, err := s.rawStore.Save(mr)
				require.NoError(err)
				require.True(updated, err)
				return s.a.Do(&Job{RepositoryID: uuid.UUID(mr.ID)})
			})
			require.NoError(err)

			checkNoFiles(t, s.txFs)
			checkNoFiles(t, s.tmpFs)

			checkReferences(t, nr, ct.NewReferences)

			// check references in database
			mr, err := s.rawStore.FindOne(model.NewRepositoryQuery().FindByID(rid))
			require.NoError(err)
			checkReferencesInDB(t, mr, ct.NewReferences)

			// check references in siva files
			fis, err := s.rootedFs.ReadDir(".")
			if len(ct.NewReferences) != 0 {
				require.NoError(err)
				initHashesInStorage := make(map[string]bool)

				for _, fi := range fis {
					initHashesInStorage[strings.Replace(fi.Name(), ".siva", "", -1)] = true
				}

				// we check that all the references that we have into the database exists as a rooted repository
				for _, ref := range mr.References {
					_, ok := initHashesInStorage[ref.Init.String()]
					require.True(ok)
				}
			}
		})
	}
}

func (s *ArchiverSuite) TestNotExistingRepository() {
	rid := s.newRepositoryModel("file:///this/repository/does/not/exists")
	err := s.a.Do(&Job{RepositoryID: uuid.UUID(rid)})
	s.NoError(err)

	mr, err := s.rawStore.FindOne(model.NewRepositoryQuery().FindByID(rid))
	s.NoError(err)

	s.Equal(model.NotFound, mr.Status)
}

func (s *ArchiverSuite) TestProcessingRepository() {
	rid := s.newRepositoryModel("git://foo.bar.baz")
	repo, err := s.rawStore.FindOne(model.NewRepositoryQuery().FindByID(rid))
	s.NoError(err)
	repo.Status = model.Fetching
	_, err = s.rawStore.Save(repo)
	s.NoError(err)

	err = s.a.Do(&Job{RepositoryID: uuid.UUID(rid)})
	s.True(ErrAlreadyFetching.Is(err))

	mr, err := s.rawStore.FindOne(model.NewRepositoryQuery().FindByID(rid))
	s.NoError(err)

	s.Equal(model.Fetching, mr.Status)
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
	require.Equal(len(refs), len(obtained.References))
	obtainedRefs := modelToMemRefs(t, obtained.References)
	expectedRefs := modelToMemRefs(t, refs)
	require.Equal(expectedRefs, obtainedRefs)
}

func modelToMemRefs(t *testing.T, refs []*model.Reference) memory.ReferenceStorage {
	require := require.New(t)
	m := memory.ReferenceStorage{}
	for _, ref := range refs {
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
