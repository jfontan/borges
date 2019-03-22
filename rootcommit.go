package borges

import (
	"gopkg.in/src-d/core-retrieval.v0/model"
	"gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

var (
	// ErrObjectTypeNotSupported returned by ResolveCommit when the referenced
	// object isn't a Commit nor a Tag.
	ErrObjectTypeNotSupported = errors.NewKind("object type %q not supported")
)

// ResolveCommit gets the hash of a commit that is referenced by a tag, per
// example. The only resolvable objects are Tags and Commits, if the object is
// not one of them, this method will return an ErrObjectTypeNotSupported. The
// output hash always will be a Commit hash.
func ResolveCommit(r *git.Repository, h plumbing.Hash) (*object.Commit, error) {
	obj, err := r.Object(plumbing.AnyObject, h)
	if err != nil {
		return nil, err
	}

	switch o := obj.(type) {
	case *object.Commit:
		return o, nil
	case *object.Tag:
		return ResolveCommit(r, o.Target)
	default:
		return nil, ErrObjectTypeNotSupported.New(o.Type())
	}
}

type commitFrame struct {
	cursor int
	hashes []plumbing.Hash
}

func newCommitFrame(hashes ...plumbing.Hash) *commitFrame {
	return &commitFrame{0, hashes}
}

// lastHash returns the last visited hash, assuming one has at least been
// visited before. That is, this should not be called before incrementing the
// cursor for the first time.
func (f *commitFrame) lastHash() plumbing.Hash {
	return f.hashes[f.cursor-1]
}

// rootCommits returns the commits with no parents reachable from `start`. To do
// so, all the commits are iterated using a stack where frames are the parent
// commits of the last visited hash of the previous frame.
//
// As we go down, if a commit has parents, we add a new frame to the stack with
// these parents as hashes. If the commit does not have parents its a root, so
// we add it to the list found roots and keep going.
//
// If we have not visited all the hashes in the current frame it means we have
// to switch branches. That means caching the roots found so far for the last
// visited commit in the frame and reset the roots so we can find the ones for
// the new root. If these branches converge nothing happens, that point will be
// cached and we'll load them from the cache and continue. If we have visited
// all the hashes in the current frame we cache the found roots and move all the
// roots found in all the hashes in the frame to the last visited hash of the
// previous frame. The found roots now will be the same roots we pushed to the
// previous frame.
//
// After repeating this process, when we get to the root frame, we just have to
// return the roots cached for it, which will be the roots of all reachable
// commits from the start.
func rootCommits(
	r *git.Repository,
	start *object.Commit,
	seenRoots map[plumbing.Hash][]model.SHA1,
) ([]model.SHA1, error) {
	stack := []*commitFrame{
		newCommitFrame(start.Hash),
	}
	store := r.Storer
	var roots []model.SHA1

	for {
		current := len(stack) - 1
		if current < 0 {
			return nil, nil
		}

		frame := stack[current]
		if len(frame.hashes) <= frame.cursor {
			roots = deduplicateHashes(roots)
			seenRoots[frame.lastHash()] = roots

			// root frame is guaranteed to have just one hash
			if current == 0 {
				return seenRoots[frame.lastHash()], nil
			}

			// move all the roots of all the branches to the last visited
			// hash of the previous frame
			prevFrame := stack[current-1]

			prevHash := prevFrame.lastHash()
			for _, h := range frame.hashes {
				seenRoots[prevHash] = append(seenRoots[prevHash], seenRoots[h]...)
			}

			roots = deduplicateHashes(seenRoots[prevHash])
			seenRoots[prevHash] = roots

			stack = stack[:current]
			continue
		} else if frame.cursor > 0 {
			// if the frame cursor is bigger than 0 and we're not done with it
			// cache the roots of the previous hash and start anew with the
			// next branch.
			seenRoots[frame.lastHash()] = deduplicateHashes(roots)
			roots = nil
		}

		frame.cursor++
		hash := frame.lastHash()
		// use cached roots for this commit, if any
		if cachedRoots, ok := seenRoots[hash]; ok {
			roots = cachedRoots
			continue
		}

		var c *object.Commit
		if hash != start.Hash {
			obj, err := store.EncodedObject(plumbing.CommitObject, hash)
			if err != nil {
				return nil, err
			}

			do, err := object.DecodeObject(store, obj)
			if err != nil {
				return nil, err
			}

			c = do.(*object.Commit)
		} else {
			c = start
		}

		if c.NumParents() > 0 {
			stack = append(stack, newCommitFrame(c.ParentHashes...))
		} else {
			roots = append(roots, model.SHA1(c.Hash))
		}
	}
}

func deduplicateHashes(hashes []model.SHA1) []model.SHA1 {
	var set hashSet
	for _, h := range hashes {
		set.add(h)
	}
	return []model.SHA1(set)
}

type hashSet []model.SHA1

func (hs *hashSet) add(hash model.SHA1) {
	if !hs.contains(hash) {
		*hs = append(*hs, hash)
	}
}

func (hs hashSet) contains(hash model.SHA1) bool {
	for _, h := range hs {
		if h == hash {
			return true
		}
	}
	return false
}
