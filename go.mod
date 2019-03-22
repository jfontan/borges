module github.com/src-d/borges

go 1.12

require (
	github.com/Masterminds/squirrel v1.1.0 // indirect
	github.com/go-sql-driver/mysql v1.4.1 // indirect
	github.com/hashicorp/golang-lru v0.5.1 // indirect
	github.com/lib/pq v1.0.0 // indirect
	github.com/mattn/go-sqlite3 v1.10.0 // indirect
	github.com/oklog/ulid v1.3.1 // indirect
	github.com/satori/go.uuid v1.2.0 // indirect
	github.com/src-d/go-borges v0.0.0-00010101000000-000000000000
	google.golang.org/appengine v1.5.0 // indirect
	gopkg.in/src-d/core-retrieval.v0 v0.0.0-20181128152223-27a526a7da6f
	gopkg.in/src-d/go-billy-siva.v4 v4.3.0 // indirect
	gopkg.in/src-d/go-billy.v4 v4.3.0
	gopkg.in/src-d/go-errors.v1 v1.0.0
	gopkg.in/src-d/go-git.v4 v4.10.0
	gopkg.in/src-d/go-kallax.v1 v1.3.5 // indirect
	gopkg.in/src-d/go-siva.v1 v1.4.0 // indirect
)

replace github.com/src-d/go-borges => github.com/jfontan/go-borges v0.0.0-20190322174843-3518586cba95

replace gopkg.in/src-d/go-git.v4 => github.com/jfontan/go-git v0.0.0-20190321162938-b40af0bc1b6d

replace gopkg.in/src-d/go-billy-siva.v4 => github.com/jfontan/go-billy-siva v3.0.1-0.20190322160634-e30e37634e2a+incompatible
