module gorequest

go 1.24.4

require (
	github.com/gocql/gocql v1.17.0
	github.com/google/uuid v1.6.0
	go.uber.org/zap v1.13.0
)

require (
	github.com/BurntSushi/toml v0.3.1 // indirect
	github.com/google/go-cmp v0.5.4 // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	go.uber.org/atomic v1.5.0 // indirect
	go.uber.org/multierr v1.3.0 // indirect
	go.uber.org/tools v0.0.0-20190618225709-2cfd321de3ee // indirect
	golang.org/x/lint v0.0.0-20190930215403-16217165b5de // indirect
	golang.org/x/tools v0.0.0-20191029190741-b9c20aec41a5 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	honnef.co/go/tools v0.0.1-2019.2.3 // indirect
)

replace github.com/gocql/gocql => github.com/scylladb/gocql v1.15.1
