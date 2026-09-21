module github.com/apache/airflow/kubernetes-tests/lang_sdk/go_example

go 1.25.0

require github.com/apache/airflow/go-sdk v0.0.0

require (
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/spf13/cobra v1.10.1 // indirect
	github.com/spf13/pflag v1.0.10 // indirect
	github.com/vmihailenco/msgpack/v5 v5.4.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// The Go SDK is unreleased, so resolve it from the in-repo sources.
replace github.com/apache/airflow/go-sdk => ../../../go-sdk

// airflow-go-pack (the SDK's bundle packer) builds this package into a
// self-contained Airflow bundle binary; expose it as a module tool so
// `go tool airflow-go-pack` works from this directory.
tool github.com/apache/airflow/go-sdk/cmd/airflow-go-pack
