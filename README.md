# Crux Dataflow Spike

*SPIKE / pre-ALPHA*

Integrates Crux with https://github.com/sixthnormal/clj-3df and
https://github.com/comnik/declarative-dataflow

Build and run the declarative-dataflow v0.2.0 server directly from
git:

```
git clone https://github.com/comnik/declarative-dataflow/tree/v0.2.0
git checkout v0.2.0
cd server
cargo build
cargo run
```

There's a version of this binary living at
`resources/declarative-server-v0.2.0-x86_64-unknown-linux-gnu` as
well. Note that the server really has v0.1.0, but the
declarative-dataflow version is v0.2.0.
