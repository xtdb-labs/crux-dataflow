# Crux Dataflow Spike

*pre-ALPHA*

Integrates Crux with [Declarative Dataflow](https://github.com/comnik/declarative-dataflow)
via [clj-3df](https://github.com/sixthnormal/clj-3df).

You can submit Datalog queries and subscribe for updates. You can
choose updates of different form. See `crux-dataflow.dev` ns for examples.


## More context
Declarative Dataflow is a reactive data environment that not only computes
queries but then computes and broadcasts the differential updates to those queries.

Itself it's built on top of
https://github.com/timelydataflow/timely-dataflow
https://github.com/comnik/declarative-dataflow/tree/master/docs/adr

See paper "Naiad: a timely dataflow system"
 https://dl.acm.org/citation.cfm?id=2522738

## Build and run
Build and run the declarative-dataflow v0.2.0 server directly from
git:

```
git clone https://github.com/comnik/declarative-dataflow/tree/v0.2.0
git checkout v0.2.0
cd server
cargo build
cargo run --release
```

There's a Linux version of this binary living at
`resources/declarative-server-v0.2.0-x86_64-unknown-linux-gnu` as
well. Note that the server really has v0.1.0, but the
declarative-dataflow version is v0.2.0.

## Use
See `src/crux-dataflow/dev.clj` for examples.

```clojure
(ns crux-dataflow.dev
  (:require
    [crux.api :as api]
    [crux-dataflow.api-2 :as dataflow]
    [clojure.pprint :as pp]
    [crux-dataflow.schema :as schema])
  (:import (java.util.concurrent LinkedBlockingQueue TimeUnit)
           (java.io Closeable)
           (java.time Duration)))

; 1. start node
(def node
  (api/start-node
    {:crux.node/topology :crux.standalone/topology
     :crux.node/kv-store :crux.kv.rocksdb/kv
     :crux.standalone/event-log-kv-store :crux.kv.rocksdb/kv
     :crux.standalone/event-log-dir "data/eventlog"
     :crux.kv/db-dir "data/db-dir"}))

; 2. connect to Declarative Dataflow server
(def ^Closeable crux-3df
  (dataflow/start-dataflow-tx-listener
    node
    {:crux.dataflow/schema full-schema
     :crux.dataflow/debug-connection? true
     :crux.dataflow/embed-server?     false}))

; 3 Subscribe query
(def ^LinkedBlockingQueue sub2
  (dataflow/subscribe-query! crux-3df
    {:crux.dataflow/sub-id ::three
     :crux.dataflow/query-name "user-with-eid"
     :crux.dataflow/results-shape :crux.dataflow.results-shape/maps
     :crux.dataflow/results-root-symbol '?user
     :crux.dataflow/query
     {:find ['?user '?name '?email]
      :where
      [['?user :user/name '?name]
       ['?user :user/email '?email]]}}))

; in the background your app writes
(api/submit-tx node
  [[:crux.tx/put
    {:crux.db/id :ids/patrik
     :user/name  "Pat3"
     :user/email "pat@pat.pat3"}]])

; Consume results from the queue
(.poll sub2 10 TimeUnit/MILLISECONDS)
; yields
{:updated-props
 {"#crux/id :ids/patrik"
  {:crux.db/id "#crux/id :ids/patrik",
   :user/name "Pat3",
   :user/email "pat@pat.pat3"},
  "#crux/id :patrik"
  {:crux.db/id "#crux/id :patrik",
   :user/name "7",
   :user/email "7"}}}
```


## TODO
- [x] investigate the indexing bug with tx-log
- [x] split subscription key and query key
- [x] 3DF string/uuid ids
- [x] transact in only required query data
- [x] transact in full results data, so better query modification
- [x] query data shape - vector
- [x] polling failure resiliency
- [x] query data shape - map

- [ ] automatic query keys
  No point in giving query an id, as query just identifies itself.
- [ ] collection values ser / deser
- [ ] ingest transact in entities bindings from rules
- [ ] evict/delete/cas txs

optional
- [ ] manage external 3df server processes?


## Known Caveats
- Issue #364 causes worker thread death and possibly we're missing some of the updates
- Maps results shape is tested only for small queries and must include one entity id

- If you submit schema to a 3df server the second time – it seems to work worse,
  but not sure.
- There's a ~350ms latency between diff tuples sent to 3df and results arrival.
  Partly because of a built-in setting, and partly because of architecture. For
  A batch of 500 submits takes only 1500ms to process.
  https://github.com/comnik/declarative-dataflow/blob/master/server/src/main.rs#L581
