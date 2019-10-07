# Crux 3DF Spike

Example of the crux-dataflow module. Uses the embedded
declarative-dataflow server from crux-dataflow. x86_64 Linux only.

(This example project also contains a Dockerfile and a
docker-compose.yml, but they are using HEAD and not pinned to a
specific version of declarative-dataflow.)

Run the example:

```
lein run
```

This should output:

```
["patrik-email"
 (([{:String "p@p.com"}] {:TxId 6} 1)
  ([{:String "p@p.com"}] {:TxId 7} -1))]
["patrik-likes"
 (([{:String "apples"}] {:TxId 6} 1)
  ([{:String "apples"}] {:TxId 7} -1)
  ([{:String "bananas"}] {:TxId 6} 1)
  ([{:String "bananas"}] {:TxId 7} -1)
  ([{:String "change this"}] {:TxId 7} 1)
  ([{:String "something new"}] {:TxId 7} 1))]
["patrik-knows-1" (([{:Eid 3}] {:TxId 7} 1))]
```

The last query example using a transitive rule, "patrik-knows",
doesn't seem to work.

Note that the `data` directory is kept between runs and needs to be
deleted to start fresh.
