# Gossip Glomers BEAM (Elixir, Gleam)

Main page: https://fly.io/dist-sys/

Maelstrom docs: https://github.com/jepsen-io/maelstrom

## Elixir

1. [X] Echo (`echo`)
    - https://fly.io/dist-sys/1/
2. [X] Unique ID Generation (`unique-ids`)
    - https://fly.io/dist-sys/2/
3. [X] Broadcast (`broadcast`)
    - https://fly.io/dist-sys/3a/
    - https://fly.io/dist-sys/3b/
    - https://fly.io/dist-sys/3c/
    - https://fly.io/dist-sys/3d/
    - https://fly.io/dist-sys/3e/
4. [X] Grow-Only Counter (`g-counter`)
    - https://fly.io/dist-sys/4/
5. [X] Kafka-Style Log (`kafka`)
    - https://fly.io/dist-sys/5a/
    - https://fly.io/dist-sys/5b/
    - https://fly.io/dist-sys/5c/
6. [X] Totally-Available... (`txn-rw-register`)
    - https://fly.io/dist-sys/6a/
    - https://fly.io/dist-sys/6b/
    - https://fly.io/dist-sys/6c/

Extras? Not listed on Fly.io Gossip Glomers, but exist within Maelstrom:
- Datomic (`txn-list-append`)
    - [ ] https://github.com/jepsen-io/maelstrom/blob/main/doc/05-datomic/01-single-node.md
        ```
        ./maelstrom test -w txn-list-append --bin EXECUTABLE --time-limit 10 --node-count 1
        ./maelstrom test -w txn-list-append --bin EXECUTABLE --time-limit 30 --node-count 1 --concurrency 10n --rate 100
        ```
    - [ ] https://github.com/jepsen-io/maelstrom/blob/main/doc/05-datomic/02-shared-state.md
        ```
        ./maelstrom test -w txn-list-append --bin EXECUTABLE --time-limit 10 --node-count 2
        ./maelstrom test -w txn-list-append --bin EXECUTABLE --time-limit 10 --node-count 2 --rate 100
        ```
    - [ ] https://github.com/jepsen-io/maelstrom/blob/main/doc/05-datomic/03-persistent-trees.md
        ```
        ./maelstrom test -w txn-list-append --bin EXECUTABLE --time-limit 10 --node-count 2 --rate 100
        ```
    - [ ] https://github.com/jepsen-io/maelstrom/blob/main/doc/05-datomic/04-optimization.md
        ```
        ./maelstrom test -w txn-list-append --bin EXECUTABLE --time-limit 10 --node-count 2 --rate 1
        ./maelstrom test -w txn-list-append --bin EXECUTABLE --time-limit 10 --node-count 2 --rate 100
        ./maelstrom test -w txn-list-append --bin EXECUTABLE --time-limit 10 --node-count 2 --rate 10
        ./maelstrom test -w txn-list-append --bin EXECUTABLE --time-limit 10 --node-count 2 --rate 100
        ./maelstrom test -w txn-list-append --bin EXECUTABLE --time-limit 10 --node-count 2 --rate 100 --consistency-models serializable
        ```
- Raft (`lin-kv`)
    - [ ] https://github.com/jepsen-io/maelstrom/blob/main/doc/06-raft/01-key-value.md
        ```
        ./maelstrom test -w lin-kv --bin EXECUTABLE --time-limit 10 --rate 10 --node-count 1 --concurrency 2n
        ./maelstrom test -w lin-kv --bin EXECUTABLE --time-limit 10 --node-count 2 --rate 10 --concurrency 2n
        ```
    - [ ] https://github.com/jepsen-io/maelstrom/blob/main/doc/06-raft/02-leader-election.md
        ```
        ./maelstrom test -w lin-kv --bin EXECUTABLE --time-limit 10 --node-count 3 --concurrency 2n --rate 10
        ./maelstrom test -w lin-kv --bin EXECUTABLE --time-limit 10 --node-count 3 --concurrency 2n
        ./maelstrom test -w lin-kv --bin EXECUTABLE --time-limit 10 --node-count 3 --concurrency 2n
        ./maelstrom test -w lin-kv --bin EXECUTABLE --time-limit 10 --node-count 3 --concurrency 2n --rate 0
        ```
    - [ ] https://github.com/jepsen-io/maelstrom/blob/main/doc/06-raft/03-replication.md
        ```
        ./maelstrom test -w lin-kv --bin EXECUTABLE --time-limit 10 --node-count 3 --concurrency 2n --rate 5
        ./maelstrom test -w lin-kv --bin EXECUTABLE --time-limit 60 --node-count 3 --concurrency 2n --rate 1 --nemesis partition
        ```
    - [ ] https://github.com/jepsen-io/maelstrom/blob/main/doc/06-raft/04-committing.md
        ```
        ./maelstrom test -w lin-kv --bin EXECUTABLE --time-limit 60 --node-count 3 --concurrency 10n --rate 100 --nemesis partition --nemesis-interval 3 --test-count 5
        ./maelstrom test -w lin-kv --bin EXECUTABLE --time-limit 10 --node-count 3 --concurrency 2n --rate 100
        ./maelstrom test -w lin-kv --bin EXECUTABLE --time-limit 30 --node-count 3 --concurrency 2n --rate 1 --nemesis partition --nemesis-interval 10
        ./maelstrom test -w lin-kv --bin EXECUTABLE --node-count 3 --concurrency 4n --rate 30 --time-limit 60 --nemesis partition --nemesis-interval 10 --test-count 10
        ```

## Gleam

TBD!

## Miscellaneous Links

- https://notes.eatonphil.com/2025-08-09-what-even-is-distributed-systems.html
- https://transactional.blog/blog/2024-data-replication-design-spectrum
- https://transactional.blog/talk/enough-with-all-the-raft
