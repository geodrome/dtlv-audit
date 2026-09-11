# dtlv-audit

Durable transaction history and a queryable audit projection for
[Datalevin](https://github.com/datalevin/datalevin).

Datalevin keeps no history: retracted data is gone and there is no as-of.
`tx!` journals each transaction report into the *same* LMDB transaction as the
domain write; a polling projector turns that journal into a separate audit DB
you can query with Datalog.

## Usage

```clojure
{:deps {dtlv-audit/dtlv-audit {:local/root "../dtlv-audit"}}}
```

```clojure
(def store (core/open! {:dir         "data/domain"
                        :audit-dir   "data/audit"
                        :schema      domain-schema
                        :conn-opts   {:auto-entity-time? true}
                        :annotations {:audit/actor {:db/valueType :db.type/string}
                                      :audit/op    {}}}))
(def stop! (core/start-projector! store))

(core/tx! store [{:quote/number "Q-1042"}] {:audit/actor "geo"})

(core/history store 42)
(core/by-annotation store :audit/actor "geo")
```

`:conn-opts` and `:audit-conn-opts` pass through to `d/get-conn` for the domain
and audit connections. Nothing is defaulted: a store opened here behaves
exactly as the same store opened with `d/get-conn` directly. The one exception
is `:closed-schema?` on the audit DB, which `:strict?` owns.

### Annotations

Annotations are a declared vocabulary — checked on write, merged into the audit
schema, enforced there by `:closed-schema?`. Declare one with no
`:db/valueType` to make it untyped: it takes any value, stored as EDN and
matchable by equality, which is what carries a command in the shape
`[:entity/verb args]`. Untyped is still *declared*; an unknown key is still
rejected. `:strict? false` is the escape hatch for genuinely unknown keys.

Annotations are handed to Datalevin as well as journaled, so they reach the
tx-report and anything registered with `d/listen!` — a listener sees why a write
happened, not only what changed. Datalevin does not persist them; the journal
remains the only durable record.

`tx!` returns the Datalevin tx-report without its `:db-before`/`:db-after`
handles — both are dead by the time it returns, and in Datalevin both wrap the
same mutable store anyway, so neither shows prior state. `:tx-data` is the
before/after that survives: the old value as a retraction, the new one as an
assertion. Two ids are added, `:tx` and `:commit`, which place the write in the
journal.

### Read-then-write

Work that must read and write under one transaction scopes the store to one.
Writes through the scoped store join it, and a throw rolls back the domain
writes and their journal entries together:

```clojure
(core/with-tx [s store]
  (when (ok? (d/q ... (core/db s)))
    (core/tx! s tx-data {:audit/op [:quote/create args]})))
```

### Transactions and commits

Datalevin assigns a tx id per `d/transact!` and commits per
`d/with-transaction`, so one commit may hold several txs. Each journal entry
records both: `tx` is its own id and the journal key, `:commit` is the tx id the
commit began at. A write outside a transaction is its own commit, so the two are
equal and grouping by `:commit` needs no special case.

The journal is the only place this survives. A datom's tx is not persisted, and
there is no `as-of`, `history` or `tx-range` to ask — so which writes were atomic
is a fact that exists here or nowhere.

### Clearing

The journal is a KV sub-database, so retracting the domain data leaves it
intact: emptying a store by retracting its datoms leaves a complete record of
everything that used to be there. `clear-journal!` is what empties it; deleting
the store directory takes it too. The audit DB is derived, so `rebuild!` is
what reconciles a projection with a journal that was cleared.

### Required JVM options

Datalevin needs these on JDK 17+ (they are set for you in the `:dev` and
`:test` aliases, but your own project must set them too):

```clojure
:jvm-opts ["--add-opens=java.base/java.nio=ALL-UNNAMED"
           "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
           "--enable-native-access=ALL-UNNAMED"]  ; JDK 24+, silences native-access warnings
```

### Logging

Logging goes through [Trove](https://github.com/taoensso/trove), a facade — no
backend is imposed on you. Out of the box logs go to `*out*`. To route them to
your own stack:

```clojure
(require '[taoensso.trove :as trove]
         '[taoensso.trove.telemere])  ; or .timbre / .slf4j / .mulog / .tools-logging

(trove/set-log-fn! (taoensso.trove.telemere/get-log-fn))
(trove/set-log-fn! nil)  ; or silence it entirely
```

Event ids emitted: `::invalid-annotation`, `::journal-gap`,
`::projector-failed`, `::projector-stuck`, `::projector-slow-stop`.

## Development

```bash
clojure -M:dev            # REPL with test path + JVM opts
clojure -X:test           # run tests
clojure -T:build jar      # build a jar
clojure -T:build install  # install to the local Maven repo
```

## License

Copyright © 2026
