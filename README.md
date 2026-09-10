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
