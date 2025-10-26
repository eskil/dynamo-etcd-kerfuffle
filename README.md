# Dynamo etcd kerfuffle

Dynamo fails during a leader election/network hiccup. In this repo, I'm exploring that a bit.

## What

The repo has a copy of dynamos `lib/runtime`, which includes it's etcd client code. `main.rs` is a dummy program that uses that code to get a lease and check it's alive.

`rust-client/lib/runtime/src/transports/etcd/lease.rs` is modified to add retry logic when the keep alive loop fails. There's a bool (`ENABLE_RETRY`) to toggle between that and the original logic.

Besides that, all changes are just spraying debug printfs all over.

## How-to

The docker-compose file runs 3 etcds. The makefile has some targets to build & run the rust code and cause etcd leadership issues.

Here's how I've been testing

```shell
make compose # bring up everything
make rust-logs # monitor rust
```

Periodically restart the leader
```shell
while True; do sleep 60; make restart-leader; done
```

Explicitiy kill the killer
```shell
make restart-leader
```

Cause a split by pausing the leader
```shell
make partition
```

If you want to tweak the rust code, just kill the rust-client container with `make rust-stop` and rebuild & start with `make rust-run`.

## Conclusion

Without the retry, `main.rs` acts as we've seen in prod. A etcd hiccup causes death.

With the retry, it's more resistent to eg. a leadership change/short partition.
