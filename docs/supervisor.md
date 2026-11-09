# Gas Station supervisor

The supervisor owns one child process. CLI paths override `process.binary` and
`process.config`; relative paths use the working directory. The Gas Station keeps
its existing configuration and environment, including `GAS_STATION_AUTH`.
Use a separate state directory for each station. A file lock prevents two
supervisors from owning the same directory.

## Monitoring

`HealthMonitor` checks `GET /`. An optional `health.readiness_path` adds a readiness
check. The current Gas Station has no readiness endpoint, so readiness stays
unknown by default. Startup failures count only after `process.startup_grace`.

`PoolMonitor` reads the metrics port from the Gas Station config. It watches
available coin count, balance, unreleased coins, invariant violations and
oversized coins. Counter resets after a restart do not count as new failures.
Child output is inspected for known recovery signals; raw output is not echoed.

| Signal | First response | Escalation |
| --- | --- | --- |
| HTTP failures or process exit | Restart with backoff | Continue monitoring |
| Referenced object missing | Refresh, verify network and object ownership | Rescan only after confirmed divergence or operator request |
| Unreleased coin counter grows | Diagnose ownership; objects may have been destroyed legally | No automatic rebuild from this counter |
| Invariant violations or oversized coins | Refresh/reconciliation and normal Gas Station splitting | Rescan only with separate evidence |
| Persistent balance/count divergence | Refresh, then compare again | Request a full rescan |
| Network changes | Block recovery | Correct the network configuration first |

To compare on-chain state, set `pool.rpc_url`, `pool.sponsor_address` and preferably
`pool.expected_chain_id`. The first observed chain ID is pinned when no expected
ID is supplied. Ensure this JSON-RPC endpoint uses the same network as the Gas
Station fullnode. Only that sponsor's pool metrics are compared.

The available pool excludes reserved coins. Comparisons therefore require both
`gas_station_active_reservations` and `gas_station_pending_executions` gauges to
report zero. The current Gas Station does not provide these gauges; a compatible
build or adapter is needed for automatic reconciliation. Missing metrics never
mean zero. A failed scrape or RPC lookup breaks the divergence streak and blocks
automatic rescans. Known network mismatches and active work remain blockers
until a later valid observation clears them.

## Recovery

```text
Signal -> Refresh -> Wait -> Observe pool again
                                 |
                         Persistent divergence
                                 |
                         Approval + activity checks
                                 |
                   Restart with --allow-reinit
                          --delete-coin-registry
                                 |
                       Gas Station lock acquisition
```

The current Gas Station has no external refresh API. Without `recovery.refresh_url`,
the supervisor waits for its built-in refresh interval plus the grace period.
A normal restart does not force a refresh. A custom refresh hook receives a POST;
successful HTTP acceptance is followed by observation, not treated as proof of
recovery. Failed hooks are retried with the grace period and never justify a rescan.

`auto_full_rescan: false` and `require_manual_approval: true` are the defaults.
Automatic rescans require both settings to be explicitly changed, fresh idle
gauges and a verified chain. Stop incoming traffic and drain the station before
allowing destructive recovery; a scrape is a snapshot, not an admission lock.

The first full rescan respects initialization and maintenance locks. Setting
`aggression.ignore_locks_after` above zero allows bypassing locks after the stated
number of explicit lock failures. This does not bypass approval or activity/network
guards. `max_full_rescans` limits attempts per supervisor run. Recovery flags are
one-shot: crash and health restarts always use the normal command.

## Approve one rescan

`state_dir/status.json` contains the child PID, health, pool observations, recovery
counters and `pending_recovery`. It contains no auth tokens or private keys.

After checking the network, stopping traffic and draining outstanding work, approve
the pending request by writing its ID:

```sh
jq -r '.pending_recovery.id // empty' .supervisor/status.json > .supervisor/approve-recovery
```

The supervisor consumes this file before one full-rescan launch. Old IDs are not
accepted for later requests. An operator can also request and approve one rescan
at startup with `--approve-recovery`. Manual approval acknowledges external
verification when activity metrics are unavailable; it never overrides known
active work or a detected network mismatch.

`Ctrl+C` and `SIGTERM` terminate the owned process group. If graceful shutdown
exceeds `shutdown_timeout`, the supervisor kills and reaps it.
