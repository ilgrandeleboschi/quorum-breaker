# quorum-breaker

**Cluster-synchronized `HALF_OPEN` recovery for [Resilience4j](https://resilience4j.readme.io/)'s `CircuitBreaker`.**

[![CI](https://github.com/ilgrandeleboschi/quorum-breaker/actions/workflows/ci.yml/badge.svg)](https://github.com/ilgrandeleboschi/quorum-breaker/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-21%2B-orange)](#requirements)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.2%E2%80%934.1-brightgreen)](#requirements)

Run N replicas of a service, and each one tests `HALF_OPEN` recovery independently — a dependency
sized for 5 concurrent probes can end up seeing N×5, right when it's most fragile. quorum-breaker
moves that trial budget out of each instance and into the cluster: exactly
`permittedNumberOfCallsInHalfOpenState` calls get through *cluster-wide*, and the CLOSE/REOPEN
decision is made once and broadcast to every node. No aspect, no annotation, no code change — it
swaps every breaker already in your `CircuitBreakerRegistry` for a cluster-aware decorator.

## Quick start

For a Spring Boot app that wants Hazelcast-backed coordination, fully autoconfigured:

```xml
<dependency>
    <groupId>uk.groveio</groupId>
    <artifactId>quorum-breaker-spring</artifactId>
    <version>1.0.0</version>
</dependency>
<dependency>
    <groupId>uk.groveio</groupId>
    <artifactId>quorum-breaker-hazelcast-spring</artifactId>
    <version>1.0.0</version>
</dependency>
```

`spring-boot-starter` is `provided` scope on both modules — quorum-breaker doesn't pin a version,
it builds against whatever your own app already depends on. `hazelcast` itself is a normal compile
dependency of `quorum-breaker-hazelcast-spring` (transitively pulled in), so you don't need to add
it yourself unless you want a specific version.

Then give it a Hazelcast cluster to coordinate through — either bean works:

```java
@Bean
HazelcastInstance hazelcastInstance() {
    // an instance you already run, e.g. for caching or sessions
    return Hazelcast.newHazelcastInstance(myConfig);
}
```

```java
@Bean
Config hazelcastConfig() {
    // let quorum-breaker start and own its own embedded node
    Config config = new Config();
    config.getNetworkConfig().getJoin().getTcpIpConfig()
        .setEnabled(true).addMember("10.0.0.1").addMember("10.0.0.2");
    return config;
}
```

That's it. Every `CircuitBreaker` your app creates through the standard Resilience4j
`CircuitBreakerRegistry` — annotated with `@CircuitBreaker` or created programmatically — is now
cluster-aware.

`quorum-breaker-hazelcast-spring` namespaces its shared state under `spring.application.name`, so
that two unrelated applications sharing the same managed Hazelcast cluster never collide on the
same map keys. If `spring.application.name` isn't set, context startup fails fast rather than
defaulting silently — the same way an unreachable database fails a `DataSource` bean.

If neither bean is present, quorum-breaker logs a warning and steps aside entirely: your breakers
keep running exactly as plain Resilience4j, no embedded node gets started behind your back.

A shared window is kept alive on the cluster for `slowCallDurationThreshold` (from each breaker's
own `CircuitBreakerConfig`) plus a margin, so it comfortably outlives however long that breaker's
trial calls are expected to take. That margin is configurable, with `quorum-breaker-spring`:

```yaml
quorum-breaker:
  window-ttl-padding: 5s
```

It defaults to 3 seconds if not set.

**Not on Spring?** Take just `quorum-breaker-core` and `quorum-breaker-hazelcast` and wire it up
yourself:

```java
QuorumBreakerRegistryBinder binder = new QuorumBreakerRegistryBinder(
        circuitBreakerRegistry, HazelcastClusterCoordinator.wrap(hazelcastInstance, "orders-service"), Clock.systemUTC());
binder.bindAll();
```

The second argument to `wrap`/`embedded`/`client` is a namespace, required on every factory method.
It's used to scope quorum-breaker's shared state on the Hazelcast cluster, so that two unrelated
applications connected to the same managed cluster never collide on the same map keys — pick
something that uniquely identifies your application (an artifact id, a service name) and keep it
stable across replicas. Your application's own name is usually the simplest choice — the same value
`quorum-breaker-hazelcast-spring` defaults to automatically from `spring.application.name`.

`QuorumBreakerRegistryBinder` implements `AutoCloseable`. Call `binder.close()` when you're done with
it (app shutdown, or before replacing it with a new one) to stop its background resubscribe loop and
close any live remote-transition subscriptions. `quorum-breaker-spring` does this for you automatically,
since Spring calls `close()` on beans by convention.

`wrap`/`embedded`/`client` all run their Hazelcast calls on a dedicated, named virtual-thread-per-task
executor by default, so they never compete with the rest of your application for platform threads.
Pass your own `Executor` as the last argument to any of them if you need to, e.g. to keep cluster calls
deterministic in a test or to propagate tracing/MDC context across that boundary.

If `resilience4j-micrometer` and a `MeterRegistry` are both on your classpath, `quorum-breaker-spring`
binds the cluster-coordinator guard's health to your `MeterRegistry` automatically. Wiring it up
yourself in plain Java is one line:

```java
CircuitBreaker guard = binder.clusterCoordinatorGuard();
CircuitBreakerRegistry guardRegistry = CircuitBreakerRegistry.ofDefaults();
guardRegistry.circuitBreaker(guard.getName());
guardRegistry.replace(guard.getName(), guard);
TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(guardRegistry).bindTo(meterRegistry);
```

(a small dedicated registry, not your application's own `CircuitBreakerRegistry` — the guard must never
go through `bindAll()`'s own binding logic, or it would end up coordinating itself through the very
cluster call it's meant to guard.)

### Redis instead of Hazelcast

`quorum-breaker-redis` and `quorum-breaker-redis-spring` are newer than the Hazelcast modules. The
coordination logic mirrors the Hazelcast module's; see [Building](#building) for how its test suite
is split between a Mockito-based behavioural suite (runs everywhere, every build) and a live-Redis
suite (runs against a real containerized Redis, opt-in). If your stack already runs Redis (for
caching or sessions, say) and you'd rather not stand up Hazelcast just for this, swap the two
dependencies:

```xml
<dependency>
    <groupId>uk.groveio</groupId>
    <artifactId>quorum-breaker-spring</artifactId>
    <version>1.0.0</version>
</dependency>
<dependency>
    <groupId>uk.groveio</groupId>
    <artifactId>quorum-breaker-redis-spring</artifactId>
    <version>1.0.0</version>
</dependency>
```

`lettuce-core` is a normal compile dependency of `quorum-breaker-redis-spring` (transitively pulled
in), the same way `hazelcast` is for the Hazelcast module. Then give it something to connect
through — either bean works:

```java
@Bean
RedisClient redisClient() {
    // a Lettuce client you already run, e.g. for caching or sessions
    return RedisClient.create(RedisURI.create("redis://localhost:6379"));
}
```

```java
@Bean
RedisURI redisUri() {
    // let quorum-breaker create and own its own RedisClient
    return RedisURI.create("redis://redis.internal:6379");
}
```

Same namespacing under `spring.application.name`, the same fail-fast-without-it behaviour, and the
same "logs a warning and steps aside" fallback when neither bean is present, as the Hazelcast module.

**Not on Spring?** Take just `quorum-breaker-core` and `quorum-breaker-redis`:

```java
QuorumBreakerRegistryBinder binder = new QuorumBreakerRegistryBinder(
        circuitBreakerRegistry,
        RedisClusterCoordinator.client(RedisURI.create("redis://localhost:6379"), "orders-service"),
        Clock.systemUTC());
binder.bindAll();
```

Same namespace contract, same default virtual-thread executor you can override as the last argument,
same `binder.close()` lifecycle as Hazelcast's `wrap`/`embedded`/`client`. One difference: Redis has
no concept of an embeddable in-process node, so there's no `embedded()` factory method here —
`wrap(RedisClient, ...)` attaches to a client you already own and never closes it, `client(RedisURI, ...)`
creates and owns its own (client and connections alike), and `wrap(StatefulRedisConnection,
StatefulRedisPubSubConnection, ...)` attaches to connections you already own if you'd rather manage
the `RedisClient` yourself.

## The problem, in detail

Resilience4j's `CircuitBreaker` does exactly what it's configured to do — per instance. That's the
part worth noticing: the `permittedNumberOfCallsInHalfOpenState` budget it uses to test whether a
dependency has recovered is scoped to that one instance, with no notion of the other replicas
running the exact same breaker.

Run 10 replicas of a service, and a dependency sized to take 5 concurrent probe requests can end up
seeing 10x that, because every instance tends to reach `HALF_OPEN` around the same time — they were
all watching the same failures and the same wait duration. Each instance is behaving correctly in
isolation; it's the aggregate that nobody sized for. Depending on how much headroom the dependency
actually has, that's anywhere between "no visible effect" and "the recovery probe itself becomes
the thing that keeps the dependency from recovering."

quorum-breaker fixes this by moving the `HALF_OPEN` trial budget out of each instance and into
the cluster: however many replicas you run, exactly `permittedNumberOfCallsInHalfOpenState` calls
are let through *cluster-wide*, and the CLOSE/REOPEN decision is made once and broadcast to every
node.

## How

No aspect, no annotation, no new API to learn. quorum-breaker finds every `CircuitBreaker` in
your `CircuitBreakerRegistry` and swaps it for a decorator via `registry.replace(name, ...)`.
Resilience4j's own `@CircuitBreaker` aspect resolves the instance from the registry on every
invocation, so it picks up cluster coordination transparently — your code doesn't change.

- While the breaker is `CLOSED` (or `DISABLED` / `METRICS_ONLY` / `FORCED_OPEN`), every call goes
  straight to Resilience4j. quorum-breaker doesn't get involved.
- The instant a breaker opens anywhere in the cluster, every other instance's breaker opens too.
- Once the wait duration elapses, `HALF_OPEN` trial calls claim a slot from a shared,
  atomically-decremented budget instead of a local one.
- The first node to see all trial outcomes resolved decides CLOSE or REOPEN, once, and
  broadcasts it. Every instance ends up in the same state.
- If the cluster is unreachable, quorum-breaker keeps admitting trial calls locally rather than
  blocking traffic — but never more than `permittedNumberOfCallsInHalfOpenState` for that node, so a
  coordination outage never means an availability outage, and it never means an unbounded one either.

## Modules

| Module | Description                                                                                                                                                                                                                                            |
|---|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `quorum-breaker-core` | The decorator, the registry binder, and the `ClusterCoordinator` contract.                                                                                                                                                                             |
| `quorum-breaker-spring` | Autoconfiguration that wires the binder against whatever `CircuitBreakerRegistry` and `ClusterCoordinator` beans it finds.                                                                                                                             |
| `quorum-breaker-hazelcast` | A `ClusterCoordinator` backed by Hazelcast: the shared trial budget lives in an `IMap`, updated via an optimistic compare-and-swap retry loop, and CLOSE/REOPEN decisions broadcast over an `ITopic`. Usable by constructing `QuorumBreakerRegistryBinder` yourself. |
| `quorum-breaker-hazelcast-spring` | Turns a `HazelcastInstance`/`Config` bean into the `ClusterCoordinator` bean that `quorum-breaker-spring` picks up.                                                                                                                                    |
| `quorum-breaker-redis` | A `ClusterCoordinator` backed by Redis (via Lettuce): the shared window lives as a string value updated through an atomic compare-and-swap Lua script, and CLOSE/REOPEN decisions broadcast over Redis pub/sub. Proven against a real containerized Redis in an opt-in test suite (see [Building](#building)), on top of a Mockito-based behavioural suite that runs on every build. |
| `quorum-breaker-redis-spring` | Turns a `RedisClient`/`RedisURI` bean into the `ClusterCoordinator` bean that `quorum-breaker-spring` picks up. |

## Requirements

- **Java 21+.** The published jars are compiled with `--release 21`, so they run unchanged on
  Java 21 or 25. `quorum-breaker-hazelcast` uses virtual threads internally, which is why the
  floor is 21 rather than 17.
- **Resilience4j 2.0.0–2.4.0.** Tested down to 2.0.0 and up to the currently pinned 2.4.0; newer 2.x
  releases will be tested and this range updated as they come out.
- **Hazelcast 4.0.6–5.7.0.** Tested down to 4.0.6 and up to the currently pinned 5.7.0; newer 4.x/5.x
  releases will be tested and this range updated as they come out.
- **Lettuce 5.1.0–6.7.1.RELEASE**, for `quorum-breaker-redis`/`quorum-breaker-redis-spring`. Tested
  against every minor release from 5.1 through the currently pinned 6.7 — 5.1, 5.2, 5.3, 6.0, 6.1,
  6.2, 6.3, 6.4, 6.5, 6.6, 6.7. **5.0.x does not work**: `RedisNoScriptException`, which the `EVALSHA`
  → `SCRIPT LOAD`/`EVAL` fallback relies on, was only added in 5.1.0.
- **Redis test coverage is split across two suites.** A Mockito-based suite (runs on every build,
  no Docker needed) exercises the coordinator's logic against mocked Lettuce command interfaces —
  claim/release/trace retries, subscribe/unsubscribe reference counting, resource cleanup on a
  failed connect. An opt-in, Testcontainers-based suite (`mvn verify -Pdocker-tests`, see
  [Building](#building)) proves the same claim/trace path against a real Redis 7.x server: the CAS
  Lua script under real concurrent contention (50 threads racing for 10 slots, same as the
  Hazelcast module's equivalent proof), exactly-once decisions under concurrent outcomes, the
  `NOSCRIPT`→`EVALSHA`→`eval` fallback, `PUBLISH`/`SUBSCRIBE`, and TTL-based self-expiry. CI runs
  this suite on every push (see the badge above), so it's not a one-time check. It's still pinned
  to one Redis version, not the full 5.1–8.x range Lettuce itself is tested against — test it
  against your own actual Redis version before relying on this in production.
- Neither Hazelcast nor Lettuce is managed by Spring Boot's BOM in the modules that don't depend on
  Spring at all; the build currently pins exact versions (`resilience4j.version`/`hazelcast.version`/
  `lettuce.version` in the root `pom.xml`).
- **Spring Boot 3.2–4.1**, for `quorum-breaker-spring` and `quorum-breaker-hazelcast-spring` only.
  `spring-boot-starter` is `provided` scope on both, so quorum-breaker doesn't pin a version — it
  builds against whatever Spring Boot BOM your own app already imports.

## Design notes

- **Fail-safe, but still bounded.** Every cluster call is wrapped: if Hazelcast is unreachable,
  quorum-breaker keeps admitting trial calls rather than blocking traffic, capped at this node's own
  share of `permittedNumberOfCallsInHalfOpenState` — never an ungoverned local budget.
- **Boot-time reachability is not covered by that promise.** If you wire a `HazelcastInstance`/`Config`
  bean through `quorum-breaker-hazelcast-spring`'s autoconfiguration, an unreachable cluster at
  startup fails Spring context refresh — by design, the same way an unreachable database fails a
  `DataSource` bean. Configure Hazelcast's own join/discovery timeouts in your `Config`/`ClientConfig`
  to control how long that takes, or supply an already-running `HazelcastInstance` if you want to
  manage that lifecycle yourself.
- **A breaker is never permanently left without cluster-wide OPEN awareness.** If subscribing to
  remote transitions fails when a breaker is first bound (e.g. Hazelcast is still unreachable at that
  moment), quorum-breaker doesn't give up — it keeps retrying that one breaker every 30 seconds,
  cheaply, in the background, until it succeeds. The breaker still participates fully in shared
  half-open trial coordination in the meantime; the only thing it's missing until the retry lands is
  the instant, cluster-wide propagation of a remote OPEN.
- **Admission checks are synchronous and cluster-bound, not instant.** Unlike a plain Resilience4j
  breaker, quorum-breaker doesn't reject `OPEN`-state calls locally the instant they arrive. Most of
  them still are - a local cooldown check denies calls immediately for as long as the wait duration
  hasn't elapsed - but once it has, the small number of calls competing to become a `HALF_OPEN` trial
  do wait on a round trip to the cluster (bounded by the coordinator's call timeout, e.g. one second
  for `quorum-breaker-hazelcast`'s default) before finding out if they were admitted or denied.
- **Exactly-once decisions.** The shared window's outcome is resolved via an atomic
  compare-and-swap retry loop on the shared outcome entry, so concurrent trial calls from different
  nodes can never double-decide the same window.
- **A denied trial still means recovery testing is under way.** Once the wait duration elapses,
  a node whose claim is denied because the cluster-wide `HALF_OPEN` budget is already spent still
  transitions its local breaker to `HALF_OPEN` before rejecting the call, so both the breaker's
  observable state and the `CallNotPermittedException` message correctly say `HALF_OPEN` - not
  `OPEN` - even on a node that never personally won a trial slot. A call rejected purely because the
  wait duration hasn't elapsed yet still reports `OPEN`, since no recovery testing is happening
  anywhere in the cluster yet.
- **Self-cleaning state.** Shared window entries carry a TTL, so a node dying mid-window doesn't
  leave cluster state stuck forever. This also bounds a known gap: a trial call decorated via
  `decorateFuture`/`executeFuture` only reports its outcome once the caller calls `get()` — a caller
  that cancels the `Future` and never calls `get()` afterward, or that never calls `get()` at all
  (e.g. only polls `isDone()`), leaves that trial's outcome unreported, with no way to detect that
  after the fact, since `java.util.concurrent.Future` has no completion callback to hook into. The
  window simply won't decide until its TTL expires, at which point recovery testing restarts with a
  fresh window. Tune `window-ttl-padding` if this matters for your traffic.

## Building

```bash
mvn clean install
```

Full suite includes unit tests, Mockito-based behavioural tests, and integration tests that spin
up a real 2-member embedded Hazelcast cluster — including one that proves the `HALF_OPEN` budget
is genuinely shared across two independent instances rather than duplicated per node.

The Redis modules can't do the same trick: there's no pure-Java embeddable Redis the way
`Hazelcast.newHazelcastInstance(...)` gives you a real embedded Hazelcast member for free, so a
live proof needs an actual Redis server. `mvn clean install` alone never asks for one — it's meant
to be usable anywhere Java runs, on a bare JDK, no daemon or emulator required, on any OS. The
live-Redis proof lives in a separate, opt-in suite instead:

```bash
mvn verify -Pdocker-tests
```

This spins up a real Redis container (via Testcontainers, so it needs a working Docker daemon) and
proves the shared-budget claim under real concurrent contention, exactly-once decisions, the CAS
Lua script, `PUBLISH`/`SUBSCRIBE`, and TTL self-expiry against it — the same things the Hazelcast
suite proves for free on every build. It only runs against `quorum-breaker-redis` and only under
this profile; every other module and the default `mvn clean install` are unaffected whether or not
Docker is available. CI runs this profile on every push and pull request, so it's not something a
contributor has to remember to run by hand for the guarantee to hold. See
[Requirements](#requirements) for exactly what this does and doesn't cover.

## License

Apache License 2.0 - see [LICENSE](LICENSE).
