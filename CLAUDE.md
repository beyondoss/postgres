## Documentation

**Keep docs in sync**: When changing code that affects documented behavior (data flows, state machines, APIs, config), update the ARCHITECTURE.md in the same commit. Stale docs are worse than no docs.

## Local Development

We use mise for running development tasks.

Search tasks:

```sh
mise tasks | grep "search"
```

## System Design

**IMPORTANT**: We seek the minimum effective abstraction. Elegant simplicity. Composable parts that "just work".

**Performance is a feature, not an optimization pass.**

- Do less work. The fastest code is code that doesn't run.
- Minimize allocations. Reuse where it matters.
- Parallelize only when the work itself is the bottleneck—not as a first instinct.
- Measure before you optimize, but design with performance in mind from the start.

## Operations & State

All operations that modify state—infrastructure (GlideFS, VXLAN, iptables, TAP devices) and application—**must be idempotent and atomic**.

**Idempotent**: Running an operation multiple times produces the same result as running it once.

- Check before create; don't error if it exists
- Check before destroy; don't error if it's gone
- Safe to retry after network failures or crashes

**Atomic (or safe)**: An operation either fully succeeds, fully fails, or leaves the system in a valid intermediate state that subsequent retries can recover from.

- Multi-step operations should use transactions or compensating actions
- If you can't make it atomic, make the intermediate states safe to observe

These properties are critical for crash recovery, distributed coordination, and reasoning about system behavior.

## Performance Improvement

Apply the **Theory of Constraints**: a system's throughput is limited by its single tightest bottleneck. Optimizing anything else is waste.

1. **Identify** the constraint. Profile. Trace. Measure. Don't guess — find the one thing that actually bounds throughput or latency right now.
2. **Exploit** the constraint. Squeeze maximum performance from the bottleneck with minimal change — better batching, fewer allocations, smarter scheduling. No redesigns yet.
3. **Subordinate** everything else. Non-bottleneck components should serve the constraint, not outrun it. Over-optimizing a fast path that feeds into a slow one is wasted effort.
4. **Elevate** the constraint. If exploiting isn't enough, invest in removing it — redesign, parallelize, change the algorithm, add capacity.
5. **Repeat.** The bottleneck has shifted. Go back to step 1.

The corollary: if you can't name the current constraint, you aren't ready to optimize.

<!-- wiki-managed:start (managed by `wiki claude install`; edits inside this block will be overwritten) -->

## Wiki

This repo uses [agent-wiki](.wiki/): `.wiki/` indexes repo markdown docs and code symbols into a queryable knowledge graph.

**Read the wiki before grepping the codebase or reading ARCHITECTURE.md.** Pages are pre-indexed — searching them is faster and ~5–10× cheaper than re-deriving from raw files.

Wiki tools — pick based on what you need:

- `wiki_query "<term>"` — first move for any specific question. BM25++ over repo docs and code symbols; returns ranked hits with paths, scores, and inline snippets.
- `wiki_answer "<question>"` — returns top-ranked pages with query-relevant passage extracts in one round-trip. Best when you expect the answer exists and want it immediately.
- `wiki_read "path/to/page.md"` (optionally `section: "..."` or `paths: [...]`) — full page, one section, or multiple pages in one call.
- `wiki_search_code "<query>"` — search exported symbols, signatures, and doc comments when you need to locate a declaration or understand an API.
- `wiki_usage_examples "<symbol>"` — real call sites with surrounding source code. Use before changing a function (to see every calling convention you must preserve) or when learning how an unfamiliar API is actually used.
- `wiki_impact "<symbol>"` — blast radius: every symbol that transitively calls this one, ranked by hop distance. Use before refactoring or renaming to know what breaks.
- `wiki_callees "<symbol>"` — outgoing call hierarchy (rust-analyzer equivalent): every function this symbol transitively calls, ranked by hop distance. Use when you need to understand what a function depends on before touching it — its DB calls, service calls, and abstractions.
- `wiki_implementors "<symbol>"` — go-to-implementations (rust-analyzer equivalent): every concrete type that implements a trait or interface. Use when you need to know what's behind a trait object, or how many types a trait change will affect.

<!-- wiki-managed:end -->
