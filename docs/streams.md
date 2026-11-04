# Streams design status

As of 4 November 2026, the Streams implementation, its package export, runtime tests, runnable example, and build integration have been removed. Only Actor Core is implemented and exported.

The public API will be reviewed one decision at a time. Cold observables must be supported alongside shared hot execution; this supersedes the previous hot-only assumption. For a source like `of(1, 2, 3)`, each subscription should start its own execution, receiving `1`, `2`, `3` and a terminal event. This does not require synchronous delivery in Solid.

This matches the per-subscription iteration in [RxJS 7.8.2's source](https://github.com/ReactiveX/rxjs/blob/7.8.2/src/internal/observable/innerFrom.ts#L61-L77). It is a fresh execution, not replay from a shared completed producer.

The earlier [API draft](../design/streams/README.md), declarations, examples, and [backend contract](../design/streams/backend-contract.md) remain historical design material. They are not an adopted specification, and their hot-only semantics must be reconsidered. No replacement Streams API or choice of default execution mode has been implemented.
