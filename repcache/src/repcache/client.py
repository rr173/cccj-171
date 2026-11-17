"""The thread-safe HTTP representation reuse client.

Coordination model
------------------
* A single lock guards the SQLite store and the in-flight table; network I/O
  always happens outside the lock, so independent representations of one URL
  (or of different URLs) are fetched in parallel.
* In-flight keys:
    - Vary rule unknown  -> ("cold", url, exact canonical request headers):
      only byte-identical request header sets share one GET.
    - Vary rule known    -> ("rep", url, epoch, vary-value tuple, mode):
      callers the rule proves equivalent share one fetch/revalidation;
      irrelevant header differences do not prevent sharing.
* A waiter never receives the leader's body directly.  When the leader
  finishes, waiters wake and re-run selection: they use the freshly stored
  representation only if the final Vary rule still selects them; on
  no-store / Vary:* / late-arriving responses nothing was stored, so they
  fetch independently.  A failed leader wakes everyone with the same error.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Callable, Optional, Tuple

from .errors import (
    CacheClosed,
    NetworkError,
    ProtocolError,
    RepCacheError,
)
from .http1 import WireResponse, wire_request
from .model import AgeInfo, FetchResult, Source
from .parsing import (
    ResponseMeta,
    compute_current_age,
    compute_initial_age,
    filter_representation_headers,
    merge_headers,
    parse_response_meta,
)
from .store import Rep, Store, dumps_values
from .urlutil import PreparedRequest, prepare_request

_MAX_REVALIDATION_RETRIES = 8


class _Retry(Exception):
    """Internal: the in-flight validation became obsolete; re-select."""


_RETRY = _Retry()


class _InFlight:
    __slots__ = ("_event", "error")

    def __init__(self) -> None:
        self._event = threading.Event()
        self.error: Optional[BaseException] = None

    def finish(self, error: Optional[BaseException]) -> None:
        self.error = error
        self._event.set()

    def wait(self) -> Optional[BaseException]:
        self._event.wait()
        return self.error


@dataclass
class _Plan:
    kind: str  # "result" | "wait" | "lead"
    result: Optional[FetchResult] = None
    handle: Optional[_InFlight] = None
    key: Optional[tuple] = None
    if_none_match: Optional[str] = None
    snap_epoch: Optional[int] = None
    snap_vary_values: Optional[str] = None


def _select_values(
    fields: Tuple[str, ...], headers: Tuple[Tuple[str, str], ...]
) -> Tuple[Tuple[str, Optional[str]], ...]:
    """The caller's exact values for the vary fields; missing != empty."""

    by_name = dict(headers)
    return tuple((field, by_name.get(field)) for field in fields)


class RepCacheClient:
    """HTTP representation reuse client backed by SQLite.

    Parameters
    ----------
    db_path:   cache database file; reusable across client restarts.
    max_bytes: hard response-size limit; larger responses fail and are
               never cached.
    timeout:   connect/read timeout per network exchange.
    clock:     seconds clock, injectable for tests; defaults to time.time.
    """

    def __init__(
        self,
        db_path: str,
        *,
        max_bytes: int = 1 << 20,
        timeout: float = 10.0,
        clock: Optional[Callable[[], float]] = None,
    ) -> None:
        self._clock = clock or time.time
        self._max_bytes = max_bytes
        self._timeout = timeout
        self._store = Store(db_path)
        self._lock = threading.RLock()
        self._inflight: dict = {}
        self._closed = False

    # ------------------------------------------------------------------ API

    def get(self, url: str, headers=None) -> FetchResult:
        """Fetch a representation, reusing the cache whenever allowed."""

        req = prepare_request(url, headers)
        retries = 0
        while True:
            plan = self._plan(req)
            if plan.kind == "result":
                return plan.result
            if plan.kind == "wait":
                error = plan.handle.wait()
                if error is not None:
                    raise error
                continue
            try:
                return self._lead(req, plan)
            except _Retry:
                retries += 1
                if retries > _MAX_REVALIDATION_RETRIES:
                    raise RepCacheError("revalidation did not converge")
                continue

    def refresh(self, url: str, headers=None) -> FetchResult:
        """Administrator fetch: unconditional GET, never joins in-flight
        normal fetches and runs in parallel with them."""

        req = prepare_request(url, headers)
        with self._lock:
            self._ensure_open()
            rule = self._store.get_rule(req.identity)
            snap_epoch = rule.epoch if rule is not None else None
        wire = self._transport(req, if_none_match=None)
        if wire.status == 304:
            raise ProtocolError("304 received for an unconditional request")
        if wire.status != 200:
            return self._result_from_wire(req, wire)
        meta = parse_response_meta(wire.headers)
        initial_age = compute_initial_age(meta.date_epoch, meta.age, wire.q, wire.r)
        with self._lock:
            self._ensure_open()
            if not meta.cache_control.no_store and not meta.vary_star:
                self._store_200_locked(req, snap_epoch, meta, wire, initial_age)
        return self._result_from_200(req, wire, meta, initial_age)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._store.close()

    def __enter__(self) -> "RepCacheClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ------------------------------------------------------------- planning

    def _ensure_open(self) -> None:
        if self._closed:
            raise CacheClosed("client is closed")

    def _plan(self, req: PreparedRequest) -> _Plan:
        now = self._clock()
        with self._lock:
            self._ensure_open()
            rule = self._store.get_rule(req.identity)
            if rule is not None:
                values = _select_values(rule.fields, req.headers)
                values_json = dumps_values(values)
                rep = self._store.get_rep(req.identity, rule.epoch, values_json)
                if rep is not None and self._fresh(rep, now):
                    return _Plan(
                        kind="result",
                        result=self._result_from_rep(
                            rep, now, Source.CACHE, validated=False, network_status=None
                        ),
                    )
                if rep is not None and rep.etag is not None:
                    mode = ("cond", rep.etag)
                    if_none_match = rep.etag
                    snap_vary_values = values_json
                else:
                    mode = ("uncond",)
                    if_none_match = None
                    snap_vary_values = values_json if rep is not None else None
                key = ("rep", req.identity, rule.epoch, values_json, mode)
                snap_epoch = rule.epoch
            else:
                key = ("cold", req.identity, req.headers)
                snap_epoch = None
                snap_vary_values = None
                if_none_match = None
            handle = self._inflight.get(key)
            if handle is None:
                handle = _InFlight()
                self._inflight[key] = handle
                return _Plan(
                    kind="lead",
                    handle=handle,
                    key=key,
                    if_none_match=if_none_match,
                    snap_epoch=snap_epoch,
                    snap_vary_values=snap_vary_values,
                )
            return _Plan(kind="wait", handle=handle)

    @staticmethod
    def _fresh(rep: Rep, now: float) -> bool:
        if rep.must_validate or rep.lifetime is None:
            return False
        if now < rep.r:
            # Clock moved backwards (e.g. a reopened database): be
            # conservative and require validation.
            return False
        return compute_current_age(rep.initial_age, rep.r, now) < rep.lifetime

    # -------------------------------------------------------------- leading

    def _transport(self, req: PreparedRequest, if_none_match: Optional[str]) -> WireResponse:
        return wire_request(
            req.scheme,
            req.host,
            req.port,
            req.path_query,
            req.headers,
            timeout=self._timeout,
            max_bytes=self._max_bytes,
            clock=self._clock,
            if_none_match=if_none_match,
        )

    def _lead(self, req: PreparedRequest, plan: _Plan) -> FetchResult:
        try:
            wire = self._transport(req, plan.if_none_match)
            outcome = self._process_wire(req, plan, wire)
        except _Retry:
            self._complete(plan, None)
            raise
        except BaseException as exc:
            error = exc if isinstance(exc, RepCacheError) else NetworkError(str(exc))
            self._complete(plan, error)
            raise
        self._complete(plan, None)
        return outcome

    def _complete(self, plan: _Plan, error: Optional[BaseException]) -> None:
        with self._lock:
            self._inflight.pop(plan.key, None)
            plan.handle.finish(error)

    def _process_wire(self, req: PreparedRequest, plan: _Plan, wire: WireResponse) -> FetchResult:
        if wire.status == 200:
            return self._process_200(req, plan, wire)
        if wire.status == 304:
            return self._process_304(req, plan, wire)
        # Any other status is handed to the caller verbatim and never stored.
        return self._result_from_wire(req, wire)

    def _process_200(self, req: PreparedRequest, plan: _Plan, wire: WireResponse) -> FetchResult:
        meta = parse_response_meta(wire.headers)
        initial_age = compute_initial_age(meta.date_epoch, meta.age, wire.q, wire.r)
        with self._lock:
            self._ensure_open()
            if not meta.cache_control.no_store and not meta.vary_star:
                self._store_200_locked(req, plan.snap_epoch, meta, wire, initial_age)
        return self._result_from_200(req, wire, meta, initial_age)

    def _store_200_locked(
        self,
        req: PreparedRequest,
        snap_epoch: Optional[int],
        meta: ResponseMeta,
        wire: WireResponse,
        initial_age: float,
    ) -> bool:
        """Commit a 200 under the vary rules; False when dropped as late."""

        cur = self._store.get_rule(req.identity)
        if cur is None:
            epoch = 1
            self._store.put_rule(req.identity, meta.vary_fields, epoch)
        elif cur.fields == meta.vary_fields:
            epoch = cur.epoch
        else:
            if snap_epoch is None or snap_epoch < cur.epoch:
                # A concurrent response already replaced the rule set after
                # this request started; a late arrival must not resurrect
                # the retired rules or representations.
                return False
            epoch = cur.epoch + 1
            self._store.put_rule(req.identity, meta.vary_fields, epoch)
            self._store.delete_reps_before(req.identity, epoch)
        values = _select_values(meta.vary_fields, req.headers)
        self._store.put_rep(
            Rep(
                url=req.identity,
                epoch=epoch,
                vary_values=values,
                status=200,
                headers=filter_representation_headers(wire.headers),
                body=wire.body,
                etag=meta.etag,
                lifetime=meta.cache_control.lifetime,
                must_validate=meta.cache_control.must_validate,
                date_epoch=meta.date_epoch,
                age_header=meta.age,
                q=wire.q,
                r=wire.r,
                initial_age=initial_age,
            )
        )
        return True

    def _process_304(self, req: PreparedRequest, plan: _Plan, wire: WireResponse) -> FetchResult:
        meta = parse_response_meta(wire.headers)
        now = self._clock()
        with self._lock:
            self._ensure_open()
            if plan.if_none_match is None:
                raise ProtocolError("304 received but no If-None-Match was sent")
            if meta.etag is not None and meta.etag != plan.if_none_match:
                raise ProtocolError(
                    f"304 ETag {meta.etag!r} does not match the sent "
                    f"If-None-Match {plan.if_none_match!r}"
                )
            rule = self._store.get_rule(req.identity)
            if rule is None or plan.snap_epoch != rule.epoch:
                raise _Retry  # vary rules changed while validating; re-select
            if meta.vary_star or (
                meta.has_vary and set(meta.vary_fields) != set(rule.fields)
            ):
                raise ProtocolError("304 carries a different Vary field set")
            rep = self._store.get_rep(req.identity, rule.epoch, plan.snap_vary_values)
            if rep is None or rep.etag != plan.if_none_match:
                # The representation was replaced meanwhile (e.g. refresh
                # published a new body): this validation is obsolete and must
                # not overwrite the newer body, headers or rules.
                raise _Retry
            merged = self._merge_304(rep, meta, wire)
            if meta.cache_control.no_store:
                self._store.delete_rep(req.identity, rule.epoch, plan.snap_vary_values)
            else:
                self._store.put_rep(merged)
            return self._result_from_rep(
                merged, now, Source.REVALIDATED, validated=True, network_status=304
            )

    @staticmethod
    def _merge_304(rep: Rep, meta: ResponseMeta, wire: WireResponse) -> Rep:
        """Merge 304 metadata over the stored representation.

        Fields the 304 provides override; missing fields keep stored values.
        The age is recomputed from this exchange (new q/r), never from the
        old storage time.
        """

        cc = meta.cache_control
        lifetime = cc.lifetime if cc.present else rep.lifetime
        must_validate = cc.must_validate if cc.present else rep.must_validate
        date_epoch = meta.date_epoch if meta.date_epoch is not None else rep.date_epoch
        age_header = meta.age if meta.has_age else rep.age_header
        initial_age = compute_initial_age(date_epoch, age_header, wire.q, wire.r)
        return Rep(
            url=rep.url,
            epoch=rep.epoch,
            vary_values=rep.vary_values,
            status=rep.status,
            headers=merge_headers(
                rep.headers, filter_representation_headers(wire.headers)
            ),
            body=rep.body,
            etag=meta.etag if meta.etag is not None else rep.etag,
            lifetime=lifetime,
            must_validate=must_validate,
            date_epoch=date_epoch,
            age_header=age_header,
            q=wire.q,
            r=wire.r,
            initial_age=initial_age,
        )

    # -------------------------------------------------------------- results

    def _result_from_rep(
        self,
        rep: Rep,
        now: float,
        source: Source,
        validated: bool,
        network_status: Optional[int],
    ) -> FetchResult:
        return FetchResult(
            url=rep.url,
            status=rep.status,
            body=rep.body,
            headers=rep.headers,
            source=source,
            network_status=network_status,
            validated=validated,
            vary=rep.vary_fields,
            age=AgeInfo(
                q=rep.q,
                r=rep.r,
                now=now,
                date_epoch=rep.date_epoch,
                age_header=rep.age_header,
                initial_age=rep.initial_age,
                current_age=compute_current_age(rep.initial_age, rep.r, now),
                lifetime=rep.lifetime,
                must_validate=rep.must_validate,
            ),
        )

    def _result_from_200(
        self, req: PreparedRequest, wire: WireResponse, meta: ResponseMeta, initial_age: float
    ) -> FetchResult:
        now = self._clock()
        return FetchResult(
            url=req.identity,
            status=200,
            body=wire.body,
            headers=filter_representation_headers(wire.headers),
            source=Source.NETWORK,
            network_status=200,
            validated=False,
            vary=meta.vary_fields,
            age=AgeInfo(
                q=wire.q,
                r=wire.r,
                now=now,
                date_epoch=meta.date_epoch,
                age_header=meta.age,
                initial_age=initial_age,
                current_age=compute_current_age(initial_age, wire.r, now),
                lifetime=meta.cache_control.lifetime,
                must_validate=meta.cache_control.must_validate,
            ),
        )

    def _result_from_wire(self, req: PreparedRequest, wire: WireResponse) -> FetchResult:
        now = self._clock()
        return FetchResult(
            url=req.identity,
            status=wire.status,
            body=wire.body,
            headers=filter_representation_headers(wire.headers),
            source=Source.NETWORK,
            network_status=wire.status,
            validated=False,
            vary=(),
            age=AgeInfo(
                q=wire.q,
                r=wire.r,
                now=now,
                date_epoch=None,
                age_header=0.0,
                initial_age=0.0,
                current_age=0.0,
                lifetime=None,
                must_validate=True,
            ),
        )
