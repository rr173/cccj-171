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
* stale-if-error is strictly per call.  An eligible failure (transport
  failure or HTTP 500/502/503/504) is published to waiters as a *failure*,
  never as a body.  Each caller then re-reads the current cache state and
  decides on its own against its own limit, so waiters with different limits
  can legitimately disagree.  A body replaced by refresh in the meantime is
  never resurrected.
"""

from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass, replace
from typing import Callable, Optional, Tuple

from .errors import (
    CacheClosed,
    NetworkError,
    ProtocolError,
    RepCacheError,
    RequestRejected,
)
from .http1 import WireResponse, wire_request
from .model import AgeInfo, FetchResult, Source, StaleFallback
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

#: HTTP statuses for which an opt-in stale body may replace the failure.
_STALE_FALLBACK_STATUSES = frozenset({500, 502, 503, 504})


class _Retry(Exception):
    """Internal: the in-flight validation became obsolete; re-select."""


_RETRY = _Retry()


@dataclass(frozen=True)
class _Failure:
    """An upstream failure shared with waiters; carries no selected body."""

    error: Optional[BaseException] = None  # transport failure, if any
    wire: Optional[WireResponse] = None  # HTTP failure response, if any

    @property
    def status(self) -> Optional[int]:
        return self.wire.status if self.wire is not None else None

    def describe(self) -> Optional[str]:
        return str(self.error) if self.error is not None else None


class _InFlight:
    __slots__ = ("_event", "outcome")

    def __init__(self) -> None:
        self._event = threading.Event()
        #: None = success (waiters re-select and fetch for themselves if
        #: nothing was stored); _Failure = eligible upstream failure; or an
        #: exception that every waiter must re-raise.
        self.outcome: object = None

    def finish(self, outcome: object) -> None:
        self.outcome = outcome
        self._event.set()

    def wait(self) -> object:
        self._event.wait()
        return self.outcome


@dataclass
class _Plan:
    kind: str  # "result" | "wait" | "lead"
    result: Optional[FetchResult] = None
    handle: Optional[_InFlight] = None
    key: Optional[tuple] = None
    if_none_match: Optional[str] = None
    snap_epoch: Optional[int] = None
    snap_vary_values: Optional[str] = None


def _check_stale_if_error(value: object) -> Optional[float]:
    """Validate the per-call stale-if-error limit (None = disabled)."""

    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RequestRejected("stale_if_error must be a number of seconds or None")
    seconds = float(value)
    if not math.isfinite(seconds) or seconds < 0:
        raise RequestRejected(
            "stale_if_error must be a finite, non-negative number of seconds"
        )
    return seconds


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

    def get(self, url: str, headers=None, *, stale_if_error=None) -> FetchResult:
        """Fetch a representation, reusing the cache whenever allowed.

        ``stale_if_error`` (seconds, default None = disabled) opts this single
        call into serving an expired stored representation when the origin
        fails with a transport error or 500/502/503/504.  Both the response's
        own ``stale-if-error`` grant and this limit must permit the body, and
        it must still match the request's Vary values.  The returned result is
        marked ``source=STALE_IF_ERROR``/``stale=True`` and carries the
        upstream failure; it is never reported as fresh or as a successful
        fetch.  ``refresh`` never falls back.
        """

        req = prepare_request(url, headers)
        limit = _check_stale_if_error(stale_if_error)
        retries = 0
        while True:
            plan = self._plan(req)
            if plan.kind == "result":
                return plan.result
            if plan.kind == "wait":
                outcome = plan.handle.wait()
                if isinstance(outcome, _Failure):
                    stale = self._stale_fallback(req, outcome, limit)
                    if stale is not None:
                        return stale
                    if outcome.wire is not None:
                        return self._result_from_wire(req, outcome.wire)
                    raise outcome.error
                if isinstance(outcome, BaseException):
                    raise outcome
                continue
            try:
                return self._lead(req, plan, limit)
            except _Retry:
                retries += 1
                if retries > _MAX_REVALIDATION_RETRIES:
                    raise RepCacheError("revalidation did not converge")
                continue

    def refresh(self, url: str, headers=None) -> FetchResult:
        """Administrator fetch: unconditional GET, never joins in-flight
        normal fetches and runs in parallel with them.

        Failures always surface; refresh never serves a stale body.
        """

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

    # ------------------------------------------------------ stale-if-error

    def _stale_fallback(
        self, req: PreparedRequest, failure: _Failure, limit: Optional[float]
    ) -> Optional[FetchResult]:
        """Serve an expired representation for an eligible failure, per call.

        The decision is made *after* the failure, against the current cache
        state, so a waiter never inherits the leader's chosen body.  The body
        must still match this request's Vary values under the *current* rule
        (a replaced representation or changed rule simply yields nothing), and
        both the stored response's ``stale-if-error`` grant and the caller's
        limit must admit the staleness.  Serving is read-only: nothing about
        the stored representation (storage time, lifetime, metadata) changes.

        A representation that became fresh again (e.g. refresh replaced it
        while the request was in flight) is re-selected for *every* caller as
        a plain cache hit; the replaced older body is never resurrected.
        """

        now = self._clock()
        with self._lock:
            self._ensure_open()
            rule = self._store.get_rule(req.identity)
            if rule is None:
                return None
            values = _select_values(rule.fields, req.headers)
            values_json = dumps_values(values)
            rep = self._store.get_rep(req.identity, rule.epoch, values_json)
            if rep is None:
                return None
            if now < rep.r:
                # Clock moved backwards: staleness would be meaningless.
                return None
            current_age = compute_current_age(rep.initial_age, rep.r, now)
            if (
                rep.lifetime is not None
                and not rep.must_validate
                and current_age < rep.lifetime
            ):
                # A newer representation replaced the failed one while the
                # request was on the wire.  Re-select it as a plain fresh hit
                # instead of resurrecting anything older or failing.
                return self._result_from_rep(
                    rep, now, Source.CACHE, validated=False, network_status=None
                )
            if limit is None:
                return None
            if rep.lifetime is None or not rep.allows_stale_if_error:
                return None
            if rep.stale_if_error is None:
                return None
            staleness = current_age - rep.lifetime
            allowed = min(rep.stale_if_error, limit)
            if staleness > allowed:
                return None
            result = self._result_from_rep(
                rep,
                now,
                Source.STALE_IF_ERROR,
                validated=False,
                network_status=failure.status,
            )
        return replace(
            result,
            stale=True,
            stale_fallback=StaleFallback(
                upstream_status=failure.status,
                upstream_error=failure.describe(),
                staleness=staleness,
                allowed_seconds=allowed,
                server_limit=rep.stale_if_error,
                caller_limit=limit,
            ),
        )

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

    def _lead(
        self, req: PreparedRequest, plan: _Plan, limit: Optional[float]
    ) -> FetchResult:
        failure = None
        try:
            wire = self._transport(req, plan.if_none_match)
            outcome = self._process_wire(req, plan, wire)
            if not isinstance(outcome, _Failure):
                self._complete(plan, None)
                return outcome
            failure = outcome
        except _Retry:
            self._complete(plan, None)
            raise
        except RequestRejected:
            # The caller's own request is invalid; waiters are unaffected.
            self._complete(plan, None)
            raise
        except BaseException as exc:
            error = exc if isinstance(exc, RepCacheError) else NetworkError(str(exc))
            if error.stale_if_error_eligible:
                # Publish the failure, not a body: each waiter decides for
                # itself against its own limit and the current cache state.
                failure = _Failure(error=error)
            else:
                self._complete(plan, error)
                raise
        # Eligible upstream failure: this caller, like every waiter, decides
        # independently from the current cache state.
        self._complete(plan, failure)
        stale = self._stale_fallback(req, failure, limit)
        if stale is not None:
            return stale
        if failure.wire is not None:
            return self._result_from_wire(req, failure.wire)
        raise failure.error

    def _complete(self, plan: _Plan, outcome: object) -> None:
        with self._lock:
            self._inflight.pop(plan.key, None)
            plan.handle.finish(outcome)

    def _process_wire(
        self, req: PreparedRequest, plan: _Plan, wire: WireResponse
    ) -> object:
        if wire.status == 200:
            return self._process_200(req, plan, wire)
        if wire.status == 304:
            return self._process_304(req, plan, wire)
        if wire.status in _STALE_FALLBACK_STATUSES:
            # Handed to the caller verbatim (or replaced per-call by a stale
            # body); never stored.
            return _Failure(wire=wire)
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
                must_revalidate=meta.cache_control.must_revalidate,
                stale_if_error=meta.cache_control.stale_if_error,
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
        must_revalidate = (
            cc.must_revalidate if cc.present else rep.must_revalidate
        )
        # A present Cache-Control replaces the whole directive set, so an
        # omitted stale-if-error on a 304 revokes an earlier grant.
        stale_if_error = cc.stale_if_error if cc.present else rep.stale_if_error
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
            must_revalidate=must_revalidate,
            stale_if_error=stale_if_error,
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
