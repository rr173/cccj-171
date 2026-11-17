"""Result model returned to scrapers."""

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import Optional, Tuple


class Source(enum.Enum):
    """Where the body of a :class:`FetchResult` came from."""

    #: Fetched from the network during this call (a 200 response).
    NETWORK = "network"
    #: A still-fresh local representation; no network request was made.
    CACHE = "cache"
    #: A conditional request was confirmed by a 304; the body is the
    #: previously stored representation (never an empty 304 body).
    REVALIDATED = "revalidated"
    #: An opt-in stale-if-error fallback: the request failed, and a stored
    #: representation was served although it is past its freshness lifetime.
    #: Never to be mistaken for a fresh hit or a successful fetch.
    STALE_IF_ERROR = "stale-if-error"


@dataclass(frozen=True)
class StaleFallback:
    """Why and how far a STALE_IF_ERROR body was allowed to be served.

    upstream_status  the HTTP failure status seen on the wire (one of
                     500/502/503/504), None for a transport failure.
    upstream_error   description of the transport failure (connect/timeout/
                     reset/truncated stream), None for an HTTP status failure.
    staleness        ``current_age - lifetime`` at the moment of the fallback.
    allowed_seconds  the stricter of the response's stale-if-error grant and
                     the caller's per-call limit; ``staleness <= allowed``.
    server_limit     the stale-if-error seconds declared by the response.
    caller_limit     the per-call maximum the caller supplied.
    """

    upstream_status: Optional[int]
    upstream_error: Optional[str]
    staleness: float
    allowed_seconds: float
    server_limit: float
    caller_limit: float


@dataclass(frozen=True)
class AgeInfo:
    """The inputs behind the freshness decision (all seconds, client clock).

    q, r            request-sent / response-received times of the exchange
                    that produced the stored metadata (None if this call made
                    no request and none was recorded).
    now             the client clock reading used for this result.
    date_epoch      parsed HTTP Date of the (possibly merged) response,
                    None when absent (r is then used in the formula).
    age_header      parsed Age of the (possibly merged) response, 0 if absent.
    initial_age     max(max(0, r - Date), Age + (r - q)) at receipt.
    current_age     initial_age + max(0, now - r).
    lifetime        freshness lifetime from Cache-Control (s-maxage wins over
                    max-age); None means "no explicit lifetime".
    must_validate   True when every use requires validation (no-cache or no
                    explicit lifetime).
    """

    q: Optional[float]
    r: Optional[float]
    now: float
    date_epoch: Optional[float]
    age_header: float
    initial_age: float
    current_age: float
    lifetime: Optional[float]
    must_validate: bool


@dataclass(frozen=True)
class FetchResult:
    """The outcome of one get()/refresh() call.

    status          effective HTTP status of the representation (200 for
                    cache hits and 304-validated bodies; the wire status for
                    unstored statuses such as 404/500).
    body            the complete response body, safe to hand to a parser;
                    a 304 confirmation always yields the stored body.
    headers         effective representation headers (after 304 merging),
                    hop-by-hop headers removed.
    source          NETWORK / CACHE / REVALIDATED / STALE_IF_ERROR.
    network_status  the status actually seen on the wire during this call,
                    or None when no request was made.
    validated       True when this call performed a conditional request whose
                    304 was applied to the stored representation.
    vary            the Vary field-name tuple of the selected representation.
    age             the freshness computation inputs.
    stale           True when the body is being served past its freshness
                    lifetime through the opt-in stale-if-error fallback.
    stale_fallback  the :class:`StaleFallback` details when ``stale`` is true,
                    otherwise None.  The upstream failure is never hidden:
                    ``network_status``/``upstream_status`` and the transport
                    error text are both preserved.
    """

    url: str
    status: int
    body: bytes
    headers: Tuple[Tuple[str, str], ...]
    source: Source
    network_status: Optional[int]
    validated: bool
    vary: Tuple[str, ...]
    age: AgeInfo
    stale: bool = False
    stale_fallback: Optional[StaleFallback] = None

    @property
    def from_cache(self) -> bool:
        """True when no network request was made for this result."""
        return self.source is Source.CACHE

    @property
    def from_stale(self) -> bool:
        """True when this result is a stale-if-error fallback body."""
        return self.source is Source.STALE_IF_ERROR

    @property
    def ok(self) -> bool:
        """True only for a genuine 200 representation served as a success.

        A stale-if-error fallback is a degraded result, not a successful
        fetch, so it is never ``ok`` even though the body is a 200 one.
        """

        return self.status == 200 and not self.stale

    @property
    def text(self) -> str:
        return self.body.decode("utf-8")
