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
    source          NETWORK / CACHE / REVALIDATED.
    network_status  the status actually seen on the wire during this call,
                    or None when no request was made.
    validated       True when this call performed a conditional request whose
                    304 was applied to the stored representation.
    vary            the Vary field-name tuple of the selected representation.
    age             the freshness computation inputs.
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

    @property
    def from_cache(self) -> bool:
        """True when no network request was made for this result."""
        return self.source is Source.CACHE

    @property
    def ok(self) -> bool:
        return self.status == 200

    @property
    def text(self) -> str:
        return self.body.decode("utf-8")
