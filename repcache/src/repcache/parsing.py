"""Parsing of the supported HTTP response-metadata subset.

Supported: Vary (field list or *), Cache-Control (s-maxage, max-age,
no-cache, no-store, must-revalidate, stale-if-error), a single valid ETag
(weak tags included, used verbatim), Date and Age.  Anything malformed or
conflicting raises ProtocolError; the caller keeps its previous cache and the
failed exchange is never stored.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Optional, Sequence, Tuple

from .errors import ProtocolError

_TOKEN_RE = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")
# entity-tag = [ weak ] opaque-tag ; opaque-tag = DQUOTE *etagc DQUOTE
# etagc = %x21 / %x23-7E / obs-text
_ETAG_RE = re.compile(r'^(?:W/)?"[\x21\x23-\x7e\x80-\xff]*"$')

HOP_BY_HOP = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
)


def _values(headers: Sequence[Tuple[str, str]], name: str) -> list:
    return [v for n, v in headers if n.lower() == name]


def parse_vary(headers: Sequence[Tuple[str, str]]) -> Tuple[Tuple[str, ...], bool]:
    """Return (field-name tuple, star).  Names are lowercased and deduped."""

    fields: list = []
    star = False
    for value in _values(headers, "vary"):
        for part in value.split(","):
            token = part.strip().lower()
            if not token:
                continue
            if token == "*":
                star = True
                continue
            if not _TOKEN_RE.match(token):
                raise ProtocolError(f"invalid Vary field name {token!r}")
            if token not in fields:
                fields.append(token)
    return tuple(fields), star


@dataclass(frozen=True)
class CacheControl:
    present: bool
    no_store: bool
    no_cache: bool
    lifetime: Optional[float]  # s-maxage wins over max-age; None = no lifetime
    must_revalidate: bool = False
    stale_if_error: Optional[float] = None  # None = no stale-if-error grant

    @property
    def must_validate(self) -> bool:
        return self.no_cache or self.lifetime is None


def parse_cache_control(headers: Sequence[Tuple[str, str]]) -> CacheControl:
    values = _values(headers, "cache-control")
    if not values:
        return CacheControl(False, False, False, None)
    s_maxage: Optional[int] = None
    max_age: Optional[int] = None
    stale_if_error: Optional[int] = None
    no_store = False
    no_cache = False
    must_revalidate = False
    for value in values:
        for part in value.split(","):
            part = part.strip()
            if not part:
                continue
            name, eq, raw = part.partition("=")
            name = name.strip().lower()
            if not eq:
                if name == "no-store":
                    no_store = True
                elif name == "no-cache":
                    no_cache = True
                elif name == "must-revalidate":
                    must_revalidate = True
                # other flag directives (public, private, ...) ignored
                continue
            raw = raw.strip()
            if name in ("s-maxage", "max-age", "stale-if-error"):
                if not raw.isdigit():
                    raise ProtocolError(f"invalid {name} value {raw!r}")
                number = int(raw)
                if name == "s-maxage":
                    if s_maxage is not None:
                        raise ProtocolError("conflicting s-maxage directives")
                    s_maxage = number
                elif name == "max-age":
                    if max_age is not None:
                        raise ProtocolError("conflicting max-age directives")
                    max_age = number
                else:
                    if stale_if_error is not None:
                        raise ProtocolError(
                            "conflicting stale-if-error directives"
                        )
                    stale_if_error = number
            # unknown directives with values are ignored
    lifetime = s_maxage if s_maxage is not None else max_age
    return CacheControl(
        True,
        no_store,
        no_cache,
        float(lifetime) if lifetime is not None else None,
        must_revalidate,
        float(stale_if_error) if stale_if_error is not None else None,
    )


def parse_etag(headers: Sequence[Tuple[str, str]]) -> Optional[str]:
    values = _values(headers, "etag")
    if not values:
        return None
    if len(values) > 1:
        raise ProtocolError("multiple ETag headers")
    value = values[0].strip()
    if not _ETAG_RE.match(value):
        raise ProtocolError(f"invalid ETag {value!r}")
    return value


def parse_date(headers: Sequence[Tuple[str, str]]) -> Optional[float]:
    values = _values(headers, "date")
    if not values:
        return None
    if len(values) > 1:
        raise ProtocolError("multiple Date headers")
    try:
        from datetime import timezone

        dt = parsedate_to_datetime(values[0])
        if dt is None:
            raise ValueError("unparseable")
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except (TypeError, ValueError, OverflowError) as exc:
        raise ProtocolError(f"invalid Date header {values[0]!r}") from exc


def parse_age(headers: Sequence[Tuple[str, str]]) -> float:
    values = _values(headers, "age")
    if not values:
        return 0.0
    if len(values) > 1:
        raise ProtocolError("multiple Age headers")
    raw = values[0].strip()
    if not raw.isdigit():
        raise ProtocolError(f"invalid Age header {raw!r}")
    return float(int(raw))


def compute_initial_age(
    date_epoch: Optional[float], age_header: float, q: float, r: float
) -> float:
    """initial_age = max(max(0, r - Date), Age + (r - q)); Date missing -> r."""

    apparent = max(0.0, r - (date_epoch if date_epoch is not None else r))
    return max(apparent, age_header + (r - q))


def compute_current_age(initial_age: float, r: float, now: float) -> float:
    return initial_age + max(0.0, now - r)


def filter_representation_headers(
    headers: Sequence[Tuple[str, str]],
) -> Tuple[Tuple[str, str], ...]:
    return tuple((n, v) for n, v in headers if n.lower() not in HOP_BY_HOP)


def merge_headers(
    old: Sequence[Tuple[str, str]], new: Sequence[Tuple[str, str]]
) -> Tuple[Tuple[str, str], ...]:
    """Representation metadata merge: fields present in ``new`` replace the
    stored ones; fields absent from ``new`` keep their stored values."""

    new_names = {n.lower() for n, _ in new}
    kept = [(n, v) for n, v in old if n.lower() not in new_names]
    return tuple(kept) + tuple(new)


@dataclass(frozen=True)
class ResponseMeta:
    """Parsed metadata of a 200 or 304 response."""

    vary_fields: Tuple[str, ...]
    vary_star: bool
    has_vary: bool
    cache_control: CacheControl
    etag: Optional[str]
    date_epoch: Optional[float]
    age: float
    has_age: bool


def parse_response_meta(headers: Sequence[Tuple[str, str]]) -> ResponseMeta:
    vary_fields, vary_star = parse_vary(headers)
    return ResponseMeta(
        vary_fields=vary_fields,
        vary_star=vary_star,
        has_vary=bool(_values(headers, "vary")),
        cache_control=parse_cache_control(headers),
        etag=parse_etag(headers),
        date_epoch=parse_date(headers),
        age=parse_age(headers),
        has_age=bool(_values(headers, "age")),
    )
