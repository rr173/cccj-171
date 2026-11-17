"""URL normalization and caller-request validation.

URL identity rules: scheme and host are lowercased, default ports (80/443)
are dropped, the fragment is removed, and path/query participate verbatim
(query parameters are never rewritten, reordered or decoded).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Mapping, Optional, Tuple, Union
from urllib.parse import urlsplit

from .errors import RequestRejected

_DEFAULT_PORTS = {"http": 80, "https": 443}

#: Headers a caller may not supply: credentials, conditional evaluation,
#: partial content, and transport-level fields owned by this client.
FORBIDDEN_HEADERS = frozenset(
    {
        "authorization",
        "proxy-authorization",
        "cookie",
        "cookie2",
        "range",
        "if-none-match",
        "if-modified-since",
        "if-match",
        "if-unmodified-since",
        "if-range",
        "host",
        "content-length",
        "connection",
        "keep-alive",
        "transfer-encoding",
        "te",
        "trailer",
        "upgrade",
        "expect",
    }
)

_TOKEN_RE = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")


@dataclass(frozen=True)
class PreparedRequest:
    """A validated, normalized caller request."""

    original_url: str
    identity: str  # normalized URL used as cache identity
    scheme: str
    host: str
    port: int
    path_query: str  # origin-form request target
    headers: Tuple[Tuple[str, str], ...]  # canonical: lowercase names, sorted,
    # deduplicated, Accept-Encoding fixed to identity


def normalize_url(url: str) -> Tuple[str, str, int, str, str]:
    """Return (identity, scheme, host, port, path_query) or raise."""

    if not isinstance(url, str) or not url:
        raise RequestRejected("URL must be a non-empty string")
    try:
        parts = urlsplit(url)
    except ValueError as exc:
        raise RequestRejected(f"unparseable URL {url!r}: {exc}") from exc
    scheme = parts.scheme.lower()
    if scheme not in _DEFAULT_PORTS:
        raise RequestRejected(f"unsupported URL scheme in {url!r} (http/https only)")
    if parts.username is not None or parts.password is not None:
        raise RequestRejected("URLs with user-info are rejected")
    host = parts.hostname
    if not host:
        raise RequestRejected(f"URL {url!r} has no host")
    try:
        port = parts.port
    except ValueError as exc:
        raise RequestRejected(f"bad port in {url!r}: {exc}") from exc
    port = port if port is not None else _DEFAULT_PORTS[scheme]
    path = parts.path or "/"
    path_query = path + ("?" + parts.query if parts.query else "")
    hostport = f"[{host}]" if ":" in host else host
    if port != _DEFAULT_PORTS[scheme]:
        hostport = f"{hostport}:{port}"
    identity = f"{scheme}://{hostport}{path_query}"
    return identity, scheme, host, port, path_query


def validate_headers(
    headers: Union[Mapping[str, str], Tuple[Tuple[str, str], ...], list, None],
) -> Tuple[Tuple[str, str], ...]:
    """Validate caller headers and return the canonical effective set.

    Field names are case-insensitive and must not repeat; values are compared
    verbatim elsewhere, so they are kept exactly as supplied.  A caller
    supplied Accept-Encoding is overridden: this client always requests
    identity encoding.
    """

    if headers is None:
        items = []
    elif isinstance(headers, Mapping):
        items = list(headers.items())
    else:
        items = list(headers)
    seen = {}
    for item in items:
        try:
            name, value = item
        except (TypeError, ValueError) as exc:
            raise RequestRejected(f"bad header entry {item!r}") from exc
        if not isinstance(name, str) or not isinstance(value, str):
            raise RequestRejected("header names and values must be strings")
        lname = name.lower()
        if not _TOKEN_RE.match(lname):
            raise RequestRejected(f"invalid header name {name!r}")
        if lname in FORBIDDEN_HEADERS:
            raise RequestRejected(f"header {name!r} is not allowed")
        if "\r" in value or "\n" in value or "\x00" in value:
            raise RequestRejected(f"invalid characters in value of {name!r}")
        if lname in seen:
            raise RequestRejected(f"duplicate request header {name!r}")
        seen[lname] = value
    # The client fixes identity encoding; a caller supplied value is replaced.
    seen.pop("accept-encoding", None)
    seen["accept-encoding"] = "identity"
    return tuple(sorted(seen.items()))


def prepare_request(url: str, headers) -> PreparedRequest:
    identity, scheme, host, port, path_query = normalize_url(url)
    canon = validate_headers(headers)
    return PreparedRequest(
        original_url=url,
        identity=identity,
        scheme=scheme,
        host=host,
        port=port,
        path_query=path_query,
        headers=canon,
    )
