"""HTTP/1.1 transport built on the standard library only.

No redirects, no content compression (the client always requests identity),
response size limited, declared length verified.  Truncated or oversize
bodies raise NetworkError and are never handed to the cache.
"""

from __future__ import annotations

import http.client
import socket
from dataclasses import dataclass
from typing import Callable, List, Optional, Tuple

from .errors import NetworkError, ProtocolError

_BODYLESS_STATUSES = frozenset({204, 304})


@dataclass(frozen=True)
class WireResponse:
    status: int
    reason: str
    headers: Tuple[Tuple[str, str], ...]
    body: bytes
    q: float  # request-sent time on the client clock
    r: float  # response-fully-received time on the client clock


def wire_request(
    scheme: str,
    host: str,
    port: int,
    path_query: str,
    headers: Tuple[Tuple[str, str], ...],
    *,
    timeout: float,
    max_bytes: int,
    clock: Callable[[], float],
    if_none_match: Optional[str] = None,
) -> WireResponse:
    """Perform one GET and return the complete response.

    Raises NetworkError on timeout/truncation/oversize and ProtocolError on
    responses outside the supported subset.
    """

    conn_cls = http.client.HTTPSConnection if scheme == "https" else http.client.HTTPConnection
    conn = conn_cls(host, port, timeout=timeout)
    header_dict = dict(headers)
    if if_none_match is not None:
        header_dict["If-None-Match"] = if_none_match
    header_dict["Connection"] = "close"
    q = clock()
    try:
        conn.request("GET", path_query, headers=header_dict)
        resp = conn.getresponse()
        status = resp.status
        reason = resp.reason or ""
        resp_headers: List[Tuple[str, str]] = list(resp.getheaders())

        encodings = [v for n, v in resp_headers if n.lower() == "content-encoding"]
        for value in encodings:
            if value.strip().lower() != "identity":
                raise ProtocolError(f"unsupported Content-Encoding {value!r}")

        declared: Optional[int] = None
        cl_values = [v for n, v in resp_headers if n.lower() == "content-length"]
        if cl_values:
            lengths = set()
            for value in cl_values:
                for part in value.split(","):
                    part = part.strip()
                    if not part.isdigit():
                        raise ProtocolError(f"invalid Content-Length {part!r}")
                    lengths.add(int(part))
            if len(lengths) != 1:
                raise ProtocolError("conflicting Content-Length headers")
            declared = lengths.pop()
            if (
                declared > max_bytes
                and status not in _BODYLESS_STATUSES
                and not 100 <= status < 200
            ):
                raise NetworkError(
                    f"response too large: declared {declared} bytes, limit {max_bytes}"
                )

        try:
            body = resp.read(max_bytes + 1)
        except http.client.IncompleteRead as exc:
            raise NetworkError(f"truncated body: {exc}") from exc
        r = clock()

        if len(body) > max_bytes:
            raise NetworkError(
                f"response too large: got more than {max_bytes} bytes"
            )
        if (
            declared is not None
            and len(body) != declared
            and status not in _BODYLESS_STATUSES
            and not 100 <= status < 200
        ):
            raise NetworkError(
                f"length mismatch: declared {declared}, received {len(body)}"
            )
        return WireResponse(status, reason, tuple(resp_headers), body, q, r)
    except (NetworkError, ProtocolError):
        raise
    except (socket.timeout, TimeoutError) as exc:
        raise NetworkError(f"timeout: {exc}") from exc
    except (OSError, http.client.HTTPException) as exc:
        raise NetworkError(f"transport failure: {exc}") from exc
    finally:
        conn.close()
