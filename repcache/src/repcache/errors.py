"""Exception hierarchy for repcache.

Failure meanings
----------------
RequestRejected
    The caller's request was refused before any network or cache activity:
    forbidden header (Authorization/Cookie/Range/conditional/...), duplicate
    header name, user-info in URL, unsupported scheme, header injection.
ProtocolError
    The origin server violated the supported HTTP subset: invalid or
    conflicting Cache-Control lifetime, invalid ETag, invalid Date/Age,
    unsupported Content-Encoding, conflicting Content-Length, a 304 that does
    not correspond to the precondition we sent (none sent, ETag mismatch,
    different Vary field set).  Previously cached representations are kept;
    the failed exchange is never stored nor reported as success.
NetworkError
    Transport level failure: connect error, timeout, reset, truncated body
    (断流), declared-length mismatch, or response over the size limit.
    Truncated/oversize bodies are never committed to the cache.
CacheClosed
    The client (or another client object for the same operation) was closed.
RepCacheError
    Base class; also raised when cache selection fails to converge.
"""


class RepCacheError(Exception):
    """Base class for all repcache failures."""


class RequestRejected(RepCacheError, ValueError):
    """The caller's request is not allowed by this client's protocol subset."""


class ProtocolError(RepCacheError):
    """The origin server violated the supported HTTP subset."""


class NetworkError(RepCacheError):
    """Timeout, truncation, oversize or other transport failure."""


class CacheClosed(RepCacheError):
    """The client has been closed."""
