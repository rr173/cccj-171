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
    Base class for wire-level failures.  Not all of them may be masked by a
    stale-if-error fallback; see TransportError below.
TransportError
    A failure of the connection itself: connect error, timeout, reset by the
    peer, or a body stream that ended early (断流).  Only this family is
    eligible for an opt-in stale-if-error fallback.
OversizeError
    The response exceeded ``max_bytes`` (before or after reading).  Never
    committed to the cache and never masked by a stale body.
LengthMismatchError
    The received body length did not match a well-formed declared
    Content-Length.  Never committed nor masked.
MalformedResponseError
    The peer spoke something outside the supported HTTP framing (bad status
    line, etc.).  Never masked.
CacheClosed
    The client (or another client object for the same operation) was closed.
RepCacheError
    Base class; also raised when cache selection fails to converge.
"""


class RepCacheError(Exception):
    """Base class for all repcache failures.

    ``stale_if_error_eligible`` is the class-level answer to "may an opt-in
    stale-if-error fallback hide this failure?".  It is False everywhere by
    default, so new failure modes stay visible until explicitly classified.
    """

    stale_if_error_eligible = False


class RequestRejected(RepCacheError, ValueError):
    """The caller's request is not allowed by this client's protocol subset."""


class ProtocolError(RepCacheError):
    """The origin server violated the supported HTTP subset."""


class NetworkError(RepCacheError):
    """Base class for wire-level failures."""


class TransportError(NetworkError):
    """Connect failure, timeout, reset or truncated stream (断流)."""

    stale_if_error_eligible = True


class OversizeError(NetworkError):
    """The response exceeded the configured size limit."""


class LengthMismatchError(NetworkError):
    """The body length disagreed with the declared Content-Length."""


class MalformedResponseError(NetworkError):
    """The peer sent HTTP framing this client does not support."""


class CacheClosed(RepCacheError):
    """The client has been closed."""
