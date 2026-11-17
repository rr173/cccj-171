"""repcache: an HTTP representation reuse client for scrapers.

Public API: RepCacheClient, FetchResult, Source, AgeInfo and the error
hierarchy rooted at RepCacheError.
"""

from .client import RepCacheClient
from .errors import (
    CacheClosed,
    LengthMismatchError,
    MalformedResponseError,
    NetworkError,
    OversizeError,
    ProtocolError,
    RepCacheError,
    RequestRejected,
    TransportError,
)
from .model import AgeInfo, FetchResult, Source, StaleFallback

__all__ = [
    "RepCacheClient",
    "FetchResult",
    "Source",
    "AgeInfo",
    "StaleFallback",
    "RepCacheError",
    "RequestRejected",
    "ProtocolError",
    "NetworkError",
    "TransportError",
    "OversizeError",
    "LengthMismatchError",
    "MalformedResponseError",
    "CacheClosed",
]

__version__ = "1.0.0"
