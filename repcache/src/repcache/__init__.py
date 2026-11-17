"""repcache: an HTTP representation reuse client for scrapers.

Public API: RepCacheClient, FetchResult, Source, AgeInfo and the error
hierarchy rooted at RepCacheError.
"""

from .client import RepCacheClient
from .errors import (
    CacheClosed,
    NetworkError,
    ProtocolError,
    RepCacheError,
    RequestRejected,
)
from .model import AgeInfo, FetchResult, Source

__all__ = [
    "RepCacheClient",
    "FetchResult",
    "Source",
    "AgeInfo",
    "RepCacheError",
    "RequestRejected",
    "ProtocolError",
    "NetworkError",
    "CacheClosed",
]

__version__ = "1.0.0"
