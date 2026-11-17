"""SQLite persistence for URL vary-rules and representations.

One URL has at most one *current* vary rule (field set + epoch).  When a 200
changes the field set, the epoch is bumped and every representation stored
under older epochs is deleted, so two selection rules can never coexist.
In-flight coordination lives only in process memory, so reopening a cache
file can never deadlock on stale markers.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import dataclass
from typing import Optional, Sequence, Tuple

_SCHEMA = """
CREATE TABLE IF NOT EXISTS url_rules (
    url TEXT PRIMARY KEY,
    vary_fields TEXT NOT NULL,   -- JSON array of lowercase field names
    epoch INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS representations (
    url TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    vary_values TEXT NOT NULL,   -- JSON array of [field, value-or-null]
    status INTEGER NOT NULL,
    headers TEXT NOT NULL,       -- JSON array of [name, value]
    body BLOB NOT NULL,
    etag TEXT,                   -- NULL when the 200 carried none
    lifetime REAL,               -- seconds; NULL => always validate
    must_validate INTEGER NOT NULL,
    must_revalidate INTEGER NOT NULL DEFAULT 0,
    stale_if_error REAL,         -- server grant; NULL => no stale-if-error
    date_epoch REAL,             -- parsed Date; NULL when absent
    age_header REAL NOT NULL,    -- parsed Age; 0 when absent
    q REAL NOT NULL,             -- request-sent time of the storing exchange
    r REAL NOT NULL,             -- response-received time of that exchange
    initial_age REAL NOT NULL,
    PRIMARY KEY (url, epoch, vary_values)
);
"""


@dataclass(frozen=True)
class Rule:
    fields: Tuple[str, ...]
    epoch: int


@dataclass(frozen=True)
class Rep:
    url: str
    epoch: int
    vary_values: Tuple[Tuple[str, Optional[str]], ...]
    status: int
    headers: Tuple[Tuple[str, str], ...]
    body: bytes
    etag: Optional[str]
    lifetime: Optional[float]
    must_validate: bool
    date_epoch: Optional[float]
    age_header: float
    q: float
    r: float
    initial_age: float
    must_revalidate: bool = False
    stale_if_error: Optional[float] = None

    @property
    def vary_fields(self) -> Tuple[str, ...]:
        return tuple(name for name, _ in self.vary_values)

    @property
    def allows_stale_if_error(self) -> bool:
        """The stored response's own permission, independent of callers.

        Requires an explicit ``stale-if-error`` second and an explicit
        freshness lifetime, and is vetoed by no-cache (folded into
        ``must_validate``) or must-revalidate.  A no-lifetime representation
        has no defined staleness, so it is never served stale.
        """

        return (
            self.stale_if_error is not None
            and not self.must_validate
            and not self.must_revalidate
            and self.lifetime is not None
        )


def dumps_values(values: Sequence[Tuple[str, Optional[str]]]) -> str:
    return json.dumps(list(values), separators=(",", ":"))


def _loads_values(text: str) -> Tuple[Tuple[str, Optional[str]], ...]:
    return tuple((name, value) for name, value in json.loads(text))


class Store:
    """Thread-safe-enough SQLite store; callers serialize via the client lock."""

    def __init__(self, path: str):
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA foreign_keys=ON")
        with self._conn:
            self._conn.executescript(_SCHEMA)
            self._migrate()

    def _migrate(self) -> None:
        """Add columns introduced after the initial schema (old cache files)."""

        columns = {
            row[1]
            for row in self._conn.execute("PRAGMA table_info(representations)")
        }
        if "must_revalidate" not in columns:
            self._conn.execute(
                "ALTER TABLE representations"
                " ADD COLUMN must_revalidate INTEGER NOT NULL DEFAULT 0"
            )
        if "stale_if_error" not in columns:
            self._conn.execute(
                "ALTER TABLE representations ADD COLUMN stale_if_error REAL"
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # -- vary rules ---------------------------------------------------------

    def get_rule(self, url: str) -> Optional[Rule]:
        row = self._conn.execute(
            "SELECT vary_fields, epoch FROM url_rules WHERE url=?", (url,)
        ).fetchone()
        if row is None:
            return None
        return Rule(fields=tuple(json.loads(row[0])), epoch=int(row[1]))

    def put_rule(self, url: str, fields: Sequence[str], epoch: int) -> None:
        with self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO url_rules (url, vary_fields, epoch)"
                " VALUES (?, ?, ?)",
                (url, json.dumps(list(fields), separators=(",", ":")), epoch),
            )

    # -- representations ----------------------------------------------------

    def get_rep(self, url: str, epoch: int, vary_values_json: str) -> Optional[Rep]:
        row = self._conn.execute(
            "SELECT status, headers, body, etag, lifetime, must_validate,"
            "       date_epoch, age_header, q, r, initial_age,"
            "       must_revalidate, stale_if_error"
            " FROM representations WHERE url=? AND epoch=? AND vary_values=?",
            (url, epoch, vary_values_json),
        ).fetchone()
        if row is None:
            return None
        return Rep(
            url=url,
            epoch=epoch,
            vary_values=_loads_values(vary_values_json),
            status=int(row[0]),
            headers=tuple((n, v) for n, v in json.loads(row[1])),
            body=bytes(row[2]),
            etag=row[3],
            lifetime=row[4],
            must_validate=bool(row[5]),
            date_epoch=row[6],
            age_header=float(row[7]),
            q=float(row[8]),
            r=float(row[9]),
            initial_age=float(row[10]),
            must_revalidate=bool(row[11]),
            stale_if_error=row[12],
        )

    def put_rep(self, rep: Rep) -> None:
        with self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO representations"
                " (url, epoch, vary_values, status, headers, body, etag,"
                "  lifetime, must_validate, must_revalidate, stale_if_error,"
                "  date_epoch, age_header, q, r, initial_age)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    rep.url,
                    rep.epoch,
                    dumps_values(rep.vary_values),
                    rep.status,
                    json.dumps([list(h) for h in rep.headers], separators=(",", ":")),
                    sqlite3.Binary(rep.body),
                    rep.etag,
                    rep.lifetime,
                    1 if rep.must_validate else 0,
                    1 if rep.must_revalidate else 0,
                    rep.stale_if_error,
                    rep.date_epoch,
                    rep.age_header,
                    rep.q,
                    rep.r,
                    rep.initial_age,
                ),
            )

    def delete_rep(self, url: str, epoch: int, vary_values_json: str) -> None:
        with self._conn:
            self._conn.execute(
                "DELETE FROM representations"
                " WHERE url=? AND epoch=? AND vary_values=?",
                (url, epoch, vary_values_json),
            )

    def delete_reps_before(self, url: str, epoch: int) -> None:
        with self._conn:
            self._conn.execute(
                "DELETE FROM representations WHERE url=? AND epoch<?", (url, epoch)
            )
