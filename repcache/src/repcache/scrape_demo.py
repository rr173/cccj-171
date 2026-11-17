"""Executable scraping drill (采集演练) for repcache.

Spins up the local bilingual directory service and walks through the
required scenarios, asserting every step against the *server-side* receive
log.  Time is advanced through an injected clock; nothing sleeps long and
nothing touches the public network.

Run:  python -m repcache.scrape_demo   (or: python examples/scrape_demo.py)
"""

from __future__ import annotations

import os
import tempfile
import threading
import time

from .client import RepCacheClient
from .demo_server import DemoServer, Flag, Gate, ResponseSpec, http_date
from .errors import NetworkError, ProtocolError
from .model import Source

ZH_BODY = "目录\n- 公告\n- 产品\n".encode("utf-8")
EN_BODY = b"directory\n- announcements\n- products\n"


class Clock:
    def __init__(self, t=1_700_000_000.0):
        self.t = float(t)

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


def check(label, cond):
    if not cond:
        raise AssertionError(label)
    print(f"  ok - {label}")


def parallel(calls):
    results, errors = [None] * len(calls), [None] * len(calls)

    def wrap(i, fn):
        try:
            results[i] = fn()
        except BaseException as exc:  # noqa: BLE001
            errors[i] = exc

    threads = [threading.Thread(target=wrap, args=(i, f)) for i, f in enumerate(calls)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(15)
    return results, errors


def main():
    clock = Clock()
    tmp = tempfile.TemporaryDirectory()
    db = os.path.join(tmp.name, "demo-cache.sqlite")
    server = DemoServer(clock=clock)
    server.start()
    try:
        client = RepCacheClient(db, clock=clock, timeout=5.0)

        print("scenario 1: two identical cold requests share a single GET")
        gate = Gate()

        def route_dir(req):
            gate.wait()
            lang = req.header("accept-language") or "en"
            body = ZH_BODY if lang == "zh" else EN_BODY
            return ResponseSpec(
                200,
                [
                    ("Content-Type", "text/plain; charset=utf-8"),
                    ("Vary", "Accept-Language"),
                    ("Cache-Control", "max-age=30"),
                    ("ETag", f'"{lang}1"'),
                    ("Date", http_date(clock())),
                ],
                body,
            )

        server.route("/dir", route_dir)
        url = server.url("/dir")
        results, errors = [None, None], [None, None]

        def call(i):
            try:
                results[i] = client.get(url, {"Accept-Language": "zh"})
            except BaseException as exc:  # noqa: BLE001
                errors[i] = exc

        ts = [threading.Thread(target=call, args=(i,)) for i in range(2)]
        for t in ts:
            t.start()
        server.wait_for_requests(1, "/dir")
        time.sleep(0.2)
        check("only one request reached the server", len(server.requests("/dir")) == 1)
        gate.open()
        for t in ts:
            t.join(15)
        check("both scrapers got the Chinese directory",
              [r.body for r in results] == [ZH_BODY, ZH_BODY])

        print("scenario 2: different languages are different representations")
        res_en = client.get(url, {"Accept-Language": "en"})
        check("english task got the english body", res_en.body == EN_BODY)
        check("server saw one request per language", len(server.requests("/dir")) == 2)
        check("cached zh hit needs no network",
              client.get(url, {"Accept-Language": "zh"}).source is Source.CACHE)
        check("still two server requests", len(server.requests("/dir")) == 2)

        print("scenario 3: known Vary -> one revalidation per representation, "
              "other language fetches in parallel")
        clock.advance(40)  # both representations are stale now
        barrier = threading.Barrier(2, timeout=5)

        def route_dir_304(req):
            barrier.wait()  # zh and en validations must overlap on the wire
            return ResponseSpec(
                304,
                [
                    ("Vary", "Accept-Language"),
                    ("Cache-Control", "max-age=30"),
                    ("Date", http_date(clock())),
                ],
                b"",
            )

        server.route("/dir", route_dir_304)
        before = len(server.requests("/dir"))
        results, errors = parallel(
            [
                lambda: client.get(url, {"Accept-Language": "zh", "X-Trace": "a"}),
                lambda: client.get(url, {"Accept-Language": "zh", "X-Trace": "b"}),
                lambda: client.get(url, {"Accept-Language": "en"}),
            ]
        )
        check("no errors", errors == [None, None, None])
        check("bodies correct", [r.body for r in results] == [ZH_BODY, ZH_BODY, EN_BODY])
        phase = server.requests("/dir")[before:]
        check("exactly two validations on the wire", len(phase) == 2)
        inms = sorted(r.header("if-none-match") for r in phase)
        check("each validation carried its own representation's ETag",
              inms == ['"en1"', '"zh1"'])
        check("no 304 empty body leaked to parsers", all(r.body for r in results))
        server.route("/dir", route_dir)

        print("scenario 4: Vary changes (language -> language+region), "
              "mismatched waiters refetch")
        clock.advance(40)

        def route_region(req):
            region = req.header("x-region") or "none"
            return ResponseSpec(
                200,
                [
                    ("Vary", "Accept-Language, X-Region"),
                    ("Cache-Control", "max-age=30"),
                    ("ETag", '"zh2"'),
                    ("Date", http_date(clock())),
                ],
                f"zh-dir region={region}".encode(),
            )

        server.route("/dir", route_region)
        before = len(server.requests("/dir"))
        results, errors = parallel(
            [
                lambda: client.get(url, {"Accept-Language": "zh", "X-Region": "r1"}),
                lambda: client.get(url, {"Accept-Language": "zh", "X-Region": "r2"}),
            ]
        )
        check("no errors", errors == [None, None])
        check("each caller reads its own region's body",
              results[0].body == b"zh-dir region=r1"
              and results[1].body == b"zh-dir region=r2")
        check("one shared conditional + one refetch",
              len(server.requests("/dir")) == before + 2)

        print("scenario 5: refresh beats a late in-flight 304")
        gate304, arrived = Gate(), Flag()
        state = {"phase": 1}

        def route_refresh(req):
            inm = req.header("if-none-match")
            if inm == '"v1"':
                arrived.set()
                gate304.wait()
                return ResponseSpec(
                    304,
                    [("Vary", "Accept-Language"), ("Cache-Control", "max-age=30"),
                     ("Date", http_date(clock()))],
                    b"",
                )
            if state["phase"] == 1:
                return ResponseSpec(
                    200,
                    [("Vary", "Accept-Language"), ("Cache-Control", "max-age=30"),
                     ("ETag", '"v1"'), ("Date", http_date(clock()))],
                    b"directory v1",
                )
            return ResponseSpec(
                200,
                [("Vary", "Accept-Language"), ("Cache-Control", "max-age=300"),
                 ("ETag", '"v2"'), ("Date", http_date(clock()))],
                b"directory v2 (refreshed)",
            )

        server.route("/page", route_refresh)
        purl = server.url("/page")
        check("primed v1", client.get(purl, {"Accept-Language": "zh"}).body == b"directory v1")
        state["phase"] = 2
        clock.advance(40)
        held = {}

        def stale_call():
            try:
                held["res"] = client.get(purl, {"Accept-Language": "zh"})
            except BaseException as exc:  # noqa: BLE001
                held["err"] = exc

        t = threading.Thread(target=stale_call)
        t.start()
        arrived.wait()
        refreshed = client.refresh(purl, {"Accept-Language": "zh"})
        check("refresh published v2", refreshed.body == b"directory v2 (refreshed)")
        gate304.open()
        t.join(15)
        check("no error in the held caller", "err" not in held)
        check("the original call did not fall back to the old body",
              held["res"].body == b"directory v2 (refreshed)")
        check("later queries serve v2 without new traffic",
              client.get(purl, {"Accept-Language": "zh"}).body == b"directory v2 (refreshed)")
        check("server saw prime + conditional + refresh",
              len(server.requests("/page")) == 3)

        print("scenario 6: no-store / Vary:* bodies are never shared")
        server.route("/ns", lambda req: ResponseSpec(
            200, [("Cache-Control", "no-store"), ("Date", http_date(clock()))],
            b"no-store body"))
        nsurl = server.url("/ns")
        results, _ = parallel([lambda: client.get(nsurl), lambda: client.get(nsurl)])
        check("both got the body", [r.body for r in results] == [b"no-store body"] * 2)
        check("waiter could not share: two requests", len(server.requests("/ns")) == 2)
        server.route("/vs", lambda req: ResponseSpec(
            200, [("Vary", "*"), ("Cache-Control", "max-age=60"),
                  ("Date", http_date(clock()))], b"star body"))
        vsurl = server.url("/vs")
        parallel([lambda: client.get(vsurl), lambda: client.get(vsurl)])
        check("Vary:* also fetched per caller", len(server.requests("/vs")) == 2)

        print("scenario 7: Age boundary (current_age == lifetime is stale)")
        server.route("/age", lambda req: ResponseSpec(
            304 if req.header("if-none-match") == '"a1"' else 200,
            [("Cache-Control", "max-age=10"), ("Age", "0" if req.header("if-none-match") else "6"),
             ("ETag", '"a1"'), ("Date", http_date(clock()))],
            b"" if req.header("if-none-match") else b"aged body"))
        aurl = server.url("/age")
        client.get(aurl)
        clock.advance(3)
        check("age 9 < 10 is fresh",
              client.get(aurl).source is Source.CACHE and len(server.requests("/age")) == 1)
        clock.advance(1)
        res = client.get(aurl)
        check("age 10 == 10 forces revalidation",
              res.source is Source.REVALIDATED and len(server.requests("/age")) == 2)

        print("scenario 8: no-cache validates every single use")
        server.route("/nc", lambda req: ResponseSpec(
            304 if req.header("if-none-match") == '"nc1"' else 200,
            [("Cache-Control", "no-cache"), ("ETag", '"nc1"'), ("Date", http_date(clock()))],
            b"" if req.header("if-none-match") else b"no-cache body"))
        ncurl = server.url("/nc")
        client.get(ncurl)
        client.get(ncurl)
        client.get(ncurl)
        check("three uses -> three server requests", len(server.requests("/nc")) == 3)

        print("scenario 9: 304 metadata rewrites the lifetime")
        state9 = {"n": 0}

        def route_life(req):
            if req.header("if-none-match") == '"m1"':
                return ResponseSpec(
                    304, [("Cache-Control", "max-age=100"), ("Age", "0"),
                          ("Date", http_date(clock()))], b"")
            return ResponseSpec(
                200, [("Cache-Control", "max-age=5"), ("ETag", '"m1"'),
                      ("Date", http_date(clock()))], b"lifetime body")

        server.route("/life", route_life)
        lurl = server.url("/life")
        client.get(lurl)
        clock.advance(10)
        res = client.get(lurl)
        check("revalidated", res.source is Source.REVALIDATED and res.age.lifetime == 100.0)
        clock.advance(50)
        check("fresh under the new lifetime",
              client.get(lurl).source is Source.CACHE and len(server.requests("/life")) == 2)

        print("scenario 10: 304 without precondition / with wrong ETag fails clearly")
        server.route("/bad304", lambda req: ResponseSpec(
            304, [("Cache-Control", "max-age=5"), ("Date", http_date(clock()))], b""))
        try:
            client.get(server.url("/bad304"))
            raise SystemExit("expected ProtocolError")
        except ProtocolError:
            check("304 to an unconditional request raised ProtocolError", True)
        server.route("/wrongtag", lambda req: ResponseSpec(
            304 if req.header("if-none-match") else 200,
            [("Cache-Control", "max-age=5"), ("Date", http_date(clock()))]
            + ([("ETag", '"other"')] if req.header("if-none-match") else [("ETag", '"w1"')]),
            b"" if req.header("if-none-match") else b"wrong-tag body"))
        wurl = server.url("/wrongtag")
        client.get(wurl)
        clock.advance(10)
        try:
            client.get(wurl)
            raise SystemExit("expected ProtocolError")
        except ProtocolError:
            check("304 with a mismatched ETag raised ProtocolError", True)

        print("scenario 11: truncated stream fails, next call retries")
        state11 = {"ok": False}

        def route_trunc(req):
            if not state11["ok"]:
                return ResponseSpec(200, [("Cache-Control", "max-age=60"),
                                          ("Date", http_date(clock()))],
                                    b"full body here", truncate_at=4)
            return ResponseSpec(200, [("Cache-Control", "max-age=60"),
                                      ("Date", http_date(clock()))], b"full body here")

        server.route("/trunc", route_trunc)
        turl = server.url("/trunc")
        try:
            client.get(turl)
            raise SystemExit("expected NetworkError")
        except NetworkError:
            check("truncated body raised NetworkError", True)
        state11["ok"] = True
        check("retry fetched the full body", client.get(turl).body == b"full body here")
        check("nothing truncated was cached (2 network requests)",
              len(server.requests("/trunc")) == 2)

        print("scenario 12: close and reopen reuses committed representations")
        server.route("/stable", lambda req: ResponseSpec(
            200, [("Cache-Control", "max-age=100000"), ("ETag", '"s1"'),
                  ("Date", http_date(clock()))], b"stable body"))
        surl = server.url("/stable")
        check("primed stable representation",
              client.get(surl).body == b"stable body")
        client.close()
        client = RepCacheClient(db, clock=clock, timeout=5.0)
        res = client.get(surl)
        check("served from the reopened cache", res.source is Source.CACHE)
        check("no new traffic after reopen", len(server.requests("/stable")) == 1)
        client.close()

        print("\nall scenarios passed")
    finally:
        server.stop()
        tmp.cleanup()


if __name__ == "__main__":
    main()
