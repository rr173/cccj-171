"""End-to-end tests for repcache against a local bilingual HTTP service.

Every expectation about network traffic is asserted against the *server-side*
receive log (never client counters); gates control interleavings; time is
advanced through an injected clock, never by sleeping.
"""

import os
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from repcache import (  # noqa: E402
    ProtocolError,
    RepCacheClient,
    RepCacheError,
    RequestRejected,
    NetworkError,
    Source,
)
from repcache.demo_server import (  # noqa: E402
    DemoServer,
    Flag,
    Gate,
    ResponseSpec,
    http_date,
)
from repcache.urlutil import normalize_url  # noqa: E402

ZH_BODY = "目录\n- 公告\n- 产品\n".encode("utf-8")
EN_BODY = b"directory\n- announcements\n- products\n"


class FakeClock:
    def __init__(self, t=1_700_000_000.0):
        self.t = float(t)

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


def run_threads(fns):
    """Run callables concurrently, return (results, errors) by index."""
    results = [None] * len(fns)
    errors = [None] * len(fns)

    def wrap(i, fn):
        try:
            results[i] = fn()
        except BaseException as exc:  # noqa: BLE001
            errors[i] = exc

    threads = [threading.Thread(target=wrap, args=(i, fn)) for i, fn in enumerate(fns)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(15)
    for t in threads:
        assert not t.is_alive(), "a worker thread is stuck"
    return results, errors


class ClientTestCase(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.server = DemoServer(clock=self.clock)
        self.server.start()
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "cache.sqlite")
        self.client = self.new_client()

    def new_client(self, **kw):
        kw.setdefault("clock", self.clock)
        kw.setdefault("timeout", 5.0)
        return RepCacheClient(self.db, **kw)

    def tearDown(self):
        try:
            self.client.close()
        except Exception:
            pass
        self.server.stop()
        self.tmp.cleanup()

    def url(self, path):
        return self.server.url(path)


class TestColdDedup(ClientTestCase):
    def test_identical_cold_requests_share_one_get(self):
        gate = Gate()

        def route(req):
            gate.wait()
            return ResponseSpec(
                200,
                [
                    ("Content-Type", "text/plain; charset=utf-8"),
                    ("Vary", "Accept-Language"),
                    ("Cache-Control", "max-age=60"),
                    ("ETag", '"zh1"'),
                    ("Date", http_date(self.clock())),
                ],
                ZH_BODY,
            )

        self.server.route("/dir", route)
        url = self.url("/dir")
        results, errors = [None, None], [None, None]

        def call(i):
            try:
                results[i] = self.client.get(url, {"Accept-Language": "zh"})
            except BaseException as exc:  # noqa: BLE001
                errors[i] = exc

        threads = [threading.Thread(target=call, args=(i,)) for i in range(2)]
        for t in threads:
            t.start()
        # the single shared request reaches the server and blocks on the gate
        self.server.wait_for_requests(1, "/dir")
        time.sleep(0.2)  # give the second thread a chance to misbehave
        # it was deduplicated onto the in-flight request: still exactly one
        self.assertEqual(len(self.server.requests("/dir")), 1)
        gate.open()
        for t in threads:
            t.join(15)
        self.assertEqual(errors, [None, None])
        self.assertEqual([r.body for r in results], [ZH_BODY, ZH_BODY])
        self.assertEqual(len(self.server.requests("/dir")), 1)
        self.assertEqual(
            self.server.requests("/dir")[0].header("accept-language"), "zh"
        )
        self.assertEqual(
            self.server.requests("/dir")[0].header("accept-encoding"), "identity"
        )

    def test_different_languages_each_get_own_body(self):
        def route(req):
            lang = req.header("accept-language") or "en"
            body = ZH_BODY if lang == "zh" else EN_BODY
            return ResponseSpec(
                200,
                [
                    ("Vary", "Accept-Language"),
                    ("Cache-Control", "max-age=60"),
                    ("ETag", f'"{lang}1"'),
                    ("Date", http_date(self.clock())),
                ],
                body,
            )

        self.server.route("/dir", route)
        url = self.url("/dir")
        results, errors = run_threads(
            [
                lambda: self.client.get(url, {"Accept-Language": "zh"}),
                lambda: self.client.get(url, {"Accept-Language": "en"}),
            ]
        )
        self.assertEqual(errors, [None, None])
        self.assertEqual(results[0].body, ZH_BODY)
        self.assertEqual(results[1].body, EN_BODY)
        self.assertEqual(len(self.server.requests("/dir")), 2)
        # and now both are cached: no further traffic
        again_zh = self.client.get(url, {"Accept-Language": "zh"})
        again_en = self.client.get(url, {"Accept-Language": "en"})
        self.assertEqual(again_zh.body, ZH_BODY)
        self.assertEqual(again_en.body, EN_BODY)
        self.assertEqual(again_zh.source, Source.CACHE)
        self.assertEqual(len(self.server.requests("/dir")), 2)


class TestKnownVarySharing(ClientTestCase):
    def prime(self):
        def route(req):
            lang = req.header("accept-language") or "en"
            body = ZH_BODY if lang == "zh" else EN_BODY
            return ResponseSpec(
                200,
                [
                    ("Vary", "Accept-Language"),
                    ("Cache-Control", "max-age=30"),
                    ("ETag", f'"{lang}1"'),
                    ("Date", http_date(self.clock())),
                ],
                body,
            )

        self.server.route("/dir", route)
        url = self.url("/dir")
        self.assertEqual(self.client.get(url, {"Accept-Language": "zh"}).body, ZH_BODY)
        self.assertEqual(self.client.get(url, {"Accept-Language": "en"}).body, EN_BODY)
        self.assertEqual(len(self.server.requests("/dir")), 2)
        return url

    def test_stale_sharing_and_parallel_other_language(self):
        url = self.prime()
        self.clock.advance(40)  # both representations now stale

        barrier = threading.Barrier(2, timeout=5)

        def route2(req):
            inm = req.header("if-none-match")
            barrier.wait()  # prove the two validations run concurrently
            return ResponseSpec(
                304,
                [
                    ("Vary", "Accept-Language"),
                    ("Cache-Control", "max-age=30"),
                    ("Date", http_date(self.clock())),
                ],
                b"",
            )

        self.server.route("/dir", route2)
        results, errors = run_threads(
            [
                lambda: self.client.get(url, {"Accept-Language": "zh", "X-Trace": "a"}),
                lambda: self.client.get(url, {"Accept-Language": "zh", "X-Trace": "b"}),
                lambda: self.client.get(url, {"Accept-Language": "en"}),
            ]
        )
        self.assertEqual(errors, [None, None, None])
        self.assertEqual(results[0].body, ZH_BODY)
        self.assertEqual(results[1].body, ZH_BODY)
        self.assertEqual(results[2].body, EN_BODY)
        # exactly one conditional request per language reached the server
        phase2 = self.server.requests("/dir")[2:]
        self.assertEqual(len(phase2), 2)
        by_inm = {}
        for req in phase2:
            by_inm.setdefault(req.header("if-none-match"), []).append(req)
        self.assertEqual(len(by_inm['"zh1"']), 1)
        self.assertEqual(len(by_inm['"en1"']), 1)
        # the shared zh validation carried one of the irrelevant trace headers
        self.assertIn(by_inm['"zh1"'][0].header("x-trace"), ("a", "b"))
        # at least one caller was validated through the 304 path
        self.assertTrue(any(r.validated for r in results))
        self.assertTrue(all(r.body != b"" for r in results))


class TestVaryChange(ClientTestCase):
    def test_vary_change_forces_region_refetch(self):
        def route1(req):
            return ResponseSpec(
                200,
                [
                    ("Vary", "Accept-Language"),
                    ("Cache-Control", "max-age=30"),
                    ("ETag", '"zh1"'),
                    ("Date", http_date(self.clock())),
                ],
                ZH_BODY,
            )

        self.server.route("/dir", route1)
        url = self.url("/dir")
        self.assertEqual(self.client.get(url, {"Accept-Language": "zh"}).body, ZH_BODY)
        self.clock.advance(40)  # stale

        def body_for(region):
            return f"zh-dir region={region}".encode()

        def route2(req):
            region = req.header("x-region") or "none"
            return ResponseSpec(
                200,
                [
                    ("Vary", "Accept-Language, X-Region"),
                    ("Cache-Control", "max-age=30"),
                    ("ETag", '"zh2"'),
                    ("Date", http_date(self.clock())),
                ],
                body_for(region),
            )

        self.server.route("/dir", route2)
        results, errors = run_threads(
            [
                lambda: self.client.get(url, {"Accept-Language": "zh", "X-Region": "r1"}),
                lambda: self.client.get(url, {"Accept-Language": "zh", "X-Region": "r2"}),
            ]
        )
        self.assertEqual(errors, [None, None])
        # each caller received the body of its own region, never the other's
        self.assertEqual(results[0].body, body_for("r1"))
        self.assertEqual(results[1].body, body_for("r2"))
        phase2 = self.server.requests("/dir")[1:]
        self.assertEqual(len(phase2), 2)
        conditionals = [r for r in phase2 if r.header("if-none-match")]
        unconditionals = [r for r in phase2 if not r.header("if-none-match")]
        self.assertEqual(len(conditionals), 1)
        self.assertEqual(conditionals[0].header("if-none-match"), '"zh1"')
        self.assertEqual(len(unconditionals), 1)

        # both region representations are now cached under the new rule
        self.assertEqual(
            self.client.get(url, {"Accept-Language": "zh", "X-Region": "r1"}).body,
            body_for("r1"),
        )
        self.assertEqual(
            self.client.get(url, {"Accept-Language": "zh", "X-Region": "r2"}).body,
            body_for("r2"),
        )
        self.assertEqual(len(self.server.requests("/dir")), 3)

        # a caller without the region header does not match any stored
        # representation and must fetch (old rule is gone for good)
        res = self.client.get(url, {"Accept-Language": "zh"})
        self.assertEqual(res.body, body_for("none"))
        self.assertEqual(len(self.server.requests("/dir")), 4)


class TestRefreshVsStale304(ClientTestCase):
    def test_refresh_wins_over_late_304(self):
        V1 = b"directory v1"
        V2 = b"directory v2 (refreshed)"
        gate304 = Gate()
        arrived = Flag()
        state = {"phase": 1}

        def route(req):
            inm = req.header("if-none-match")
            if inm == '"v1"':
                arrived.set()
                gate304.wait()
                return ResponseSpec(
                    304,
                    [
                        ("Vary", "Accept-Language"),
                        ("ETag", '"v1"'),
                        ("Cache-Control", "max-age=30"),
                        ("Date", http_date(self.clock())),
                    ],
                    b"",
                )
            if state["phase"] == 1:
                return ResponseSpec(
                    200,
                    [
                        ("Vary", "Accept-Language"),
                        ("Cache-Control", "max-age=30"),
                        ("ETag", '"v1"'),
                        ("Date", http_date(self.clock())),
                    ],
                    V1,
                )
            return ResponseSpec(
                200,
                [
                    ("Vary", "Accept-Language"),
                    ("Cache-Control", "max-age=300"),
                    ("ETag", '"v2"'),
                    ("Date", http_date(self.clock())),
                ],
                V2,
            )

        self.server.route("/dir", route)
        url = self.url("/dir")
        self.assertEqual(self.client.get(url, {"Accept-Language": "zh"}).body, V1)
        state["phase"] = 2
        self.clock.advance(40)  # v1 representation is stale now

        results, errors = [None], [None]

        def stale_call():
            try:
                results[0] = self.client.get(url, {"Accept-Language": "zh"})
            except BaseException as exc:  # noqa: BLE001
                errors[0] = exc

        t = threading.Thread(target=stale_call)
        t.start()
        arrived.wait()  # the conditional request is blocked inside the route
        refreshed = self.client.refresh(url, {"Accept-Language": "zh"})
        self.assertEqual(refreshed.body, V2)
        self.assertEqual(refreshed.source, Source.NETWORK)
        gate304.open()  # release the stale 304
        t.join(15)
        self.assertFalse(t.is_alive())
        self.assertIsNone(errors[0])
        # the original call must not fall back to the old body
        self.assertEqual(results[0].body, V2)
        # subsequent queries serve the refreshed representation, no new traffic
        again = self.client.get(url, {"Accept-Language": "zh"})
        self.assertEqual(again.body, V2)
        self.assertEqual(again.source, Source.CACHE)
        reqs = self.server.requests("/dir")
        self.assertEqual(len(reqs), 3)
        self.assertEqual(reqs[1].header("if-none-match"), '"v1"')
        self.assertIsNone(reqs[2].header("if-none-match"))


class TestNoStoreAndVaryStar(ClientTestCase):
    def test_no_store_not_shared_between_waiters(self):
        gate = Gate()

        def route(req):
            gate.wait()
            return ResponseSpec(
                200,
                [
                    ("Vary", "Accept-Language"),
                    ("Cache-Control", "no-store"),
                    ("Date", http_date(self.clock())),
                ],
                b"no-store body",
            )

        self.server.route("/ns", route)
        url = self.url("/ns")
        results, errors = [None, None], [None, None]

        def call(i):
            try:
                results[i] = self.client.get(url, {"Accept-Language": "zh"})
            except BaseException as exc:  # noqa: BLE001
                errors[i] = exc

        threads = [threading.Thread(target=call, args=(i,)) for i in range(2)]
        for t in threads:
            t.start()
        self.server.wait_for_requests(1, "/ns")
        gate.open()
        for t in threads:
            t.join(15)
        self.assertEqual(errors, [None, None])
        self.assertEqual([r.body for r in results], [b"no-store body", b"no-store body"])
        # the waiter could not share the leader's body: it fetched its own
        self.assertEqual(len(self.server.requests("/ns")), 2)
        # nothing was stored: a later call fetches again
        self.assertEqual(self.client.get(url, {"Accept-Language": "zh"}).body, b"no-store body")
        self.assertEqual(len(self.server.requests("/ns")), 3)

    def test_vary_star_not_shared(self):
        def route(req):
            return ResponseSpec(
                200,
                [("Vary", "*"), ("Cache-Control", "max-age=60"), ("Date", http_date(self.clock()))],
                b"star body",
            )

        self.server.route("/vs", route)
        url = self.url("/vs")
        results, errors = run_threads(
            [lambda: self.client.get(url), lambda: self.client.get(url)]
        )
        self.assertEqual(errors, [None, None])
        self.assertEqual([r.body for r in results], [b"star body", b"star body"])
        self.assertEqual(len(self.server.requests("/vs")), 2)
        self.client.get(url)
        self.assertEqual(len(self.server.requests("/vs")), 3)

    def test_304_missing_fields_keep_stored_values(self):
        def route(req):
            if req.header("if-none-match") == '"k1"':
                # no Cache-Control, no Age on the 304: stored values survive
                return ResponseSpec(304, [("Date", http_date(self.clock()))], b"")
            return ResponseSpec(
                200,
                [
                    ("Cache-Control", "max-age=10"),
                    ("ETag", '"k1"'),
                    ("Date", http_date(self.clock())),
                ],
                b"kept metadata",
            )

        self.server.route("/keep", route)
        url = self.url("/keep")
        self.assertEqual(self.client.get(url).body, b"kept metadata")
        self.clock.advance(10)  # stale -> revalidate
        res = self.client.get(url)
        self.assertEqual(res.source, Source.REVALIDATED)
        self.assertEqual(res.age.lifetime, 10.0)  # lifetime kept from the 200
        self.clock.advance(5)  # fresh again under the kept lifetime
        self.assertEqual(self.client.get(url).source, Source.CACHE)
        self.assertEqual(len(self.server.requests("/keep")), 2)

    def test_missing_date_uses_receipt_time(self):
        def route(req):
            if req.header("if-none-match") == '"nd1"':
                return ResponseSpec(
                    304, [("Cache-Control", "max-age=10")], b""
                )
            return ResponseSpec(
                200,
                [("Cache-Control", "max-age=10"), ("ETag", '"nd1"')],  # no Date
                b"no date body",
            )

        self.server.route("/nodate", route)
        url = self.url("/nodate")
        res = self.client.get(url)
        self.assertEqual(res.age.date_epoch, None)
        self.assertEqual(res.age.initial_age, 0.0)  # r - r, Age missing
        self.clock.advance(5)
        self.assertEqual(self.client.get(url).source, Source.CACHE)
        self.clock.advance(5)  # current_age == lifetime
        self.assertEqual(self.client.get(url).source, Source.REVALIDATED)
        self.assertEqual(len(self.server.requests("/nodate")), 2)


class TestLateArrivals(ClientTestCase):
    def test_late_response_cannot_resurrect_retired_rules(self):
        gate = Gate()
        arrived = Flag()

        def route(req):
            inm = req.header("if-none-match")
            if inm == '"zh1"':
                # the stale revalidation under the OLD rules: held back, then
                # answers with the retired vary set
                arrived.set()
                gate.wait()
                return ResponseSpec(
                    200,
                    [
                        ("Vary", "Accept-Language"),
                        ("Cache-Control", "max-age=30"),
                        ("ETag", '"zh1"'),
                        ("Date", http_date(self.clock())),
                    ],
                    b"old-rules body",
                )
            if req.header("x-region"):
                return ResponseSpec(
                    200,
                    [
                        ("Vary", "Accept-Language, X-Region"),
                        ("Cache-Control", "max-age=30"),
                        ("ETag", '"zh2"'),
                        ("Date", http_date(self.clock())),
                    ],
                    b"new-rules body r1",
                )
            return ResponseSpec(
                200,
                [
                    ("Vary", "Accept-Language"),
                    ("Cache-Control", "max-age=30"),
                    ("ETag", '"zh1"'),
                    ("Date", http_date(self.clock())),
                ],
                ZH_BODY,
            )

        self.server.route("/dir", route)
        url = self.url("/dir")
        self.assertEqual(self.client.get(url, {"Accept-Language": "zh"}).body, ZH_BODY)
        self.clock.advance(40)  # stale to go

        held = {}

        def stale_call():
            try:
                held["res"] = self.client.get(url, {"Accept-Language": "zh"})
            except BaseException as exc:  # noqa: BLE001
                held["err"] = exc

        t = threading.Thread(target=stale_call)
        t.start()
        arrived.wait()
        # refresh publishes the new vary rules while the old revalidation
        # is still on the wire
        ref = self.client.refresh(url, {"Accept-Language": "zh", "X-Region": "r1"})
        self.assertEqual(ref.body, b"new-rules body r1")
        gate.open()
        t.join(15)
        self.assertFalse(t.is_alive())
        self.assertNotIn("err", held)
        # the late response went to its caller but was not committed
        self.assertEqual(held["res"].body, b"old-rules body")
        # the retired rules were not resurrected: a no-region caller must
        # fetch under the new rules, not hit the old-rules body
        res = self.client.get(url, {"Accept-Language": "zh"})
        self.assertEqual(res.body, ZH_BODY)
        self.assertEqual(res.source, Source.NETWORK)
        # and the refreshed representation is a cache hit
        self.assertEqual(
            self.client.get(url, {"Accept-Language": "zh", "X-Region": "r1"}).source,
            Source.CACHE,
        )


class TestNetworkTimeout(ClientTestCase):
    def test_timeout_fails_and_releases_waiters(self):
        release = Gate()

        def route(req):
            release.wait()
            return ResponseSpec(200, [], b"too late")

        self.server.route("/slow", route)
        self.client.close()
        self.client = self.new_client(timeout=0.3)
        url = self.url("/slow")
        results, errors = run_threads(
            [lambda: self.client.get(url), lambda: self.client.get(url)]
        )
        self.assertIsInstance(errors[0], NetworkError)
        self.assertIsInstance(errors[1], NetworkError)
        release.open()
        # nothing cached as success: a later call tries the network again
        self.server.route("/slow", lambda req: ResponseSpec(200, [], b"recovered"))
        self.assertEqual(self.client.get(url).body, b"recovered")


class TestFreshnessModel(ClientTestCase):
    def test_age_boundary_equality_is_stale(self):
        def route(req):
            if req.header("if-none-match") == '"a1"':
                return ResponseSpec(
                    304,
                    [
                        ("Cache-Control", "max-age=10"),
                        ("Age", "0"),
                        ("Date", http_date(self.clock())),
                    ],
                    b"",
                )
            return ResponseSpec(
                200,
                [
                    ("Cache-Control", "max-age=10"),
                    ("Age", "6"),
                    ("ETag", '"a1"'),
                    ("Date", http_date(self.clock())),
                ],
                b"aged body",
            )

        self.server.route("/a", route)
        url = self.url("/a")
        res = self.client.get(url)
        self.assertEqual(res.body, b"aged body")
        self.assertEqual(res.age.initial_age, 6.0)
        self.clock.advance(3)  # current_age = 9 < 10: fresh
        res = self.client.get(url)
        self.assertEqual(res.source, Source.CACHE)
        self.assertEqual(res.age.current_age, 9.0)
        self.assertEqual(len(self.server.requests("/a")), 1)
        self.clock.advance(1)  # current_age = 10 == lifetime: stale
        res = self.client.get(url)
        self.assertEqual(res.source, Source.REVALIDATED)
        self.assertTrue(res.validated)
        self.assertEqual(res.network_status, 304)
        self.assertEqual(res.body, b"aged body")
        self.assertEqual(len(self.server.requests("/a")), 2)
        self.assertEqual(self.server.requests("/a")[1].header("if-none-match"), '"a1"')

    def test_no_cache_validates_every_time(self):
        def route(req):
            if req.header("if-none-match") == '"nc1"':
                return ResponseSpec(
                    304,
                    [("Cache-Control", "no-cache"), ("Date", http_date(self.clock()))],
                    b"",
                )
            return ResponseSpec(
                200,
                [
                    ("Cache-Control", "no-cache"),
                    ("ETag", '"nc1"'),
                    ("Date", http_date(self.clock())),
                ],
                b"no-cache body",
            )

        self.server.route("/nc", route)
        url = self.url("/nc")
        self.assertEqual(self.client.get(url).body, b"no-cache body")
        r2 = self.client.get(url)
        r3 = self.client.get(url)
        self.assertEqual((r2.body, r3.body), (b"no-cache body", b"no-cache body"))
        self.assertEqual((r2.source, r3.source), (Source.REVALIDATED, Source.REVALIDATED))
        reqs = self.server.requests("/nc")
        self.assertEqual(len(reqs), 3)
        self.assertEqual([r.header("if-none-match") for r in reqs], [None, '"nc1"', '"nc1"'])

    def test_304_metadata_changes_lifetime(self):
        def route(req):
            if req.header("if-none-match") == '"m1"':
                return ResponseSpec(
                    304,
                    [
                        ("Cache-Control", "max-age=100"),
                        ("Age", "0"),
                        ("Date", http_date(self.clock())),
                    ],
                    b"",
                )
            return ResponseSpec(
                200,
                [
                    ("Cache-Control", "max-age=5"),
                    ("ETag", '"m1"'),
                    ("Date", http_date(self.clock())),
                ],
                b"mutable lifetime",
            )

        self.server.route("/m", route)
        url = self.url("/m")
        self.assertEqual(self.client.get(url).body, b"mutable lifetime")
        self.clock.advance(10)
        res = self.client.get(url)
        self.assertEqual(res.source, Source.REVALIDATED)
        self.assertEqual(res.age.lifetime, 100.0)
        self.assertEqual(res.body, b"mutable lifetime")
        self.clock.advance(50)  # fresh under the new lifetime
        res = self.client.get(url)
        self.assertEqual(res.source, Source.CACHE)
        self.assertEqual(len(self.server.requests("/m")), 2)

    def test_s_maxage_wins_over_max_age(self):
        def route(req):
            if req.header("if-none-match") == '"sm1"':
                return ResponseSpec(
                    304,
                    [("Cache-Control", "s-maxage=100"), ("Date", http_date(self.clock()))],
                    b"",
                )
            return ResponseSpec(
                200,
                [
                    ("Cache-Control", "s-maxage=100, max-age=5"),
                    ("ETag", '"sm1"'),
                    ("Date", http_date(self.clock())),
                ],
                b"s-maxage body",
            )

        self.server.route("/sm", route)
        url = self.url("/sm")
        self.assertEqual(self.client.get(url).body, b"s-maxage body")
        self.clock.advance(50)  # past max-age, within s-maxage
        self.assertEqual(self.client.get(url).source, Source.CACHE)
        self.assertEqual(len(self.server.requests("/sm")), 1)
        self.clock.advance(60)  # past s-maxage too
        self.assertEqual(self.client.get(url).source, Source.REVALIDATED)
        self.assertEqual(len(self.server.requests("/sm")), 2)

    def test_no_explicit_lifetime_requires_validation(self):
        def route(req):
            if req.header("if-none-match") == '"nl1"':
                return ResponseSpec(304, [("Date", http_date(self.clock()))], b"")
            return ResponseSpec(
                200, [("ETag", '"nl1"'), ("Date", http_date(self.clock()))], b"no lifetime"
            )

        self.server.route("/nl", route)
        url = self.url("/nl")
        self.assertEqual(self.client.get(url).body, b"no lifetime")
        self.assertEqual(self.client.get(url).source, Source.REVALIDATED)
        self.assertEqual(len(self.server.requests("/nl")), 2)

    def test_stale_without_etag_replaced_unconditionally(self):
        state = {"n": 0}

        def route(req):
            state["n"] += 1
            return ResponseSpec(
                200,
                [("Cache-Control", "max-age=5"), ("Date", http_date(self.clock()))],
                f"body {state['n']}".encode(),
            )

        self.server.route("/ne", route)
        url = self.url("/ne")
        self.assertEqual(self.client.get(url).body, b"body 1")
        self.clock.advance(10)
        self.assertEqual(self.client.get(url).body, b"body 2")
        reqs = self.server.requests("/ne")
        self.assertEqual(len(reqs), 2)
        self.assertIsNone(reqs[1].header("if-none-match"))

    def test_reopened_client_with_earlier_clock_validates(self):
        def route(req):
            if req.header("if-none-match") == '"r1"':
                return ResponseSpec(
                    304,
                    [("Cache-Control", "max-age=1000"), ("Date", http_date(self.clock()))],
                    b"",
                )
            return ResponseSpec(
                200,
                [
                    ("Cache-Control", "max-age=1000"),
                    ("ETag", '"r1"'),
                    ("Date", http_date(self.clock())),
                ],
                b"reopen body",
            )

        self.server.route("/ro", route)
        url = self.url("/ro")
        self.assertEqual(self.client.get(url).body, b"reopen body")
        self.client.close()

        # reopen with the same clock: fresh cache hit, no traffic
        self.client = self.new_client()
        res = self.client.get(url)
        self.assertEqual(res.source, Source.CACHE)
        self.assertEqual(res.body, b"reopen body")
        self.assertEqual(len(self.server.requests("/ro")), 1)
        self.client.close()

        # reopen with a clock earlier than the stored r: must validate
        early = FakeClock(self.clock.t - 10)
        self.client = self.new_client(clock=early)
        res = self.client.get(url)
        self.assertEqual(res.source, Source.REVALIDATED)
        self.assertEqual(res.body, b"reopen body")
        self.assertEqual(len(self.server.requests("/ro")), 2)


class Test304ProtocolViolations(ClientTestCase):
    def test_304_without_precondition_is_protocol_error(self):
        def route(req):
            return ResponseSpec(304, [("Date", http_date(self.clock()))], b"")

        self.server.route("/bad", route)
        with self.assertRaises(ProtocolError):
            self.client.get(self.url("/bad"))
        reqs = self.server.requests("/bad")
        self.assertEqual(len(reqs), 1)
        self.assertIsNone(reqs[0].header("if-none-match"))

    def test_304_with_wrong_etag_keeps_cache(self):
        state = {"phase": 1}

        def route(req):
            if state["phase"] == 1:
                return ResponseSpec(
                    200,
                    [
                        ("Cache-Control", "max-age=5"),
                        ("ETag", '"w1"'),
                        ("Date", http_date(self.clock())),
                    ],
                    b"original body",
                )
            if state["phase"] == 2:
                return ResponseSpec(
                    304,
                    [("ETag", '"w2"'), ("Date", http_date(self.clock()))],
                    b"",
                )
            return ResponseSpec(
                304,
                [("Cache-Control", "max-age=100"), ("Date", http_date(self.clock()))],
                b"",
            )

        self.server.route("/w", route)
        url = self.url("/w")
        self.assertEqual(self.client.get(url).body, b"original body")
        self.clock.advance(10)
        state["phase"] = 2
        with self.assertRaises(ProtocolError):
            self.client.get(url)
        # the previous cache survived: the next validation still offers "w1"
        state["phase"] = 3
        res = self.client.get(url)
        self.assertEqual(res.body, b"original body")
        self.assertEqual(res.source, Source.REVALIDATED)
        inms = [r.header("if-none-match") for r in self.server.requests("/w")]
        self.assertEqual(inms, [None, '"w1"', '"w1"'])

    def test_304_with_different_vary_is_protocol_error(self):
        state = {"phase": 1}

        def route(req):
            if state["phase"] == 1:
                return ResponseSpec(
                    200,
                    [
                        ("Vary", "Accept-Language"),
                        ("Cache-Control", "max-age=5"),
                        ("ETag", '"v1"'),
                        ("Date", http_date(self.clock())),
                    ],
                    ZH_BODY,
                )
            return ResponseSpec(
                304,
                [("Vary", "Accept-Language, X-Region"), ("Date", http_date(self.clock()))],
                b"",
            )

        self.server.route("/v", route)
        url = self.url("/v")
        self.assertEqual(
            self.client.get(url, {"Accept-Language": "zh"}).body, ZH_BODY
        )
        self.clock.advance(10)
        state["phase"] = 2
        with self.assertRaises(ProtocolError):
            self.client.get(url, {"Accept-Language": "zh"})

    def test_304_with_vary_star_is_protocol_error(self):
        state = {"phase": 1}

        def route(req):
            if state["phase"] == 1:
                return ResponseSpec(
                    200,
                    [
                        ("Cache-Control", "max-age=5"),
                        ("ETag", '"vs1"'),
                        ("Date", http_date(self.clock())),
                    ],
                    b"vary-star-304 body",
                )
            return ResponseSpec(
                304,
                [("Vary", "*"), ("Date", http_date(self.clock()))],
                b"",
            )

        self.server.route("/vs304", route)
        url = self.url("/vs304")
        self.assertEqual(self.client.get(url).body, b"vary-star-304 body")
        self.clock.advance(10)
        state["phase"] = 2
        with self.assertRaises(ProtocolError):
            self.client.get(url)

    def test_304_no_store_returns_body_then_deletes(self):
        state = {"phase": 1}

        def route(req):
            if state["phase"] == 1:
                return ResponseSpec(
                    200,
                    [
                        ("Cache-Control", "max-age=5"),
                        ("ETag", '"d1"'),
                        ("Date", http_date(self.clock())),
                    ],
                    b"doomed body",
                )
            if state["phase"] == 2:
                return ResponseSpec(
                    304,
                    [("Cache-Control", "no-store"), ("Date", http_date(self.clock()))],
                    b"",
                )
            return ResponseSpec(
                200,
                [
                    ("Cache-Control", "max-age=60"),
                    ("ETag", '"d2"'),
                    ("Date", http_date(self.clock())),
                ],
                b"replacement body",
            )

        self.server.route("/d", route)
        url = self.url("/d")
        self.assertEqual(self.client.get(url).body, b"doomed body")
        self.clock.advance(10)
        state["phase"] = 2
        res = self.client.get(url)  # validated body is still returned
        self.assertEqual(res.body, b"doomed body")
        self.assertEqual(res.source, Source.REVALIDATED)
        state["phase"] = 3
        res = self.client.get(url)  # representation was deleted: unconditional
        self.assertEqual(res.body, b"replacement body")
        reqs = self.server.requests("/d")
        self.assertEqual(len(reqs), 3)
        self.assertIsNone(reqs[2].header("if-none-match"))


class TestTransportFailures(ClientTestCase):
    def test_truncated_response_fails_and_retry_works(self):
        state = {"phase": 1}
        gate = Gate()

        def route(req):
            if state["phase"] == 1:
                gate.wait()
                return ResponseSpec(
                    200,
                    [("Cache-Control", "max-age=60"), ("Date", http_date(self.clock()))],
                    b"complete body that will be cut",
                    truncate_at=5,
                )
            return ResponseSpec(
                200,
                [("Cache-Control", "max-age=60"), ("Date", http_date(self.clock()))],
                b"complete body that will be cut",
            )

        self.server.route("/t", route)
        url = self.url("/t")
        results, errors = [None, None], [None, None]

        def call(i):
            try:
                results[i] = self.client.get(url)
            except BaseException as exc:  # noqa: BLE001
                errors[i] = exc

        threads = [threading.Thread(target=call, args=(i,)) for i in range(2)]
        for t in threads:
            t.start()
        self.server.wait_for_requests(1, "/t")
        gate.open()
        for t in threads:
            t.join(15)
        # the shared failed fetch woke both waiters with the same failure
        self.assertIsInstance(errors[0], NetworkError)
        self.assertIsInstance(errors[1], NetworkError)
        self.assertEqual(len(self.server.requests("/t")), 1)
        # nothing was cached as success: the next call fetches again
        state["phase"] = 2
        res = self.client.get(url)
        self.assertEqual(res.body, b"complete body that will be cut")
        self.assertEqual(len(self.server.requests("/t")), 2)

    def test_oversize_response_rejected(self):
        client = self.new_client(max_bytes=16)
        self.client.close()
        self.client = client

        def route(req):
            return ResponseSpec(200, [("Date", http_date(self.clock()))], b"x" * 100)

        self.server.route("/big", route)
        with self.assertRaises(NetworkError):
            self.client.get(self.url("/big"))
        with self.assertRaises(NetworkError):
            self.client.get(self.url("/big"))
        self.assertEqual(len(self.server.requests("/big")), 2)

    def test_other_statuses_pass_through_unstored(self):
        def route404(req):
            return ResponseSpec(404, [("Date", http_date(self.clock()))], b"nope")

        def route301(req):
            return ResponseSpec(
                301,
                [("Location", "/elsewhere"), ("Date", http_date(self.clock()))],
                b"",
            )

        self.server.route("/missing", route404)
        self.server.route("/redir", route301)
        res = self.client.get(self.url("/missing"))
        self.assertEqual(res.status, 404)
        self.assertEqual(res.network_status, 404)
        self.assertEqual(res.body, b"nope")
        self.assertEqual(res.source, Source.NETWORK)
        self.client.get(self.url("/missing"))
        self.assertEqual(len(self.server.requests("/missing")), 2)
        res = self.client.get(self.url("/redir"))
        self.assertEqual(res.status, 301)  # never followed
        self.assertEqual(len(self.server.requests("/redir")), 1)

    def test_content_encoding_rejected(self):
        def route(req):
            return ResponseSpec(
                200,
                [("Content-Encoding", "gzip"), ("Date", http_date(self.clock()))],
                b"\x1f\x8b garbage",
            )

        self.server.route("/gz", route)
        with self.assertRaises(ProtocolError):
            self.client.get(self.url("/gz"))

    def test_conflicting_content_length_rejected(self):
        def route(req):
            return ResponseSpec(
                200,
                [("Content-Length", "5"), ("Content-Length", "6")],
                b"12345",
            )

        self.server.route("/cl", route)
        with self.assertRaises(ProtocolError):
            self.client.get(self.url("/cl"))


class TestProtocolMetadataErrors(ClientTestCase):
    BAD_RESPONSES = {
        "negative max-age": [("Cache-Control", "max-age=-1")],
        "non-numeric max-age": [("Cache-Control", "max-age=abc")],
        "conflicting max-age": [("Cache-Control", "max-age=5, max-age=6")],
        "unquoted etag": [("Cache-Control", "max-age=60"), ("ETag", "abc")],
        "bad age": [("Cache-Control", "max-age=60"), ("Age", "abc")],
        "bad date": [("Cache-Control", "max-age=60"), ("Date", "not a date")],
    }

    def test_invalid_metadata_keeps_previous_cache(self):
        for i, (name, bad_headers) in enumerate(self.BAD_RESPONSES.items()):
            with self.subTest(case=name):
                path = f"/bad{i}"
                state = {"phase": 1}

                def route(req, state=state, bad_headers=bad_headers):
                    if state["phase"] == 1:
                        return ResponseSpec(
                            200,
                            [
                                ("Cache-Control", "max-age=5"),
                                ("ETag", '"p1"'),
                                ("Date", http_date(self.clock())),
                            ],
                            b"good body",
                        )
                    if state["phase"] == 2:
                        headers = list(bad_headers)
                        if not any(n.lower() == "date" for n, _ in headers):
                            headers.append(("Date", http_date(self.clock())))
                        return ResponseSpec(200, headers, b"bad body")
                    return ResponseSpec(
                        304,
                        [("Cache-Control", "max-age=100"), ("Date", http_date(self.clock()))],
                        b"",
                    )

                self.server.route(path, route)
                url = self.url(path)
                self.assertEqual(self.client.get(url).body, b"good body")
                self.clock.advance(10)
                state["phase"] = 2
                with self.assertRaises(ProtocolError):
                    self.client.get(url)
                # previous cache intact: revalidation still offers "p1"
                state["phase"] = 3
                res = self.client.get(url)
                self.assertEqual(res.body, b"good body")
                self.assertEqual(res.source, Source.REVALIDATED)
                inms = [r.header("if-none-match") for r in self.server.requests(path)]
                self.assertEqual(inms, [None, '"p1"', '"p1"'])


class TestRefresh(ClientTestCase):
    def test_refresh_is_unconditional_and_parallel(self):
        gate = Gate()

        def route(req):
            gate.wait()
            return ResponseSpec(
                200,
                [
                    ("Vary", "Accept-Language"),
                    ("Cache-Control", "max-age=1000"),
                    ("ETag", '"f1"'),
                    ("Date", http_date(self.clock())),
                ],
                b"fresh body",
            )

        self.server.route("/f", route)
        url = self.url("/f")
        results, errors = [None], [None]

        def normal():
            try:
                results[0] = self.client.get(url, {"Accept-Language": "zh"})
            except BaseException as exc:  # noqa: BLE001
                errors[0] = exc

        t = threading.Thread(target=normal)
        t.start()
        self.server.wait_for_requests(1, "/f")
        # refresh does not join the in-flight normal fetch: it runs in parallel
        refresh_result = [None]

        def do_refresh():
            refresh_result[0] = self.client.refresh(url, {"Accept-Language": "zh"})

        rt = threading.Thread(target=do_refresh)
        rt.start()
        self.server.wait_for_requests(2, "/f")
        gate.open()
        t.join(15)
        rt.join(15)
        self.assertIsNone(errors[0])
        self.assertEqual(results[0].body, b"fresh body")
        self.assertEqual(refresh_result[0].body, b"fresh body")
        reqs = self.server.requests("/f")
        self.assertEqual(len(reqs), 2)
        self.assertTrue(all(r.header("if-none-match") is None for r in reqs))
        # refresh published a fresh representation: no more traffic
        self.assertEqual(self.client.get(url, {"Accept-Language": "zh"}).source, Source.CACHE)
        self.assertEqual(len(self.server.requests("/f")), 2)


class TestRequestValidation(ClientTestCase):
    def test_forbidden_headers_rejected(self):
        for headers in [
            {"Authorization": "Bearer x"},
            {"Cookie": "a=b"},
            {"Range": "bytes=0-10"},
            {"If-None-Match": '"x"'},
            {"If-Modified-Since": "yesterday"},
            {"Host": "evil"},
            [("X-A", "1"), ("x-a", "2")],  # duplicate names, any case
            {"X-Bad": "line1\r\nInjected: yes"},
        ]:
            with self.subTest(headers=headers):
                with self.assertRaises(RequestRejected):
                    self.client.get(self.url("/dir"), headers)
        self.assertEqual(self.server.requests(), [])

    def test_bad_urls_rejected(self):
        for url in [
            "http://user:pass@example.com/",
            "http://user@example.com/",
            "ftp://example.com/",
            "example.com/no-scheme",
            "http://example.com:99999/",
        ]:
            with self.subTest(url=url):
                with self.assertRaises(RequestRejected):
                    self.client.get(url)

    def test_missing_vs_empty_header_are_distinct(self):
        def route(req):
            value = req.header("x-lang")
            body = b"missing" if value is None else (b"empty" if value == "" else value.encode())
            return ResponseSpec(
                200,
                [("Vary", "X-Lang"), ("Cache-Control", "max-age=60"), ("Date", http_date(self.clock()))],
                body,
            )

        self.server.route("/lang", route)
        url = self.url("/lang")
        self.assertEqual(self.client.get(url).body, b"missing")
        self.assertEqual(self.client.get(url, {"X-Lang": ""}).body, b"empty")
        self.assertEqual(len(self.server.requests("/lang")), 2)
        # both cached distinctly
        self.assertEqual(self.client.get(url).body, b"missing")
        self.assertEqual(self.client.get(url, {"X-Lang": ""}).body, b"empty")
        self.assertEqual(len(self.server.requests("/lang")), 2)

    def test_weak_etag_sent_verbatim(self):
        def route(req):
            if req.header("if-none-match") is not None:
                return ResponseSpec(
                    304,
                    [("Cache-Control", "max-age=60"), ("Date", http_date(self.clock()))],
                    b"",
                )
            return ResponseSpec(
                200,
                [
                    ("Cache-Control", "max-age=5"),
                    ("ETag", 'W/"weak1"'),
                    ("Date", http_date(self.clock())),
                ],
                b"weak body",
            )

        self.server.route("/weak", route)
        url = self.url("/weak")
        self.assertEqual(self.client.get(url).body, b"weak body")
        self.clock.advance(10)
        self.assertEqual(self.client.get(url).body, b"weak body")
        reqs = self.server.requests("/weak")
        self.assertEqual(len(reqs), 2)
        self.assertEqual(reqs[1].header("if-none-match"), 'W/"weak1"')

    def test_query_and_fragment_identity(self):
        def route(req):
            return ResponseSpec(
                200,
                [("Cache-Control", "max-age=60"), ("Date", http_date(self.clock()))],
                f"body for {req.path}".encode(),
            )

        self.server.route("/q?x=1", route)
        self.server.route("/q?x=2", route)
        base = self.url("/q")
        r1 = self.client.get(base + "?x=1#section")
        r2 = self.client.get(base + "?x=2")
        self.assertEqual(r1.body, b"body for /q?x=1")
        self.assertEqual(r2.body, b"body for /q?x=2")
        self.assertEqual(len(self.server.requests()), 2)
        # fragment is stripped: same identity, cache hit
        r3 = self.client.get(base + "?x=1#other")
        self.assertEqual(r3.body, b"body for /q?x=1")
        self.assertEqual(r3.source, Source.CACHE)
        self.assertEqual(len(self.server.requests()), 2)


class TestUrlNormalization(unittest.TestCase):
    def test_normalization_rules(self):
        self.assertEqual(
            normalize_url("HTTP://Example.COM:80/a?b=c#frag")[0],
            "http://example.com/a?b=c",
        )
        self.assertEqual(
            normalize_url("https://Example.com:443/a")[0], "https://example.com/a"
        )
        self.assertEqual(
            normalize_url("http://example.com:8080/a")[0], "http://example.com:8080/a"
        )
        # query is kept verbatim, never rewritten
        self.assertEqual(
            normalize_url("http://example.com/p?b=2&a=1%20")[0],
            "http://example.com/p?b=2&a=1%20",
        )
        self.assertEqual(
            normalize_url("http://example.com")[0], "http://example.com/"
        )


if __name__ == "__main__":
    unittest.main()
