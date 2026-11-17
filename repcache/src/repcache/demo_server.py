"""A small local bilingual HTTP service used by the demo and the tests.

The server keeps a thread-safe *receive log* of every request it got
(including conditional headers), and routes may block on *gates* so tests
can control interleavings precisely.  Assertions about network traffic are
always made against this server-side log, never against client counters.
No route ever leaves localhost.
"""

from __future__ import annotations

import http.server
import threading
import time
from email.utils import formatdate
from typing import Callable, Dict, List, Optional, Tuple


def http_date(epoch: float) -> str:
    return formatdate(epoch, usegmt=True)


class ReceivedRequest:
    __slots__ = ("method", "path", "headers", "when")

    def __init__(self, method: str, path: str, headers: List[Tuple[str, str]], when: float):
        self.method = method
        self.path = path
        self.headers = list(headers)
        self.when = when

    def header(self, name: str) -> Optional[str]:
        lname = name.lower()
        for n, v in self.headers:
            if n.lower() == lname:
                return v
        return None

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"ReceivedRequest({self.method} {self.path} {self.headers!r})"


class ResponseSpec:
    """What a route should send back.

    truncate_at: send only this many bytes of the body while declaring the
    full Content-Length, then close — simulates 断流 (a truncated stream).
    """

    def __init__(
        self,
        status: int,
        headers: Tuple[Tuple[str, str], ...] = (),
        body: bytes = b"",
        truncate_at: Optional[int] = None,
    ):
        self.status = status
        self.headers = list(headers)
        self.body = body
        self.truncate_at = truncate_at


class Gate:
    """A closable gate routes wait on; tests open it to release responses."""

    def __init__(self) -> None:
        self._event = threading.Event()

    def wait(self, timeout: float = 10.0) -> None:
        if not self._event.wait(timeout):
            raise RuntimeError("gate timed out (client likely serialized requests)")

    def open(self) -> None:
        self._event.set()

    def close(self) -> None:
        self._event.clear()


class Flag:
    """A settable event routes use to tell the test they were reached."""

    def __init__(self) -> None:
        self._event = threading.Event()

    def set(self) -> None:
        self._event.set()

    def wait(self, timeout: float = 10.0) -> None:
        if not self._event.wait(timeout):
            raise RuntimeError("expected request never reached the server")


class DemoServer:
    """Threading HTTP server with a receive log and programmable routes."""

    def __init__(self, clock: Optional[Callable[[], float]] = None):
        self.clock = clock or time.time
        self._routes: Dict[str, Callable[[ReceivedRequest], ResponseSpec]] = {}
        self._log: List[ReceivedRequest] = []
        self._log_lock = threading.Lock()
        handler_cls = self._make_handler()
        self._httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
        self._httpd.daemon_threads = True
        self._thread: Optional[threading.Thread] = None

    # -- lifecycle ---------------------------------------------------------

    @property
    def port(self) -> int:
        return self._httpd.server_address[1]

    def url(self, path: str) -> str:
        return f"http://127.0.0.1:{self.port}{path}"

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._httpd.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def __enter__(self) -> "DemoServer":
        self.start()
        return self

    def __exit__(self, *exc) -> None:
        self.stop()

    # -- routes and log -----------------------------------------------------

    def route(self, path: str, fn: Callable[[ReceivedRequest], ResponseSpec]) -> None:
        self._routes[path] = fn

    def requests(self, path: Optional[str] = None) -> List[ReceivedRequest]:
        with self._log_lock:
            log = list(self._log)
        if path is not None:
            log = [r for r in log if r.path == path]
        return log

    def wait_for_requests(self, count: int, path: Optional[str] = None, timeout: float = 10.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if len(self.requests(path)) >= count:
                return
            time.sleep(0.005)
        raise RuntimeError(f"server received fewer than {count} requests for {path!r}")

    def _record(self, req: ReceivedRequest) -> None:
        with self._log_lock:
            self._log.append(req)

    # -- handler ------------------------------------------------------------

    def _make_handler(self):
        server = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):  # silence
                pass

            def do_GET(self):
                req = ReceivedRequest(
                    "GET", self.path, list(self.headers.items()), server.clock()
                )
                server._record(req)
                fn = server._routes.get(self.path)
                if fn is None:
                    spec = ResponseSpec(404, (("Content-Type", "text/plain"),), b"no such route")
                else:
                    try:
                        spec = fn(req)
                    except Exception as exc:  # surface route bugs as 500
                        spec = ResponseSpec(500, (), f"route error: {exc}".encode())
                try:
                    self._send(spec)
                except (BrokenPipeError, ConnectionResetError):
                    pass  # client timed out or went away mid-response

            def _send(self, spec: ResponseSpec):
                body = spec.body
                # send_response_only: no automatic Server/Date headers, so the
                # route fully controls Date (needed for the injected clock).
                self.send_response_only(spec.status)
                header_names = {n.lower() for n, _ in spec.headers}
                for name, value in spec.headers:
                    self.send_header(name, value)
                if spec.status not in (204, 304) and "content-length" not in header_names:
                    self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                if spec.truncate_at is not None:
                    self.wfile.write(body[: spec.truncate_at])
                elif body and spec.status not in (204, 304):
                    self.wfile.write(body)
                self.close_connection = True

        return Handler


def directory_route(
    server: DemoServer,
    *,
    vary: Tuple[str, ...] = ("accept-language",),
    lifetime: int = 30,
    etag: Optional[str] = '"dir-v1"',
    bodies: Optional[Dict[str, bytes]] = None,
    gate: Optional[Gate] = None,
    cache_control: Optional[str] = None,
) -> Callable[[ReceivedRequest], ResponseSpec]:
    """A ready-made bilingual directory route for the demo.

    The body is selected by the Accept-Language request header; revalidation
    is supported through the configured ETag.
    """

    bodies = bodies or {
        "zh": "目录\n- 公告\n- 产品\n".encode(),
        "en": b"directory\n- announcements\n- products\n",
    }

    def handle(req: ReceivedRequest) -> ResponseSpec:
        if gate is not None:
            gate.wait()
        lang = req.header("accept-language") or "en"
        body = bodies.get(lang, bodies["en"])
        headers = [
            ("Content-Type", "text/plain; charset=utf-8"),
            ("Vary", ", ".join(vary)),
            ("Cache-Control", cache_control or f"max-age={lifetime}"),
            ("Date", http_date(server.clock())),
        ]
        if etag is not None:
            headers.append(("ETag", etag))
        if req.header("if-none-match") is not None and req.header("if-none-match") == etag:
            return ResponseSpec(304, headers, b"")
        return ResponseSpec(200, headers, body)

    return handle
