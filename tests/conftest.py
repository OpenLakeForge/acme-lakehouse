"""Run the connectors through the platform's own Bronze loader, offline.

The loader (`libs.bronze_dlt`) ships in the OpenLakeForge distribution, not in
this repository, exactly as it does in the project-code image. It is appended
to the path after this repository so `lakehouse_code` stays ours and never the
distribution's demo project.
"""

from __future__ import annotations

import json
import os
import sys
import threading
from collections.abc import Iterator
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest
from olf.common.distribution import runtime_layout

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.append(str(runtime_layout({**os.environ, "OLF_DISTRIBUTION_MODE": "installed"}).distribution_root))

LAKEHOUSE_CODE = ROOT / "lakehouse_code"


@dataclass
class FakeOpenFoodFacts:
    """A controlled stand-in for one Open Food Facts server."""

    products: list[dict[str, Any]] = field(default_factory=list)
    grades: dict[str, int] = field(default_factory=lambda: {"a": 10, "e": 4})
    requests: list[dict[str, Any]] = field(default_factory=list)
    url: str = ""

    def upsert(self, code: str, modified: int, **fields: Any) -> None:
        self.products = [product for product in self.products if product["code"] != code]
        self.products.append({"code": code, "last_modified_t": modified, "product_name": code, **fields})

    def search(self, query: dict[str, list[str]]) -> dict[str, Any]:
        size, page = int(query["page_size"][0]), int(query["page"][0])
        ordered = sorted(self.products, key=lambda product: product["last_modified_t"], reverse=True)
        return {"count": len(ordered), "products": ordered[(page - 1) * size : page * size]}


def _serve(api: FakeOpenFoodFacts) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - http.server's name
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            api.requests.append({"path": parsed.path, "query": query, "user_agent": self.headers["User-Agent"]})
            if parsed.path == "/api/v2/search":
                body = api.search(query)
            elif parsed.path == "/facets/nutrition-grades.json":
                body = {"tags": [{"id": grade, "products": count} for grade, count in api.grades.items()]}
            else:
                self.send_error(404)
                return
            payload = json.dumps(body).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_args: Any) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    api.url = f"http://127.0.0.1:{server.server_address[1]}"
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


@pytest.fixture
def off_servers() -> Iterator[dict[str, FakeOpenFoodFacts]]:
    """One server per stage, as DEV reads staging and PROD production."""
    apis = {"dev": FakeOpenFoodFacts(), "prod": FakeOpenFoodFacts()}
    servers = [_serve(api) for api in apis.values()]
    yield apis
    for server in servers:
        server.shutdown()


@pytest.fixture
def stage_env(monkeypatch: pytest.MonkeyPatch):  # noqa: ANN201
    """Set what a stage's run pod receives: its stage and its source bindings."""

    def apply(stage: str, bindings: dict[str, str]) -> None:
        for name in list(os.environ):
            if name.startswith("SOURCES__"):
                monkeypatch.delenv(name)
        monkeypatch.setenv("OPENLAKEFORGE_STAGE", stage)
        for name, value in bindings.items():
            monkeypatch.setenv(name, value)

    return apply


def bronze_rows(bucket: Path, source: str, resource: str) -> list[dict[str, Any]]:
    import pyarrow.parquet as pq

    rows: list[dict[str, Any]] = []
    for path in sorted((bucket / source / resource).glob("*.parquet")):
        rows.extend(pq.read_table(path).to_pylist())
    return rows


def bronze_files(bucket: Path, source: str, resource: str) -> list[str]:
    return sorted(path.name for path in (bucket / source / resource).glob("*.parquet"))
