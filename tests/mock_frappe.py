"""A stand-in for Frappe's lake.screenings endpoint.

Exists to test the *Dagster half* end to end without a database: that the
paginator reads `message.pages`, the selector reads `message.data`, the run's
partition window arrives as dateStart/dateEnd, and auth is sent.

It mimics the real endpoint's contract exactly: Frappe's `message` envelope,
a half-open [dateStart, dateEnd) window, `modified asc, name asc` ordering,
and fixed-width millisecond UTC cursors.

Every request is appended to requests.jsonl so the test can assert on what
Dagster actually sent.
"""

import datetime as dt
import json
import math
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

PORT = 8009
PATH = "/api/method/data_capture_forms.evd_screening.lake.screenings"
EXPECTED_AUTH = "token testkey:testsecret"
REQUEST_LOG = sys.argv[1] if len(sys.argv) > 1 else "requests.jsonl"

UTC = dt.timezone.utc


def iso_z(d: dt.datetime) -> str:
    d = d.astimezone(UTC)
    return d.strftime("%Y-%m-%dT%H:%M:%S.") + f"{d.microsecond // 1000:03d}Z"


def build_fixtures():
    """450 screenings on 2026-07-06 (forces 3 pages at limit=200) + 50 on 07-07.

    The 07-07 rows must never appear in the 07-06 partition; that is the
    half-open window assertion.
    """
    rows = []
    for day, count in ((6, 450), (7, 50)):
        midnight = dt.datetime(2026, 7, day, 0, 0, 0, tzinfo=UTC)
        for i in range(count):
            # spread across the day; ms precision, exactly like the real endpoint
            modified = midnight + dt.timedelta(seconds=i * 137, milliseconds=i % 1000)
            rows.append(
                {
                    "screeningIdentifier": f"POE-2026-{day:02d}{i:04d}",
                    "modified": iso_z(modified),
                    "schemaVersion": "1.0.0",
                    "phiIncluded": False,
                    "subjectPseudoId": f"pseudo{i:06d}",
                    "poeName": "Jomo Kenyatta International Airport",
                    "poeType": "airport",
                    "screeningState": "Suspected" if i % 7 == 0 else "Screened",
                    "age": 20 + (i % 50),
                    "temperatureCelsius": 36.5 + (i % 30) / 10,
                    "symptoms": [{"symptom": "Fever", "onsetDate": "2026-07-01"}] if i % 7 == 0 else [],
                    "countriesVisited": [{"country": "Uganda", "arrivalDate": "2026-06-18", "departureDate": "2026-07-02"}],
                }
            )
    # the endpoint's contract: modified asc, name asc
    rows.sort(key=lambda r: (r["modified"], r["screeningIdentifier"]))
    return rows


ROWS = build_fixtures()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass  # keep stdout clean

    def _json(self, code, body):
        raw = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        url = urlparse(self.path)
        if url.path != PATH:
            return self._json(404, {"exc_type": "DoesNotExistError"})

        auth = self.headers.get("Authorization")
        if auth != EXPECTED_AUTH:
            return self._json(403, {"exc_type": "PermissionError", "got": auth})

        q = {k: v[0] for k, v in parse_qs(url.query).items()}
        with open(REQUEST_LOG, "a") as fh:
            fh.write(json.dumps({"query": q, "auth_ok": True}) + "\n")

        start = q.get("dateStart")
        end = q.get("dateEnd")
        limit = min(int(q.get("limit", 200)), 500)
        page = max(1, int(q.get("page", 1)))

        # string comparison is valid precisely because the cursor is fixed-width
        sel = [r for r in ROWS if start is None or r["modified"] >= start]
        if end:
            sel = [r for r in sel if r["modified"] < end]  # half-open

        total = len(sel)
        window = sel[(page - 1) * limit : page * limit]
        self._json(
            200,
            {
                "message": {
                    "data": window,
                    "page": page,
                    "limit": limit,
                    "total": total,
                    "pages": max(1, math.ceil(total / limit)),
                }
            },
        )


if __name__ == "__main__":
    open(REQUEST_LOG, "w").close()
    print(f"mock frappe on :{PORT} — {len(ROWS)} rows", flush=True)
    HTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
