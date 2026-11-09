"""Local ProspectFlow API and static app. Standard library only."""

import json
import hashlib
import ipaddress
import csv
import io
import unicodedata
import os
import re
import secrets
import sqlite3
import threading
import time
import uuid
import hmac
from http.cookies import SimpleCookie
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

ROOT = Path(__file__).parent
DB_PATH = Path(
    os.environ.get("PROSPECTFLOW_DB", ROOT / "data" / "prospectflow.sqlite3")
)
SESSION_COOKIE = "prospectflow_session"
SESSION_DAYS = 14
PBKDF2_ROUNDS = 310_000
ACTOR = threading.local()
LOGIN_FAILURES = {}
LOGIN_FAILURES_LOCK = threading.Lock()
SCHEMA = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS users(id TEXT PRIMARY KEY, username TEXT NOT NULL UNIQUE, display_name TEXT NOT NULL,
  password_salt BLOB NOT NULL, password_hash BLOB NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS auth_sessions(id TEXT PRIMARY KEY, user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  token_hash TEXT NOT NULL UNIQUE, expires_at TEXT NOT NULL, created_at TEXT NOT NULL, last_seen_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS idx_auth_sessions_user_expiry ON auth_sessions(user_id,expires_at);
CREATE TABLE IF NOT EXISTS sources(id TEXT PRIMARY KEY, name TEXT NOT NULL, host TEXT NOT NULL UNIQUE,
  allowed_paths TEXT NOT NULL, min_interval_seconds INTEGER NOT NULL DEFAULT 60,
  terms_reviewed_at TEXT, robots_reviewed_at TEXT, policy_review_due TEXT, adapter TEXT NOT NULL DEFAULT 'demo',
  base_url TEXT, terms_url TEXT, last_request_at TEXT);
CREATE TABLE IF NOT EXISTS scrape_projects(id TEXT PRIMARY KEY, name TEXT NOT NULL, purpose TEXT NOT NULL DEFAULT '',
  owner_id TEXT NOT NULL, priority TEXT NOT NULL DEFAULT 'normal', status TEXT NOT NULL DEFAULT 'planning',
  start_date TEXT, end_date TEXT, timezone TEXT NOT NULL DEFAULT 'UTC', notes TEXT NOT NULL DEFAULT '',
  target_volume INTEGER, cadence TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS project_sources(id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES scrape_projects(id) ON DELETE CASCADE,
  source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE RESTRICT, allowed_paths TEXT NOT NULL,
  selected_fields TEXT NOT NULL, pacing_seconds INTEGER NOT NULL, policy_review_due TEXT, query_json TEXT NOT NULL DEFAULT '{}',
  UNIQUE(project_id, source_id));
CREATE TABLE IF NOT EXISTS scrape_jobs(id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES scrape_projects(id),
  source_id TEXT NOT NULL REFERENCES sources(id), status TEXT NOT NULL, selected_scope TEXT NOT NULL, selected_fields TEXT NOT NULL,
  scheduled_at TEXT, started_at TEXT, completed_at TEXT, cancelled_at TEXT, progress INTEGER NOT NULL DEFAULT 0,
  checkpoint TEXT, retry_count INTEGER NOT NULL DEFAULT 0, max_retries INTEGER NOT NULL DEFAULT 3,
  next_retry_at TEXT, idempotency_key TEXT NOT NULL UNIQUE, records_found INTEGER NOT NULL DEFAULT 0,
  issues TEXT NOT NULL DEFAULT '[]', search_json TEXT NOT NULL DEFAULT '{}', created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
  worker_id TEXT, heartbeat_at TEXT, cancel_requested INTEGER NOT NULL DEFAULT 0, attempt_count INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS scrape_records(id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES scrape_jobs(id) ON DELETE CASCADE,
  project_id TEXT NOT NULL REFERENCES scrape_projects(id) ON DELETE CASCADE,
  source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE RESTRICT, source_url TEXT NOT NULL,
  content_hash TEXT NOT NULL, entity_key TEXT NOT NULL, fields_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'needs_review',
  normalized_json TEXT, normalized_name TEXT, normalized_professional_id TEXT, normalized_role TEXT,
  normalized_specialty TEXT, normalized_company TEXT, normalized_location TEXT, quality_score INTEGER,
  quality_issues TEXT NOT NULL DEFAULT '[]', prepared_at TEXT, fingerprint TEXT, duplicate_of TEXT,
  reviewed_by TEXT, review_note TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
  UNIQUE(job_id,source_url,entity_key));
CREATE TABLE IF NOT EXISTS worker_heartbeats(id TEXT PRIMARY KEY, hostname TEXT NOT NULL, started_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL, active_jobs INTEGER NOT NULL DEFAULT 0, worker_version TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS agenda_entries(id TEXT PRIMARY KEY, title TEXT NOT NULL, description TEXT NOT NULL DEFAULT '',
  event_type TEXT NOT NULL, start_at TEXT NOT NULL, end_at TEXT NOT NULL, timezone TEXT NOT NULL,
  project_id TEXT REFERENCES scrape_projects(id) ON DELETE SET NULL, source_id TEXT REFERENCES sources(id) ON DELETE SET NULL,
  job_id TEXT REFERENCES scrape_jobs(id) ON DELETE SET NULL, owner_id TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'scheduled', created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS audit_events(id TEXT PRIMARY KEY, actor_id TEXT NOT NULL, entity_type TEXT NOT NULL,
  entity_id TEXT NOT NULL, action TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS idx_projects_status_owner_dates ON scrape_projects(status,owner_id,start_date,end_date);
CREATE INDEX IF NOT EXISTS idx_jobs_project_status_scheduled ON scrape_jobs(project_id,status,scheduled_at);
CREATE INDEX IF NOT EXISTS idx_agenda_time_project ON agenda_entries(start_at,end_at,project_id);
CREATE INDEX IF NOT EXISTS idx_agenda_owner_status ON agenda_entries(owner_id,status,start_at);
CREATE INDEX IF NOT EXISTS idx_records_project_status_created ON scrape_records(project_id,status,created_at DESC);
CREATE INDEX IF NOT EXISTS idx_records_job ON scrape_records(job_id,created_at);
CREATE INDEX IF NOT EXISTS idx_worker_heartbeat ON worker_heartbeats(last_seen_at);
"""


def now():
    return (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )


def connect():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(DB_PATH, timeout=10)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys=ON")
    db.execute("PRAGMA journal_mode=WAL")
    return db


def migrate():
    with connect() as db:
        db.executescript(SCHEMA)
        db.execute("INSERT OR IGNORE INTO schema_migrations VALUES(1,?)", (now(),))
        db.execute("INSERT OR IGNORE INTO schema_migrations VALUES(2,?)", (now(),))
        for table, column, definition in (
            ("sources", "base_url", "TEXT"),
            ("sources", "terms_url", "TEXT"),
            ("sources", "last_request_at", "TEXT"),
            ("scrape_jobs", "worker_id", "TEXT"),
            ("scrape_jobs", "heartbeat_at", "TEXT"),
            ("scrape_jobs", "cancel_requested", "INTEGER NOT NULL DEFAULT 0"),
            ("scrape_jobs", "attempt_count", "INTEGER NOT NULL DEFAULT 0"),
            ("project_sources", "query_json", "TEXT NOT NULL DEFAULT '{}'"),
            ("scrape_jobs", "search_json", "TEXT NOT NULL DEFAULT '{}'"),
            ("scrape_records", "normalized_json", "TEXT"),
            ("scrape_records", "normalized_name", "TEXT"),
            ("scrape_records", "normalized_professional_id", "TEXT"),
            ("scrape_records", "normalized_role", "TEXT"),
            ("scrape_records", "normalized_specialty", "TEXT"),
            ("scrape_records", "normalized_company", "TEXT"),
            ("scrape_records", "normalized_location", "TEXT"),
            ("scrape_records", "quality_score", "INTEGER"),
            ("scrape_records", "quality_issues", "TEXT NOT NULL DEFAULT '[]'"),
            ("scrape_records", "prepared_at", "TEXT"),
            ("scrape_records", "fingerprint", "TEXT"),
            ("scrape_records", "duplicate_of", "TEXT"),
        ):
            existing = {
                row[1] for row in db.execute("PRAGMA table_info(" + table + ")")
            }
            if column not in existing:
                db.execute(
                    "ALTER TABLE " + table + " ADD COLUMN " + column + " " + definition
                )
        db.execute("INSERT OR IGNORE INTO schema_migrations VALUES(3,?)", (now(),))
        db.execute("INSERT OR IGNORE INTO schema_migrations VALUES(4,?)", (now(),))
        db.execute(
            "CREATE INDEX IF NOT EXISTS idx_records_project_prepared ON scrape_records(project_id,prepared_at,created_at)"
        )
        db.execute(
            "CREATE INDEX IF NOT EXISTS idx_records_project_fingerprint ON scrape_records(project_id,fingerprint)"
        )
        db.execute(
            "CREATE INDEX IF NOT EXISTS idx_records_project_quality ON scrape_records(project_id,quality_score)"
        )
        db.execute(
            "CREATE INDEX IF NOT EXISTS idx_records_project_specialty ON scrape_records(project_id,normalized_specialty)"
        )
        db.execute("INSERT OR IGNORE INTO schema_migrations VALUES(5,?)", (now(),))
        db.execute(
            "UPDATE sources SET allowed_paths=? WHERE adapter='ans-fhir' AND allowed_paths=?",
            (
                json.dumps(
                    ["/fhir/v2/Practitioner", "/fhir/v2/Organization", "/fhir/v2/_page"]
                ),
                json.dumps(["/fhir/v2/Practitioner", "/fhir/v2/_page"]),
            ),
        )
        if not db.execute("SELECT 1 FROM sources LIMIT 1").fetchone():
            db.execute(
                "INSERT INTO sources(id,name,host,allowed_paths,min_interval_seconds,terms_reviewed_at,robots_reviewed_at,policy_review_due,adapter) VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    "source-demo",
                    "Example demo adapter",
                    "demo.example",
                    json.dumps(["/"]),
                    60,
                    "2026-01-01",
                    "2026-01-01",
                    "2027-01-01",
                    "synthetic-empty",
                ),
            )


def rowdict(row):
    return dict(row) if row else None


def clean_profile_value(value):
    if value is None:
        return ""
    value = unicodedata.normalize("NFKC", str(value))
    value = "".join(
        ch for ch in value if unicodedata.category(ch) != "Cc" or ch in "\t\n\r"
    )
    return " ".join(value.split())[:1000]


def profile_key(value):
    value = unicodedata.normalize("NFKD", str(value or "").casefold())
    return "".join(ch for ch in value if not unicodedata.combining(ch)).strip()


def prepare_profile(fields):
    normalized = {}
    values = {}
    issues = []
    for field, provenance in fields.items():
        if not isinstance(provenance, dict):
            continue
        value = clean_profile_value(provenance.get("value"))
        if not value:
            continue
        item = dict(provenance)
        item["value"] = value
        normalized[field] = item
        values[field] = value
        if not isinstance(item.get("sourceUrl"), str) or not item[
            "sourceUrl"
        ].startswith("https://"):
            issues.append({"code": "source_provenance_missing", "field": field})
        if item.get("source") == "RPPS" and not item.get("sourceUpdatedAt"):
            issues.append({"code": "source_update_missing", "field": field})
    name = values.get("name", "")
    professional_id = values.get("professional_id", "")
    digits = re.sub(r"\D", "", professional_id)
    role = values.get("role", "")
    specialty = values.get("specialty", "")
    company = values.get("company", "")
    location = values.get("location", "")
    if not name:
        issues.append({"code": "name_missing", "field": "name"})
    if not professional_id:
        issues.append({"code": "professional_id_missing", "field": "professional_id"})
    elif len(digits) != 11:
        issues.append({"code": "professional_id_format", "field": "professional_id"})
    if not role:
        issues.append({"code": "role_missing", "field": "role"})
    if not specialty:
        issues.append({"code": "specialty_missing", "field": "specialty"})
    score = max(
        0,
        100
        - 30 * int(not name)
        - 20 * int(not professional_id)
        - 10 * int(bool(professional_id) and len(digits) != 11)
        - 15 * int(not role)
        - 15 * int(not specialty)
        - 10
        * int(
            any(
                x["code"] in ("source_provenance_missing", "source_update_missing")
                for x in issues
            )
        ),
    )
    identity = digits or profile_key(name)
    fingerprint = (
        hashlib.sha256(
            "|".join(
                [
                    identity,
                    profile_key(role),
                    profile_key(specialty),
                    profile_key(company),
                    profile_key(location),
                ]
            ).encode("utf-8")
        ).hexdigest()
        if identity
        else None
    )
    return (
        normalized,
        {
            "name": name,
            "professional_id": digits or professional_id,
            "role": role,
            "specialty": specialty,
            "company": company,
            "location": location,
        },
        score,
        issues,
        fingerprint,
    )


def body(handler):
    size = int(handler.headers.get("Content-Length", "0"))
    if size > 1_000_000:
        raise ValueError("Request body exceeds 1 MB")
    return json.loads(handler.rfile.read(size) or b"{}")


def dt(value):
    if not isinstance(value, str):
        raise ValueError("Expected an ISO-8601 timestamp")
    # Python 3.8 accepts at most six fractional digits; browsers and clients may send nine.
    value = re.sub(
        r"\.(\d{7,})(?=[+-]\d{2}:?\d{2}$|Z$)", lambda m: "." + m.group(1)[:6], value
    )
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Timestamp must include a timezone")
    return (
        parsed.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def path_in_scope(path, approved):
    if not isinstance(path, str) or not path.startswith("/"):
        return False
    path = unquote(path)
    if (
        "\\" in path
        or any(segment in (".", "..") for segment in path.split("/"))
        or re.search(r"%(?:2e|2f|5c)", path, re.I)
    ):
        return False
    for root in approved:
        if root == "/":
            return True
        if root.endswith("/*") and path.startswith(root[:-1]):
            return True
        if path == root.rstrip("/") or path.startswith(root.rstrip("/") + "/"):
            return True
    return False


def source_https_url(value):
    if not isinstance(value, str):
        raise ValueError("Use an HTTPS source URL.")
    parsed = urlparse(value.strip())
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.port not in (None, 443)
        or parsed.fragment
    ):
        raise ValueError(
            "Source and terms links must use HTTPS without credentials or custom ports."
        )
    host = parsed.hostname.rstrip(".").encode("idna").decode("ascii").lower()
    if (
        "." not in host
        or host.endswith(
            (".localhost", ".local", ".internal", ".example", ".test", ".invalid")
        )
        or host in ("localhost", "example.com", "example.org", "example.net")
    ):
        raise ValueError(
            "Use a real public hostname, not a local or reserved example host."
        )
    try:
        ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        raise ValueError("Enter a DNS hostname, not an IP address.")
    return parsed, host


def audit(db, entity, eid, action):
    db.execute(
        "INSERT INTO audit_events VALUES(?,?,?,?,?,?)",
        (
            str(uuid.uuid4()),
            getattr(ACTOR, "user_id", "system"),
            entity,
            eid,
            action,
            now(),
        ),
    )


def password_digest(password, salt):
    return hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, PBKDF2_ROUNDS)


def username_key(value):
    return value.strip().casefold() if isinstance(value, str) else ""


def valid_password(password):
    return isinstance(password, str) and len(password) >= 12 and len(password) <= 1024


def cookie_session(headers):
    jar = SimpleCookie()
    try:
        jar.load(headers.get("Cookie", ""))
    except Exception:
        return None
    morsel = jar.get(SESSION_COOKIE)
    return morsel.value if morsel else None


class Handler(BaseHTTPRequestHandler):
    server_version = "ProspectFlow/0.1"

    def send(
        self, status, data, content_type="application/json; charset=utf-8", headers=None
    ):
        raw = (
            data
            if isinstance(data, bytes)
            else json.dumps(data, ensure_ascii=False).encode()
        )
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        self.route()

    def do_POST(self):
        self.route()

    def do_PATCH(self):
        self.route()

    def do_PUT(self):
        self.route()

    def do_DELETE(self):
        self.route()

    def route(self):
        path = unquote(urlparse(self.path).path)
        q = parse_qs(urlparse(self.path).query)
        if path == "/" or path.startswith("/static/"):
            static_root = (ROOT / "static").resolve()
            file = (
                static_root / "index.html" if path == "/" else ROOT / path.lstrip("/")
            )
            if static_root not in file.resolve().parents or not file.exists():
                return self.send(404, {"error": "Not found"})
            mime = {
                ".html": "text/html; charset=utf-8",
                ".css": "text/css; charset=utf-8",
                ".js": "text/javascript; charset=utf-8",
            }.get(file.suffix, "application/octet-stream")
            return self.send(200, file.read_bytes(), mime)
        if path == "/api/health":
            cutoff = (
                (datetime.now(timezone.utc) - timedelta(seconds=30))
                .isoformat(timespec="seconds")
                .replace("+00:00", "Z")
            )
            with connect() as db:
                workers = db.execute(
                    "SELECT COUNT(*) FROM worker_heartbeats WHERE last_seen_at>=?",
                    (cutoff,),
                ).fetchone()[0]
                ans_workers = db.execute(
                    "SELECT COUNT(*) FROM worker_heartbeats WHERE last_seen_at>=? AND worker_version LIKE '%ans_fhir=ready%'",
                    (cutoff,),
                ).fetchone()[0]
            return self.send(
                200,
                {
                    "ok": True,
                    "scheduler": "active",
                    "worker": "online" if workers else "offline",
                    "workers": workers,
                    "ansFhirReady": bool(ans_workers),
                },
            )
        if not path.startswith("/api/"):
            return self.send(404, {"error": "Not found"})
        if path.startswith("/api/auth/"):
            try:
                result, status, cookie = self.auth_api(path)
                return self.send(
                    status, result, headers={"Set-Cookie": cookie} if cookie else None
                )
            except (ValueError, json.JSONDecodeError) as e:
                return self.send(400, {"error": str(e)})
            except sqlite3.IntegrityError:
                return self.send(409, {"error": "That username is already registered."})
        token = cookie_session(self.headers)
        if not token:
            return self.send(401, {"error": "Sign in to continue."})
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        with connect() as db:
            row = db.execute(
                "SELECT u.id,u.username,u.display_name,s.id session_id FROM auth_sessions s JOIN users u ON u.id=s.user_id WHERE s.token_hash=? AND s.expires_at>?",
                (token_hash, now()),
            ).fetchone()
            if not row:
                return self.send(
                    401,
                    {"error": "Your session expired. Sign in again."},
                    headers={"Set-Cookie": self.clear_cookie()},
                )
            self.user = {
                "id": row["id"],
                "username": row["username"],
                "displayName": row["display_name"],
            }
            db.execute(
                "UPDATE auth_sessions SET last_seen_at=? WHERE id=?",
                (now(), row["session_id"]),
            )
        ACTOR.user_id = self.user["id"]
        records_export_match = re.fullmatch(r"/api/projects/([^/]+)/records\.csv", path)
        if records_export_match and self.command == "GET":
            try:
                data = self.project_records_csv(records_export_match.group(1), q)
                return self.send(
                    200,
                    data,
                    "text/csv; charset=utf-8",
                    {
                        "Content-Disposition": "attachment; filename=prospectflow-records.csv"
                    },
                )
            except LookupError as e:
                return self.send(404, {"error": str(e)})
            except ValueError as e:
                return self.send(400, {"error": str(e)})
        report_match = re.fullmatch(r"/api/projects/([^/]+)/report\.csv", path)
        if report_match and self.command == "GET":
            try:
                data = self.project_report_csv(report_match.group(1), q)
                return self.send(
                    200,
                    data,
                    "text/csv; charset=utf-8",
                    {
                        "Content-Disposition": "attachment; filename=prospectflow-report.csv"
                    },
                )
            except LookupError as e:
                return self.send(404, {"error": str(e)})
            except ValueError as e:
                return self.send(400, {"error": str(e)})
        try:
            result, status = self.api(path, q)
            self.send(status, result)
        except (ValueError, json.JSONDecodeError) as e:
            self.send(400, {"error": str(e)})
        except LookupError as e:
            self.send(404, {"error": str(e)})
        except sqlite3.IntegrityError as e:
            self.send(409, {"error": "State conflict", "detail": str(e)})
        except Exception as e:
            self.log_error("API error: %s", e)
            self.send(500, {"error": "Internal server error"})

    def set_cookie(self, token):
        secure = (
            "; Secure"
            if self.headers.get("X-Forwarded-Proto", "").lower() == "https"
            else ""
        )
        return (
            f"{SESSION_COOKIE}={token}; HttpOnly; SameSite=Strict; Path=/; Max-Age={SESSION_DAYS * 86400}"
            + secure
        )

    def clear_cookie(self):
        secure = (
            "; Secure"
            if self.headers.get("X-Forwarded-Proto", "").lower() == "https"
            else ""
        )
        return (
            f"{SESSION_COOKIE}=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0; Expires=Thu, 01 Jan 1970 00:00:00 GMT"
            + secure
        )

    def project_report_csv(self, pid, q):
        limit = min(10000, max(1, int(q.get("limit", [1000])[0])))
        include_duplicates = q.get("includeDuplicates", ["false"])[0].lower() == "true"
        with connect() as db:
            project = db.execute(
                "SELECT owner_id FROM scrape_projects WHERE id=?", (pid,)
            ).fetchone()
            if not project or project["owner_id"] != self.user["id"]:
                raise LookupError("Project not found")
            where = ["project_id=?", "prepared_at IS NOT NULL"]
            args = [pid]
            if not include_duplicates:
                where.append("duplicate_of IS NULL")
            if q.get("status"):
                where.append("status=?")
                args.append(q["status"][0])
            rows = db.execute(
                "SELECT normalized_name,normalized_professional_id,normalized_role,normalized_specialty,normalized_company,normalized_location,normalized_json,source_url,status,quality_score,quality_issues,duplicate_of FROM scrape_records WHERE "
                + " AND ".join(where)
                + " ORDER BY created_at,id LIMIT ?",
                (*args, limit),
            ).fetchall()
        output = io.StringIO(newline="")
        writer = csv.writer(output)
        writer.writerow(
            [
                "Name",
                "Professional ID",
                "Role",
                "Specialty",
                "Practice",
                "Practice location",
                "Data source",
                "Source data last updated",
                "Source record URL",
                "Review status",
                "Quality score",
                "Quality issues",
                "Duplicate of",
            ]
        )
        for row in rows:
            issues = json.loads(row["quality_issues"] or "[]")
            provenance = json.loads(row["normalized_json"] or "{}")
            source_field = next(
                (
                    item
                    for item in provenance.values()
                    if isinstance(item, dict) and item.get("source")
                ),
                {},
            )
            writer.writerow(
                [
                    row["normalized_name"] or "",
                    row["normalized_professional_id"] or "",
                    row["normalized_role"] or "",
                    row["normalized_specialty"] or "",
                    row["normalized_company"] or "",
                    row["normalized_location"] or "",
                    source_field.get("source", ""),
                    source_field.get("sourceUpdatedAt", ""),
                    row["source_url"],
                    row["status"],
                    row["quality_score"] if row["quality_score"] is not None else "",
                    "; ".join(x.get("code", "") for x in issues),
                    row["duplicate_of"] or "",
                ]
            )
        return output.getvalue().encode("utf-8-sig")

    def project_records_csv(self, pid, q):
        limit = min(10000, max(1, int(q.get("limit", ["10000"])[0])))
        offset = max(0, int(q.get("offset", ["0"])[0]))
        with connect() as db:
            project = db.execute(
                "SELECT owner_id FROM scrape_projects WHERE id=?", (pid,)
            ).fetchone()
            if not project or project["owner_id"] != self.user["id"]:
                raise LookupError("Project not found")
            where = ["r.project_id=?"]
            args = [pid]
            if q.get("status"):
                status = q["status"][0]
                if status not in ("needs_review", "verified", "resolved", "flagged"):
                    raise ValueError("Invalid record status.")
                where.append("r.status=?")
                args.append(status)
            if q.get("q"):
                where.append("COALESCE(r.normalized_json,r.fields_json) LIKE ?")
                args.append("%" + q["q"][0][:200] + "%")
            rows = db.execute(
                "SELECT r.fields_json,r.normalized_json,r.source_url,r.status,r.quality_score,r.quality_issues,r.duplicate_of,r.created_at,s.name source_name FROM scrape_records r JOIN sources s ON s.id=r.source_id WHERE "
                + " AND ".join(where)
                + " ORDER BY r.created_at DESC,r.id LIMIT ? OFFSET ?",
                (*args, limit, offset),
            ).fetchall()
        records = []
        field_names = set()
        for row in rows:
            fields = json.loads(row["normalized_json"] or "{}")
            if not fields:
                fields = json.loads(row["fields_json"] or "{}")
            field_names.update(fields.keys())
            records.append((row, fields))
        columns = [
            "Source",
            "Source record URL",
            "Review status",
            "Quality score",
            "Quality issues",
            "Duplicate of",
            "Created at",
        ] + sorted(field_names)

        def safe(value):
            value = "" if value is None else str(value)
            return (
                "'" + value
                if value.lstrip().startswith(("=", "+", "-", "@", "\t", "\r"))
                else value
            )

        output = io.StringIO(newline="")
        writer = csv.writer(output)
        writer.writerow(columns)
        for row, fields in records:
            issues = json.loads(row["quality_issues"] or "[]")
            base = [
                row["source_name"],
                row["source_url"],
                row["status"],
                row["quality_score"],
                "; ".join(x.get("code", "") for x in issues),
                row["duplicate_of"],
                row["created_at"],
            ]
            values = []
            for key in sorted(field_names):
                item = fields.get(key, "")
                value = item.get("value", "") if isinstance(item, dict) else item
                if isinstance(item, dict) and item.get("sourceUrl"):
                    value = (
                        str(value) + " (field source: " + str(item["sourceUrl"]) + ")"
                    )
                values.append(value)
            writer.writerow([safe(x) for x in base + values])
        return output.getvalue().encode("utf-8-sig")

    def auth_api(self, path):
        method = self.command
        token = cookie_session(self.headers)
        if path == "/api/auth/setup-status" and method == "GET":
            with connect() as db:
                count = db.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            return {"needsSetup": count == 0}, 200, None
        if path == "/api/auth/setup" and method == "POST":
            d = body(self)
            if not isinstance(d, dict):
                raise ValueError("Expected a JSON object.")
            with connect() as db:
                if db.execute("SELECT 1 FROM users LIMIT 1").fetchone():
                    return (
                        {
                            "error": "Initial setup has already been completed. Sign in instead."
                        },
                        409,
                        None,
                    )
            username = username_key(d.get("username", ""))
            raw_name = d.get("displayName")
            name = raw_name.strip() if isinstance(raw_name, str) else ""
            password = d.get("password", "")
            if not re.fullmatch(r"[a-z0-9][a-z0-9._-]{2,79}", username):
                raise ValueError(
                    "Username must be 3â€“80 characters using letters, numbers, dot, underscore or hyphen."
                )
            if not name or len(name) > 80:
                raise ValueError("Display name must be 1â€“80 characters.")
            if not valid_password(password):
                raise ValueError("Password must contain at least 12 characters.")
            salt = secrets.token_bytes(16)
            digest = password_digest(password, salt)
            user_id = str(uuid.uuid4())
            timestamp = now()
            raw = secrets.token_urlsafe(32)
            expires = (
                (datetime.now(timezone.utc) + timedelta(days=SESSION_DAYS))
                .isoformat(timespec="seconds")
                .replace("+00:00", "Z")
            )
            with connect() as db:
                db.execute("BEGIN IMMEDIATE")
                if db.execute("SELECT 1 FROM users LIMIT 1").fetchone():
                    raise ValueError(
                        "Initial setup has already been completed. Sign in instead."
                    )
                db.execute(
                    "INSERT INTO users(id,username,display_name,password_salt,password_hash,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                    (user_id, username, name, salt, digest, timestamp, timestamp),
                )
                db.execute(
                    "UPDATE scrape_projects SET owner_id=?,updated_at=? WHERE owner_id='local-user'",
                    (user_id, timestamp),
                )
                db.execute(
                    "UPDATE agenda_entries SET owner_id=?,updated_at=? WHERE owner_id='local-user'",
                    (user_id, timestamp),
                )
                db.execute(
                    "INSERT INTO auth_sessions(id,user_id,token_hash,expires_at,created_at,last_seen_at) VALUES(?,?,?,?,?,?)",
                    (
                        str(uuid.uuid4()),
                        user_id,
                        hashlib.sha256(raw.encode()).hexdigest(),
                        expires,
                        timestamp,
                        timestamp,
                    ),
                )
            return (
                {"user": {"id": user_id, "username": username, "displayName": name}},
                201,
                self.set_cookie(raw),
            )
        if path == "/api/auth/login" and method == "POST":
            ip = self.client_address[0]
            clock = time.time()
            with LOGIN_FAILURES_LOCK:
                fail_count, window = LOGIN_FAILURES.get(ip, (0, clock))
                if clock - window > 600:
                    fail_count, window = 0, clock
                if fail_count >= 5:
                    return (
                        {
                            "error": "Too many sign-in attempts. Wait 10 minutes and try again."
                        },
                        429,
                        None,
                    )
            d = body(self)
            if not isinstance(d, dict):
                raise ValueError("Expected a JSON object.")
            username = username_key(d.get("username", ""))
            password = d.get("password", "")
            with connect() as db:
                user = db.execute(
                    "SELECT * FROM users WHERE username=?", (username,)
                ).fetchone()
            salt = user["password_salt"] if user else b"prospectflow-dummy-salt"
            expected = (
                user["password_hash"]
                if user
                else password_digest("not-a-real-password", salt)
            )
            supplied = password_digest(
                password if isinstance(password, str) else "", salt
            )
            if not user or not hmac.compare_digest(expected, supplied):
                with LOGIN_FAILURES_LOCK:
                    count, window = LOGIN_FAILURES.get(ip, (0, clock))
                    if clock - window > 600:
                        count, window = 0, clock
                    LOGIN_FAILURES[ip] = (count + 1, window)
                return {"error": "Username or password is incorrect."}, 401, None
            with LOGIN_FAILURES_LOCK:
                LOGIN_FAILURES.pop(ip, None)
            raw = secrets.token_urlsafe(32)
            timestamp = now()
            expires = (
                (datetime.now(timezone.utc) + timedelta(days=SESSION_DAYS))
                .isoformat(timespec="seconds")
                .replace("+00:00", "Z")
            )
            with connect() as db:
                db.execute(
                    "DELETE FROM auth_sessions WHERE expires_at<=?", (timestamp,)
                )
                db.execute(
                    "INSERT INTO auth_sessions(id,user_id,token_hash,expires_at,created_at,last_seen_at) VALUES(?,?,?,?,?,?)",
                    (
                        str(uuid.uuid4()),
                        user["id"],
                        hashlib.sha256(raw.encode()).hexdigest(),
                        expires,
                        timestamp,
                        timestamp,
                    ),
                )
            return (
                {
                    "user": {
                        "id": user["id"],
                        "username": user["username"],
                        "displayName": user["display_name"],
                    }
                },
                200,
                self.set_cookie(raw),
            )
        if path == "/api/auth/me" and method == "GET":
            if not token:
                return {"error": "Sign in to continue."}, 401, None
            with connect() as db:
                user = db.execute(
                    "SELECT u.id,u.username,u.display_name,s.id session_id FROM auth_sessions s JOIN users u ON u.id=s.user_id WHERE s.token_hash=? AND s.expires_at>?",
                    (hashlib.sha256(token.encode()).hexdigest(), now()),
                ).fetchone()
                if not user:
                    return (
                        {"error": "Your session expired. Sign in again."},
                        401,
                        self.clear_cookie(),
                    )
                db.execute(
                    "UPDATE auth_sessions SET last_seen_at=? WHERE id=?",
                    (now(), user["session_id"]),
                )
            return (
                {
                    "user": {
                        "id": user["id"],
                        "username": user["username"],
                        "displayName": user["display_name"],
                    }
                },
                200,
                None,
            )
        if path == "/api/auth/password" and method == "POST":
            if not token:
                return {"error": "Sign in to continue."}, 401, None
            d = body(self)
            if not isinstance(d, dict):
                raise ValueError("Expected a JSON object.")
            new_password = d.get("newPassword", "")
            if not valid_password(new_password):
                raise ValueError("New password must contain at least 12 characters.")
            with connect() as db:
                user = db.execute(
                    "SELECT u.* FROM auth_sessions s JOIN users u ON u.id=s.user_id WHERE s.token_hash=? AND s.expires_at>?",
                    (hashlib.sha256(token.encode()).hexdigest(), now()),
                ).fetchone()
                if not user:
                    return (
                        {"error": "Your session expired. Sign in again."},
                        401,
                        self.clear_cookie(),
                    )
                current = password_digest(
                    d.get("currentPassword", "")
                    if isinstance(d.get("currentPassword"), str)
                    else "",
                    user["password_salt"],
                )
                if not hmac.compare_digest(user["password_hash"], current):
                    return {"error": "Current password is incorrect."}, 401, None
                salt = secrets.token_bytes(16)
                digest = password_digest(new_password, salt)
                raw = secrets.token_urlsafe(32)
                timestamp = now()
                expires = (
                    (datetime.now(timezone.utc) + timedelta(days=SESSION_DAYS))
                    .isoformat(timespec="seconds")
                    .replace("+00:00", "Z")
                )
                db.execute(
                    "UPDATE users SET password_salt=?,password_hash=?,updated_at=? WHERE id=?",
                    (salt, digest, timestamp, user["id"]),
                )
                db.execute("DELETE FROM auth_sessions WHERE user_id=?", (user["id"],))
                db.execute(
                    "INSERT INTO auth_sessions(id,user_id,token_hash,expires_at,created_at,last_seen_at) VALUES(?,?,?,?,?,?)",
                    (
                        str(uuid.uuid4()),
                        user["id"],
                        hashlib.sha256(raw.encode()).hexdigest(),
                        expires,
                        timestamp,
                        timestamp,
                    ),
                )
            return {"ok": True}, 200, self.set_cookie(raw)
        if path == "/api/auth/logout" and method == "POST":
            if token:
                with connect() as db:
                    db.execute(
                        "DELETE FROM auth_sessions WHERE token_hash=?",
                        (hashlib.sha256(token.encode()).hexdigest(),),
                    )
            return {"ok": True}, 200, self.clear_cookie()
        return {"error": "Authentication route not found."}, 404, None

    def api(self, path, q):
        method = self.command
        with connect() as db:
            if path == "/api/notifications" and method == "GET":
                stamp = now()
                notices = []
                overdue = db.execute(
                    "SELECT id,title,start_at,project_id FROM agenda_entries WHERE owner_id=? AND status='scheduled' AND end_at<? ORDER BY start_at LIMIT 50",
                    (self.user["id"], stamp),
                ).fetchall()
                for e in overdue:
                    notices.append(
                        {
                            "id": "agenda:" + e["id"],
                            "kind": "overdue",
                            "title": e["title"],
                            "detail": "Agenda item is overdue",
                            "href": "#project/" + e["project_id"]
                            if e["project_id"]
                            else "#agenda",
                            "at": e["start_at"],
                        }
                    )
                failed = db.execute(
                    "SELECT j.id,j.updated_at,p.id project_id,s.name source_name FROM scrape_jobs j JOIN scrape_projects p ON p.id=j.project_id JOIN sources s ON s.id=j.source_id WHERE p.owner_id=? AND j.status='failed' ORDER BY j.updated_at DESC LIMIT 50",
                    (self.user["id"],),
                ).fetchall()
                for j in failed:
                    notices.append(
                        {
                            "id": "job:" + j["id"],
                            "kind": "failed_run",
                            "title": j["source_name"] + " run failed",
                            "detail": "Review the run summary and retry if safe",
                            "href": "#project/" + j["project_id"],
                            "at": j["updated_at"],
                        }
                    )
                queued = db.execute(
                    "SELECT COUNT(*) FROM scrape_jobs j JOIN scrape_projects p ON p.id=j.project_id WHERE p.owner_id=? AND j.status IN ('queued','scheduled')",
                    (self.user["id"],),
                ).fetchone()[0]
                cutoff = (
                    (datetime.now(timezone.utc) - timedelta(seconds=30))
                    .isoformat(timespec="seconds")
                    .replace("+00:00", "Z")
                )
                workers = db.execute(
                    "SELECT COUNT(*) FROM worker_heartbeats WHERE last_seen_at>=?",
                    (cutoff,),
                ).fetchone()[0]
                if queued and not workers:
                    notices.append(
                        {
                            "id": "worker:offline",
                            "kind": "worker_offline",
                            "title": str(queued) + " run(s) waiting",
                            "detail": "Worker is offline; queued work remains saved",
                            "href": "#projects",
                            "at": stamp,
                        }
                    )
                notices.sort(key=lambda n: n["at"], reverse=True)
                return {"items": notices[:100], "total": len(notices)}, 200
            if path == "/api/sources" and method == "GET":
                return [
                    dict(x) for x in db.execute("SELECT * FROM sources ORDER BY name")
                ], 200
            if path == "/api/sources" and method == "POST":
                d = body(self)
                if not isinstance(d, dict):
                    raise ValueError("Expected a JSON object.")
                if d.get("adapter") == "ans-fhir":
                    name = (d.get("name") or "Annuaire SantÃ© (ANS FHIR)").strip()
                    due = d.get("policyReviewDue")
                    try:
                        due_date = datetime.strptime(due, "%Y-%m-%d").date()
                    except Exception:
                        raise ValueError(
                            "Set a policy review due date in YYYY-MM-DD format."
                        )
                    today = datetime.now(timezone.utc).date()
                    if d.get("termsReviewed") is not True:
                        raise ValueError(
                            "Review the ANS data reuse terms before adding the API source."
                        )
                    if due_date < today or due_date > (today + timedelta(days=366)):
                        raise ValueError(
                            "Policy review must be due within the next 12 months."
                        )
                    if not name or len(name) > 120:
                        raise ValueError("Source name must be 1â€“120 characters.")
                    sid = str(uuid.uuid4())
                    host = "gateway.api.esante.gouv.fr"
                    existing = db.execute(
                        "SELECT * FROM sources WHERE host=?", (host,)
                    ).fetchone()
                    if existing:
                        if existing["adapter"] == "ans-fhir":
                            return dict(existing), 200
                        raise ValueError(
                            "The ANS API host is already registered with a different adapter."
                        )
                    db.execute(
                        "INSERT INTO sources(id,name,host,allowed_paths,min_interval_seconds,terms_reviewed_at,robots_reviewed_at,policy_review_due,adapter,base_url,terms_url) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            sid,
                            name,
                            host,
                            json.dumps(
                                [
                                    "/fhir/v2/Practitioner",
                                    "/fhir/v2/Organization",
                                    "/fhir/v2/_page",
                                ]
                            ),
                            1,
                            today.isoformat(),
                            None,
                            due,
                            "ans-fhir",
                            "https://gateway.api.esante.gouv.fr/fhir/v2",
                            "https://ansforge.github.io/annuaire-sante-fhir-documentation/pages/cgu.html",
                        ),
                    )
                    audit(db, "source", sid, "created_ans_fhir")
                    return rowdict(
                        db.execute(
                            "SELECT * FROM sources WHERE id=?", (sid,)
                        ).fetchone()
                    ), 201
                name = (d.get("name") or "").strip()
                base, host = source_https_url(d.get("baseUrl"))
                terms, terms_host = source_https_url(d.get("termsUrl"))
                if not name or len(name) > 120:
                    raise ValueError("Source name must be 1â€“120 characters.")
                if host == "doctolib.fr" or host.endswith(".doctolib.fr"):
                    raise ValueError(
                        "Doctolib disallows doctor profile and API paths in robots.txt. Use the ANS FHIR connector or another explicitly permitted source."
                    )
                if host != terms_host:
                    raise ValueError(
                        "Terms link must use the same exact host as the source."
                    )
                if base.path not in ("", "/") or base.query:
                    raise ValueError(
                        "Enter the source origin only; configure paths below."
                    )
                paths = d.get("allowedPaths")
                if not isinstance(paths, list) or not paths:
                    raise ValueError("Add at least one permitted path.")
                for p in paths:
                    if (
                        not isinstance(p, str)
                        or not p.startswith("/")
                        or not path_in_scope(p, [p])
                        or "?" in p
                        or "#" in p
                        or ("*" in p and not p.endswith("/*"))
                    ):
                        raise ValueError(
                            "Allowed paths must be absolute paths or a trailing /* pattern."
                        )
                pacing = int(d.get("minIntervalSeconds", 60))
                if pacing < 60 or pacing > 86400:
                    raise ValueError(
                        "Minimum source pacing is 60 seconds and cannot exceed one day."
                    )
                if (
                    d.get("termsReviewed") is not True
                    or d.get("robotsReviewed") is not True
                ):
                    raise ValueError(
                        "Confirm that terms and robots.txt were reviewed before adding this source."
                    )
                due = d.get("policyReviewDue")
                try:
                    due_date = datetime.strptime(due, "%Y-%m-%d").date()
                except Exception:
                    raise ValueError(
                        "Set the policy review due date in YYYY-MM-DD format."
                    )
                today = datetime.now(timezone.utc).date()
                if due_date < today or due_date > (today + timedelta(days=366)):
                    raise ValueError(
                        "Policy review must be due within the next 12 months."
                    )
                sid = str(uuid.uuid4())
                base_url = "https://" + host
                db.execute(
                    "INSERT INTO sources(id,name,host,allowed_paths,min_interval_seconds,terms_reviewed_at,robots_reviewed_at,policy_review_due,adapter,base_url,terms_url) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        sid,
                        name,
                        host,
                        json.dumps(paths),
                        pacing,
                        today.isoformat(),
                        today.isoformat(),
                        due,
                        "public-html",
                        base_url,
                        terms.geturl(),
                    ),
                )
                audit(db, "source", sid, "created")
                return rowdict(
                    db.execute("SELECT * FROM sources WHERE id=?", (sid,)).fetchone()
                ), 201
            policy_match = re.fullmatch(r"/api/sources/([^/]+)/policy-review", path)
            if policy_match and method == "POST":
                sid = policy_match.group(1)
                d = body(self)
                source = db.execute(
                    "SELECT * FROM sources WHERE id=?", (sid,)
                ).fetchone()
                if not source:
                    raise LookupError("Source not found")
                if d.get("termsReviewed") is not True or (
                    source["adapter"] != "ans-fhir"
                    and d.get("robotsReviewed") is not True
                ):
                    raise ValueError(
                        "Confirm that source terms and robots.txt were reviewed."
                    )
                terms, host = source_https_url(d.get("termsUrl"))
                if source["adapter"] == "ans-fhir":
                    if not (
                        host in ("esante.gouv.fr", "www.esante.gouv.fr")
                        or (
                            host == "ansforge.github.io"
                            and terms.path
                            == "/annuaire-sante-fhir-documentation/pages/cgu.html"
                        )
                    ):
                        raise ValueError("Use the official ANS terms page.")
                elif host != source["host"]:
                    raise ValueError(
                        "Terms link must use the same exact host as the source."
                    )
                try:
                    due_date = datetime.strptime(
                        d.get("policyReviewDue"), "%Y-%m-%d"
                    ).date()
                except Exception:
                    raise ValueError(
                        "Set the policy review due date in YYYY-MM-DD format."
                    )
                today = datetime.now(timezone.utc).date()
                if due_date < today or due_date > (today + timedelta(days=366)):
                    raise ValueError(
                        "Policy review must be due within the next 12 months."
                    )
                db.execute(
                    "UPDATE sources SET terms_url=?,terms_reviewed_at=?,robots_reviewed_at=?,policy_review_due=? WHERE id=?",
                    (
                        terms.geturl(),
                        today.isoformat(),
                        today.isoformat() if source["adapter"] != "ans-fhir" else None,
                        due_date.isoformat(),
                        sid,
                    ),
                )
                db.execute(
                    "UPDATE project_sources SET policy_review_due=? WHERE source_id=?",
                    (due_date.isoformat(), sid),
                )
                audit(db, "source", sid, "policy_reviewed")
                return rowdict(
                    db.execute("SELECT * FROM sources WHERE id=?", (sid,)).fetchone()
                ), 200
            if path == "/api/projects" and method == "GET":
                if q.get("ownerId") and q["ownerId"][0] != self.user["id"]:
                    return [], 200
                where = ["owner_id=?"]
                args = [self.user["id"]]
                for key, col in (("status", "status"),):
                    if q.get(key):
                        where.append(col + "=?")
                        args.append(q[key][0])
                if q.get("q"):
                    where.append("name LIKE ?")
                    args.append("%" + q["q"][0] + "%")
                limit = min(100, max(1, int(q.get("limit", [20])[0])))
                offset = max(0, int(q.get("offset", [0])[0]))
                rows = db.execute(
                    "SELECT * FROM scrape_projects"
                    + (" WHERE " + " AND ".join(where) if where else "")
                    + " ORDER BY updated_at DESC LIMIT ? OFFSET ?",
                    (*args, limit, offset),
                )
                return [dict(x) for x in rows], 200
            if path == "/api/projects" and method == "POST":
                d = body(self)
                pid = str(uuid.uuid4())
                t = now()
                if not d.get("name"):
                    raise ValueError("name is required")
                db.execute(
                    "INSERT INTO scrape_projects(id,name,purpose,owner_id,priority,status,start_date,end_date,timezone,notes,target_volume,cadence,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        pid,
                        d["name"],
                        d.get("purpose", ""),
                        self.user["id"],
                        d.get("priority", "normal"),
                        d.get("status", "planning"),
                        d.get("startDate"),
                        d.get("endDate"),
                        d.get("timezone", "UTC"),
                        d.get("notes", ""),
                        d.get("targetVolume"),
                        d.get("cadence"),
                        t,
                        t,
                    ),
                )
                audit(db, "project", pid, "created")
                return rowdict(
                    db.execute(
                        "SELECT * FROM scrape_projects WHERE id=?", (pid,)
                    ).fetchone()
                ), 201
            m = re.fullmatch(
                r"/api/projects/([^/]+)(?:/(sources|runs|readiness|summary|prepare|analysis)(?:/([^/]+))?)?",
                path,
            )
            if m:
                pid, sub, subid = m.groups()
                project = db.execute(
                    "SELECT * FROM scrape_projects WHERE id=?", (pid,)
                ).fetchone()
                if not project or project["owner_id"] != self.user["id"]:
                    raise LookupError("Project not found")
                if sub is None and method == "GET":
                    return dict(project), 200
                if sub is None and method == "PATCH":
                    d = body(self)
                    cols = {
                        "name": "name",
                        "purpose": "purpose",
                        "priority": "priority",
                        "status": "status",
                        "startDate": "start_date",
                        "endDate": "end_date",
                        "timezone": "timezone",
                        "notes": "notes",
                        "targetVolume": "target_volume",
                        "cadence": "cadence",
                    }
                    updates = [(cols[k], v) for k, v in d.items() if k in cols]
                    if updates:
                        sql = ", ".join(k + "=?" for k, v in updates) + ",updated_at=?"
                        db.execute(
                            "UPDATE scrape_projects SET " + sql + " WHERE id=?",
                            (*[v for k, v in updates], now(), pid),
                        )
                        audit(db, "project", pid, "updated")
                    return rowdict(
                        db.execute(
                            "SELECT * FROM scrape_projects WHERE id=?", (pid,)
                        ).fetchone()
                    ), 200
                if sub == "sources" and method == "GET":
                    return [
                        dict(x)
                        for x in db.execute(
                            "SELECT ps.*,s.name,s.host,s.allowed_paths AS source_allowed_paths,s.min_interval_seconds,s.policy_review_due AS source_policy_review_due,s.adapter FROM project_sources ps JOIN sources s ON s.id=ps.source_id WHERE ps.project_id=?",
                            (pid,),
                        )
                    ], 200
                if sub == "sources" and method == "PUT":
                    d = body(self)
                    sid = subid
                    source = db.execute(
                        "SELECT * FROM sources WHERE id=?", (sid,)
                    ).fetchone()
                    if not source:
                        raise LookupError("Source not found")
                    approved = json.loads(source["allowed_paths"])
                    paths = d.get("allowedPaths", [])
                    fields = d.get("selectedFields", [])
                    pacing = int(d.get("pacingSeconds", source["min_interval_seconds"]))
                    if not paths or any(not path_in_scope(p, approved) for p in paths):
                        raise ValueError(
                            "Project paths must stay within source-approved paths"
                        )
                    if pacing < source["min_interval_seconds"]:
                        raise ValueError(
                            "Pacing cannot be faster than the source minimum"
                        )
                    allowed_fields = {
                        "name",
                        "role",
                        "title",
                        "company",
                        "location",
                        "professional_url",
                        "industry",
                        "seniority",
                        "public_bio",
                        "professional_id",
                        "specialty",
                        "practice_address",
                        "practice_phone",
                        "practice_email",
                    }
                    if not fields or any(f not in allowed_fields for f in fields):
                        raise ValueError(
                            "Select only approved professional-information fields"
                        )
                    project_due = d.get("policyReviewDue", source["policy_review_due"])
                    if (
                        source["policy_review_due"]
                        and project_due
                        and project_due > source["policy_review_due"]
                    ):
                        raise ValueError(
                            "A project review date cannot extend the source-approved policy review date."
                        )
                    if source["adapter"] == "ans-fhir" and set(paths) != {
                        "/fhir/v2/Practitioner",
                        "/fhir/v2/_page",
                    }:
                        raise ValueError(
                            "The ANS adapter requires its exact Practitioner search and _page continuation endpoints."
                        )
                    if source["adapter"] == "ans-fhir" and any(
                        f
                        not in {
                            "name",
                            "professional_id",
                            "role",
                            "specialty",
                            "professional_url",
                            "company",
                            "location",
                        }
                        for f in fields
                    ):
                        raise ValueError(
                            "ANS fields are limited to practitioner and practice fields available from Practitioner/PractitionerRole. Secure messaging contacts are not available for prospecting."
                        )
                    query = d.get("query", {})
                    if not isinstance(query, dict) or set(query) - {
                        "name",
                        "identifier",
                        "qualification-code",
                        "active",
                    }:
                        raise ValueError(
                            "ANS project filters may use name, identifier, qualification-code and active only."
                        )
                    if any(
                        not isinstance(v, str) or len(v) > 4000 for v in query.values()
                    ):
                        raise ValueError(
                            "Each ANS filter must be text up to 4,000 characters."
                        )
                    if "active" in query and query["active"] not in ("true", "false"):
                        raise ValueError("ANS active filter must be true or false.")
                    if source["adapter"] == "ans-fhir" and not query:
                        query = {"active": "true"}
                    db.execute(
                        "INSERT INTO project_sources(id,project_id,source_id,allowed_paths,selected_fields,pacing_seconds,policy_review_due,query_json) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(project_id,source_id) DO UPDATE SET allowed_paths=excluded.allowed_paths,selected_fields=excluded.selected_fields,pacing_seconds=excluded.pacing_seconds,policy_review_due=excluded.policy_review_due,query_json=excluded.query_json",
                        (
                            str(uuid.uuid4()),
                            pid,
                            sid,
                            json.dumps(paths),
                            json.dumps(fields),
                            pacing,
                            project_due,
                            json.dumps(query),
                        ),
                    )
                    audit(db, "project", pid, "source_configured")
                    return {"ok": True}, 200
                if sub == "sources" and method == "DELETE" and subid:
                    db.execute(
                        "DELETE FROM project_sources WHERE project_id=? AND source_id=?",
                        (pid, subid),
                    )
                    audit(db, "project", pid, "source_unlinked")
                    return {"ok": True}, 200
                if sub == "readiness" and method == "GET":
                    return self.readiness(db, pid), 200
                if sub == "summary" and method == "GET":
                    statuses = {
                        x["status"]: x["n"]
                        for x in db.execute(
                            "SELECT status,COUNT(*) n FROM scrape_jobs WHERE project_id=? GROUP BY status",
                            (pid,),
                        )
                    }
                    events = db.execute(
                        "SELECT COUNT(*) FROM agenda_entries WHERE project_id=? AND start_at>=? AND status='scheduled'",
                        (pid, now()),
                    ).fetchone()[0]
                    records = db.execute(
                        "SELECT COUNT(*) total,SUM(CASE WHEN status='needs_review' THEN 1 ELSE 0 END) needs_review FROM scrape_records WHERE project_id=?",
                        (pid,),
                    ).fetchone()
                    total_jobs = sum(statuses.values())
                    done = sum(
                        statuses.get(x, 0) for x in ("completed", "needs_review")
                    )
                    completion = int(done * 100 / total_jobs) if total_jobs else 0
                    return {
                        "project": dict(project),
                        "runsByStatus": statuses,
                        "scheduledEvents": events,
                        "sourceCount": db.execute(
                            "SELECT COUNT(*) FROM project_sources WHERE project_id=?",
                            (pid,),
                        ).fetchone()[0],
                        "recordsFound": records["total"] or 0,
                        "needsReview": records["needs_review"] or 0,
                        "completionPercent": completion,
                    }, 200
                if sub == "prepare" and method == "POST":
                    d = body(self)
                    limit = min(1000, max(1, int(d.get("limit", 1000))))
                    pending = db.execute(
                        "SELECT id,fields_json FROM scrape_records WHERE project_id=? AND prepared_at IS NULL ORDER BY created_at,id LIMIT ?",
                        (pid, limit),
                    ).fetchall()
                    prepared_at = now()
                    duplicate_count = 0
                    low_quality = 0
                    for record in pending:
                        try:
                            raw_fields = json.loads(record["fields_json"] or "{}")
                        except (ValueError, TypeError):
                            raw_fields = {}
                        normalized, flat, score, issues, fingerprint = prepare_profile(
                            raw_fields
                        )
                        duplicate = (
                            db.execute(
                                "SELECT id FROM scrape_records WHERE project_id=? AND fingerprint=? AND prepared_at IS NOT NULL AND duplicate_of IS NULL ORDER BY created_at,id LIMIT 1",
                                (pid, fingerprint),
                            ).fetchone()
                            if fingerprint
                            else None
                        )
                        duplicate_id = duplicate["id"] if duplicate else None
                        duplicate_count += int(bool(duplicate_id))
                        low_quality += int(score < 70)
                        db.execute(
                            "UPDATE scrape_records SET normalized_json=?,normalized_name=?,normalized_professional_id=?,normalized_role=?,normalized_specialty=?,normalized_company=?,normalized_location=?,quality_score=?,quality_issues=?,prepared_at=?,fingerprint=?,duplicate_of=?,updated_at=? WHERE id=?",
                            (
                                json.dumps(normalized, ensure_ascii=False),
                                flat["name"],
                                flat["professional_id"],
                                flat["role"],
                                flat["specialty"],
                                flat["company"],
                                flat["location"],
                                score,
                                json.dumps(issues),
                                prepared_at,
                                fingerprint,
                                duplicate_id,
                                prepared_at,
                                record["id"],
                            ),
                        )
                    if pending:
                        audit(db, "project", pid, "records_prepared")
                    remaining = db.execute(
                        "SELECT COUNT(*) FROM scrape_records WHERE project_id=? AND prepared_at IS NULL",
                        (pid,),
                    ).fetchone()[0]
                    return {
                        "processed": len(pending),
                        "duplicates": duplicate_count,
                        "lowQuality": low_quality,
                        "remaining": remaining,
                        "limit": limit,
                    }, 200
                if sub == "analysis" and method == "GET":
                    total = db.execute(
                        "SELECT COUNT(*) FROM scrape_records WHERE project_id=?", (pid,)
                    ).fetchone()[0]
                    prepared = db.execute(
                        "SELECT COUNT(*) FROM scrape_records WHERE project_id=? AND prepared_at IS NOT NULL",
                        (pid,),
                    ).fetchone()[0]
                    duplicate_count = db.execute(
                        "SELECT COUNT(*) FROM scrape_records WHERE project_id=? AND duplicate_of IS NOT NULL",
                        (pid,),
                    ).fetchone()[0]
                    quality = db.execute(
                        "SELECT AVG(quality_score),SUM(CASE WHEN quality_score<70 THEN 1 ELSE 0 END) FROM scrape_records WHERE project_id=? AND prepared_at IS NOT NULL",
                        (pid,),
                    ).fetchone()
                    statuses = {
                        x["status"]: x["n"]
                        for x in db.execute(
                            "SELECT status,COUNT(*) n FROM scrape_records WHERE project_id=? GROUP BY status",
                            (pid,),
                        )
                    }

                    def breakdown(column):
                        return [
                            dict(x)
                            for x in db.execute(
                                "SELECT "
                                + column
                                + " value,COUNT(*) count FROM scrape_records WHERE project_id=? AND prepared_at IS NOT NULL AND "
                                + column
                                + " IS NOT NULL AND "
                                + column
                                + "<>'' GROUP BY "
                                + column
                                + " ORDER BY count DESC,value LIMIT 8",
                                (pid,),
                            )
                        ]

                    return {
                        "total": total,
                        "prepared": prepared,
                        "remaining": total - prepared,
                        "duplicates": duplicate_count,
                        "lowQuality": quality[1] or 0,
                        "averageQuality": round(quality[0], 1)
                        if quality[0] is not None
                        else None,
                        "reviewStatuses": statuses,
                        "specialties": breakdown("normalized_specialty"),
                        "roles": breakdown("normalized_role"),
                        "locations": breakdown("normalized_location"),
                    }, 200
                if sub == "runs" and method == "GET":
                    clauses = ["project_id=?"]
                    args = [pid]
                    if q.get("status"):
                        clauses.append("status=?")
                        args.append(q["status"][0])
                    if q.get("from"):
                        clauses.append("COALESCE(scheduled_at,created_at)>=?")
                        args.append(dt(q["from"][0]))
                    if q.get("to"):
                        clauses.append("COALESCE(scheduled_at,created_at)<=?")
                        args.append(dt(q["to"][0]))
                    limit = min(100, int(q.get("limit", [20])[0]))
                    offset = max(0, int(q.get("offset", [0])[0]))
                    rows = db.execute(
                        "SELECT j.*,s.name source_name,s.adapter,p.name project_name,(SELECT COUNT(*) FROM scrape_records r WHERE r.job_id=j.id) record_count,(SELECT a.id FROM agenda_entries a WHERE a.job_id=j.id LIMIT 1) agenda_event_id FROM scrape_jobs j JOIN sources s ON s.id=j.source_id JOIN scrape_projects p ON p.id=j.project_id WHERE "
                        + " AND ".join("j." + c for c in clauses)
                        + " ORDER BY COALESCE(j.scheduled_at,j.created_at) DESC LIMIT ? OFFSET ?",
                        (*args, limit, offset),
                    )
                    return [dict(x) for x in rows], 200
                if sub == "runs" and method == "POST":
                    d = body(self)
                    readiness = self.readiness(db, pid)
                    if not readiness["ready"]:
                        raise ValueError(
                            "Project is not ready: "
                            + "; ".join(x["message"] for x in readiness["issues"])
                        )
                    sid = d.get("sourceId")
                    cfg = db.execute(
                        "SELECT * FROM project_sources WHERE project_id=? AND source_id=?",
                        (pid, sid),
                    ).fetchone()
                    if not cfg:
                        raise ValueError("Choose a source linked to this project")
                    search = json.loads(cfg["query_json"] or "{}")
                    if d.get("search"):
                        if not isinstance(d["search"], dict) or set(d["search"]) - {
                            "name",
                            "identifier",
                            "qualification-code",
                            "active",
                        }:
                            raise ValueError(
                                "Search may use name, identifier, qualification-code and active only."
                            )
                        if any(
                            not isinstance(v, str) or len(v) > 4000
                            for v in d["search"].values()
                        ):
                            raise ValueError(
                                "Each ANS search filter must be text up to 4,000 characters."
                            )
                        if "active" in d["search"] and d["search"]["active"] not in (
                            "true",
                            "false",
                        ):
                            raise ValueError("ANS active filter must be true or false.")
                        if (
                            sid
                            and db.execute(
                                "SELECT adapter FROM sources WHERE id=?", (sid,)
                            ).fetchone()["adapter"]
                            == "ans-fhir"
                        ):
                            search.update({k: v for k, v in d["search"].items() if v})
                    if db.execute(
                        "SELECT adapter FROM sources WHERE id=?", (sid,)
                    ).fetchone()["adapter"] == "ans-fhir" and not any(
                        k in search
                        for k in ("name", "identifier", "qualification-code")
                    ):
                        raise ValueError(
                            "Add a name, RPPS identifier or qualification code filter before querying the ANS directory."
                        )
                    key = d.get("idempotencyKey") or self.headers.get("Idempotency-Key")
                    if not key:
                        raise ValueError("Idempotency-Key is required")
                    existing = db.execute(
                        "SELECT * FROM scrape_jobs WHERE idempotency_key=?", (key,)
                    ).fetchone()
                    if existing:
                        return {
                            "job": dict(existing),
                            "agendaEventId": db.execute(
                                "SELECT id FROM agenda_entries WHERE job_id=?",
                                (existing["id"],),
                            ).fetchone()["id"],
                        }, 200
                    jid = str(uuid.uuid4())
                    scheduled = dt(d["scheduledAt"]) if d.get("scheduledAt") else now()
                    status = "scheduled" if scheduled > now() else "queued"
                    t = now()
                    db.execute(
                        "INSERT INTO scrape_jobs(id,project_id,source_id,status,selected_scope,selected_fields,scheduled_at,idempotency_key,search_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            jid,
                            pid,
                            sid,
                            status,
                            cfg["allowed_paths"],
                            cfg["selected_fields"],
                            scheduled,
                            key,
                            json.dumps(search),
                            t,
                            t,
                        ),
                    )
                    eid = str(uuid.uuid4())
                    start = scheduled
                    end = (
                        dt(d["endAt"])
                        if d.get("endAt")
                        else (
                            datetime.fromisoformat(start.replace("Z", "+00:00"))
                            + timedelta(hours=1)
                        )
                        .isoformat(timespec="seconds")
                        .replace("+00:00", "Z")
                    )
                    db.execute(
                        "INSERT INTO agenda_entries(id,title,event_type,start_at,end_at,timezone,project_id,source_id,job_id,owner_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            eid,
                            "Scraping run Â· " + project["name"],
                            "scrape_run",
                            start,
                            end,
                            project["timezone"],
                            pid,
                            sid,
                            jid,
                            project["owner_id"],
                            t,
                            t,
                        ),
                    )
                    audit(db, "job", jid, "scheduled")
                    return {
                        "job": rowdict(
                            db.execute(
                                "SELECT * FROM scrape_jobs WHERE id=?", (jid,)
                            ).fetchone()
                        ),
                        "agendaEventId": eid,
                    }, 201
            records_route = re.fullmatch(
                r"/api/projects/([^/]+)/records(?:/([^/]+)/review)?", path
            )
            if records_route:
                pid, rid = records_route.groups()
                project = db.execute(
                    "SELECT owner_id FROM scrape_projects WHERE id=?", (pid,)
                ).fetchone()
                if not project or project["owner_id"] != self.user["id"]:
                    raise LookupError("Project not found")
                if not rid and method == "GET":
                    where = ["r.project_id=?"]
                    args = [pid]
                    if q.get("status"):
                        where.append("r.status=?")
                        args.append(q["status"][0])
                    if q.get("q"):
                        where.append("COALESCE(r.normalized_json,r.fields_json) LIKE ?")
                        args.append("%" + q["q"][0] + "%")
                    limit = min(100, max(1, int(q.get("limit", [25])[0])))
                    offset = max(0, int(q.get("offset", [0])[0]))
                    rows = [
                        dict(x)
                        for x in db.execute(
                            "SELECT r.*,s.name source_name,j.status run_status FROM scrape_records r JOIN sources s ON s.id=r.source_id JOIN scrape_jobs j ON j.id=r.job_id WHERE "
                            + " AND ".join(where)
                            + " ORDER BY r.created_at DESC LIMIT ? OFFSET ?",
                            (*args, limit, offset),
                        )
                    ]
                    for row in rows:
                        row["fields"] = json.loads(row.pop("fields_json") or "{}")
                        row["preparedFields"] = json.loads(
                            row.pop("normalized_json") or "{}"
                        )
                        row.pop("fields_json", None)
                        row["qualityIssues"] = json.loads(
                            row.pop("quality_issues") or "[]"
                        )
                    total = db.execute(
                        "SELECT COUNT(*) FROM scrape_records r WHERE "
                        + " AND ".join(where),
                        args,
                    ).fetchone()[0]
                    return {
                        "items": rows,
                        "total": total,
                        "limit": limit,
                        "offset": offset,
                    }, 200
                if rid and method == "POST":
                    d = body(self)
                    status = d.get("status")
                    note = (d.get("note") or "").strip()
                    if status not in ("resolved", "verified", "flagged"):
                        raise ValueError("Choose resolved, verified or flagged.")
                    record = db.execute(
                        "SELECT id FROM scrape_records WHERE id=? AND project_id=?",
                        (rid, pid),
                    ).fetchone()
                    if not record:
                        raise LookupError("Record not found")
                    db.execute(
                        "UPDATE scrape_records SET status=?,reviewed_by=?,review_note=?,updated_at=? WHERE id=?",
                        (status, self.user["id"], note, now(), rid),
                    )
                    audit(db, "record", rid, "review_" + status)
                    return {"ok": True, "status": status}, 200
            if path == "/api/agenda" and method == "GET":
                where = ["owner_id=?"]
                args = [self.user["id"]]
                for key, col in (
                    ("projectId", "project_id"),
                    ("sourceId", "source_id"),
                    ("ownerId", "owner_id"),
                    ("type", "event_type"),
                    ("status", "status"),
                ):
                    if q.get(key):
                        where.append(col + "=?")
                        args.append(q[key][0])
                if q.get("from"):
                    where.append("end_at>=?")
                    args.append(dt(q["from"][0]))
                if q.get("to"):
                    where.append("start_at<=?")
                    args.append(dt(q["to"][0]))
                rows = [
                    dict(x)
                    for x in db.execute(
                        "SELECT a.*,p.name project_name,s.name source_name,j.status job_status FROM agenda_entries a LEFT JOIN scrape_projects p ON p.id=a.project_id LEFT JOIN sources s ON s.id=a.source_id LEFT JOIN scrape_jobs j ON j.id=a.job_id"
                        + (
                            " WHERE " + " AND ".join("a." + w for w in where)
                            if where
                            else ""
                        )
                        + " ORDER BY start_at LIMIT ? OFFSET ?",
                        (
                            *args,
                            min(500, int(q.get("limit", [100])[0])),
                            max(0, int(q.get("offset", [0])[0])),
                        ),
                    )
                ]
                for ev in rows:
                    ev["conflicts"] = []
                    if ev["status"] != "scheduled":
                        continue
                    overlaps = db.execute(
                        "SELECT id,title,event_type FROM agenda_entries WHERE id<>? AND status='scheduled' AND owner_id=? AND start_at<? AND end_at>?",
                        (ev["id"], ev["owner_id"], ev["end_at"], ev["start_at"]),
                    ).fetchall()
                    ev["conflicts"].extend(
                        {
                            "code": "assignment_overlap",
                            "message": "Overlaps another scheduled assignment: "
                            + x["title"],
                            "eventId": x["id"],
                        }
                        for x in overlaps
                    )
                    if ev["source_id"]:
                        src = db.execute(
                            "SELECT min_interval_seconds FROM sources WHERE id=?",
                            (ev["source_id"],),
                        ).fetchone()
                        if src:
                            interval = src["min_interval_seconds"]
                            near = db.execute(
                                "SELECT id,title FROM agenda_entries WHERE id<>? AND source_id=? AND status='scheduled' AND start_at>=? AND start_at<=?",
                                (
                                    ev["id"],
                                    ev["source_id"],
                                    (
                                        datetime.fromisoformat(
                                            ev["start_at"].replace("Z", "+00:00")
                                        )
                                        - timedelta(seconds=interval)
                                    )
                                    .isoformat(timespec="seconds")
                                    .replace("+00:00", "Z"),
                                    (
                                        datetime.fromisoformat(
                                            ev["start_at"].replace("Z", "+00:00")
                                        )
                                        + timedelta(seconds=interval)
                                    )
                                    .isoformat(timespec="seconds")
                                    .replace("+00:00", "Z"),
                                ),
                            ).fetchall()
                            ev["conflicts"].extend(
                                {
                                    "code": "source_rate_limit",
                                    "message": "Too close to another event for this source: "
                                    + x["title"],
                                    "eventId": x["id"],
                                }
                                for x in near
                            )
                    if ev["project_id"]:
                        project = db.execute(
                            "SELECT start_date,end_date FROM scrape_projects WHERE id=?",
                            (ev["project_id"],),
                        ).fetchone()
                        day = ev["start_at"][:10]
                        if project and (
                            (project["start_date"] and day < project["start_date"])
                            or (project["end_date"] and day > project["end_date"])
                        ):
                            ev["conflicts"].append(
                                {
                                    "code": "project_window",
                                    "message": "Event is outside the project date window.",
                                }
                            )
                return rows, 200
            if path == "/api/agenda" and method == "POST":
                d = body(self)
                start = dt(d.get("startAt"))
                end = dt(d.get("endAt"))
                if end <= start:
                    raise ValueError("Event end must be after its start")
                eid = str(uuid.uuid4())
                t = now()
                typ = d.get("type", "team_task")
                if typ not in (
                    "milestone",
                    "scrape_run",
                    "source_review",
                    "quality_review",
                    "team_task",
                    "follow_up",
                ):
                    raise ValueError("Unsupported event type")
                if d.get("projectId"):
                    p = db.execute(
                        "SELECT owner_id FROM scrape_projects WHERE id=?",
                        (d["projectId"],),
                    ).fetchone()
                    if not p or p["owner_id"] != self.user["id"]:
                        raise LookupError("Project not found")
                db.execute(
                    "INSERT INTO agenda_entries(id,title,description,event_type,start_at,end_at,timezone,project_id,source_id,owner_id,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        eid,
                        d.get("title", "Untitled event"),
                        d.get("description", ""),
                        typ,
                        start,
                        end,
                        d.get("timezone", "UTC"),
                        d.get("projectId"),
                        d.get("sourceId"),
                        self.user["id"],
                        d.get("status", "scheduled"),
                        t,
                        t,
                    ),
                )
                audit(db, "agenda", eid, "created")
                return rowdict(
                    db.execute(
                        "SELECT * FROM agenda_entries WHERE id=?", (eid,)
                    ).fetchone()
                ), 201
            m = re.fullmatch(r"/api/agenda/([^/]+)", path)
            if m:
                eid = m.group(1)
                event = db.execute(
                    "SELECT * FROM agenda_entries WHERE id=?", (eid,)
                ).fetchone()
                if not event or event["owner_id"] != self.user["id"]:
                    raise LookupError("Agenda event not found")
                if method == "GET":
                    return dict(event), 200
                if method == "PATCH":
                    d = body(self)
                    cols = {
                        "title": "title",
                        "description": "description",
                        "type": "event_type",
                        "startAt": "start_at",
                        "endAt": "end_at",
                        "timezone": "timezone",
                        "status": "status",
                    }
                    updates = []
                    for k, v in d.items():
                        if k in cols:
                            updates.append(
                                (cols[k], dt(v) if k in ("startAt", "endAt") else v)
                            )
                    mapped = dict((k, v) for k, v in updates)
                    start = mapped.get("start_at", event["start_at"])
                    end = mapped.get("end_at", event["end_at"])
                    if end <= start:
                        raise ValueError("Event end must be after its start")
                    if event["job_id"] and mapped.get("status") == "cancelled":
                        return {
                            "error": "Cancel a linked run through the run cancellation action so both states stay synchronized.",
                            "confirmationRequired": True,
                        }, 409
                    if event["job_id"] and ("start_at" in mapped or "end_at" in mapped):
                        linked = db.execute(
                            "SELECT status FROM scrape_jobs WHERE id=?",
                            (event["job_id"],),
                        ).fetchone()
                        if linked and linked["status"] == "running":
                            raise ValueError(
                                "This run is already active. Cancel it safely before changing its schedule."
                            )
                        db.execute(
                            "UPDATE scrape_jobs SET scheduled_at=?,status=CASE WHEN ? > ? THEN 'scheduled' ELSE 'queued' END,updated_at=? WHERE id=? AND status IN ('scheduled','queued')",
                            (start, start, now(), now(), event["job_id"]),
                        )
                    if updates:
                        db.execute(
                            "UPDATE agenda_entries SET "
                            + ", ".join(k + "=?" for k, v in updates)
                            + ",updated_at=? WHERE id=?",
                            (*[v for k, v in updates], now(), eid),
                        )
                        audit(db, "agenda", eid, "updated")
                    return rowdict(
                        db.execute(
                            "SELECT * FROM agenda_entries WHERE id=?", (eid,)
                        ).fetchone()
                    ), 200
                if method == "DELETE":
                    cancel = body(self).get("cancelLinkedRun", False)
                    if event["job_id"]:
                        job = db.execute(
                            "SELECT * FROM scrape_jobs WHERE id=?", (event["job_id"],)
                        ).fetchone()
                        if (
                            job
                            and job["status"] in ("scheduled", "queued", "running")
                            and not cancel
                        ):
                            return {
                                "error": "Linked run is " + job["status"],
                                "confirmationRequired": True,
                                "message": "Confirm whether to cancel the linked scraping run.",
                            }, 409
                        if job and cancel:
                            if job["status"] == "running":
                                db.execute(
                                    "UPDATE scrape_jobs SET cancel_requested=1,updated_at=? WHERE id=?",
                                    (now(), job["id"]),
                                )
                                audit(db, "job", job["id"], "cancellation_requested")
                                return {
                                    "cancelRequested": True,
                                    "status": "running",
                                    "message": "The worker will stop at its next safe checkpoint; the agenda event remains until it confirms cancellation.",
                                }, 202
                            db.execute(
                                "UPDATE scrape_jobs SET status='cancelled',cancelled_at=?,updated_at=? WHERE id=?",
                                (now(), now(), job["id"]),
                            )
                    db.execute("DELETE FROM agenda_entries WHERE id=?", (eid,))
                    audit(db, "agenda", eid, "deleted")
                    return {"ok": True}, 200
            if (
                path.startswith("/api/scrape-jobs/")
                and path.endswith("/cancel")
                and method == "POST"
            ):
                jid = path.split("/")[-2]
                job = db.execute(
                    "SELECT * FROM scrape_jobs WHERE id=?", (jid,)
                ).fetchone()
                if not job:
                    raise LookupError("Run not found")
                owner = db.execute(
                    "SELECT owner_id FROM scrape_projects WHERE id=?",
                    (job["project_id"],),
                ).fetchone()
                if not owner or owner["owner_id"] != self.user["id"]:
                    raise LookupError("Run not found")
                if job["status"] == "running":
                    db.execute(
                        "UPDATE scrape_jobs SET cancel_requested=1,updated_at=? WHERE id=?",
                        (now(), jid),
                    )
                    audit(db, "job", jid, "cancellation_requested")
                    return {"ok": True, "cancelRequested": True}, 202
                db.execute(
                    "UPDATE scrape_jobs SET status='cancelled',cancelled_at=?,updated_at=? WHERE id=?",
                    (now(), now(), jid),
                )
                db.execute(
                    "UPDATE agenda_entries SET status='cancelled',updated_at=? WHERE job_id=?",
                    (now(), jid),
                )
                audit(db, "job", jid, "cancelled")
                return {"ok": True}, 200
            if (
                path.startswith("/api/scrape-jobs/")
                and path.endswith("/retry")
                and method == "POST"
            ):
                jid = path.split("/")[-2]
                old = db.execute(
                    "SELECT * FROM scrape_jobs WHERE id=?", (jid,)
                ).fetchone()
                if not old:
                    raise LookupError("Run not found")
                parent = db.execute(
                    "SELECT owner_id FROM scrape_projects WHERE id=?",
                    (old["project_id"],),
                ).fetchone()
                if not parent or parent["owner_id"] != self.user["id"]:
                    raise LookupError("Run not found")
                if old["status"] != "failed":
                    raise ValueError("Only failed runs can be retried")
                if old["retry_count"] >= old["max_retries"]:
                    raise ValueError("The retry limit for this run has been reached")
                key = body(self).get("idempotencyKey") or self.headers.get(
                    "Idempotency-Key"
                )
                if not key:
                    raise ValueError("Idempotency-Key is required")
                existing = db.execute(
                    "SELECT * FROM scrape_jobs WHERE idempotency_key=?", (key,)
                ).fetchone()
                if existing:
                    return {
                        "job": dict(existing),
                        "agendaEventId": db.execute(
                            "SELECT id FROM agenda_entries WHERE job_id=?",
                            (existing["id"],),
                        ).fetchone()["id"],
                    }, 200
                pid = old["project_id"]
                project = db.execute(
                    "SELECT * FROM scrape_projects WHERE id=?", (pid,)
                ).fetchone()
                readiness = self.readiness(db, pid)
                if not readiness["ready"]:
                    raise ValueError(
                        "Project is not ready: "
                        + "; ".join(x["message"] for x in readiness["issues"])
                    )
                cfg = db.execute(
                    "SELECT * FROM project_sources WHERE project_id=? AND source_id=?",
                    (pid, old["source_id"]),
                ).fetchone()
                if not cfg:
                    raise ValueError(
                        "The original source is no longer linked to this project"
                    )
                t = now()
                new_id = str(uuid.uuid4())
                db.execute(
                    "INSERT INTO scrape_jobs(id,project_id,source_id,status,selected_scope,selected_fields,scheduled_at,retry_count,max_retries,idempotency_key,search_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        new_id,
                        pid,
                        old["source_id"],
                        "queued",
                        cfg["allowed_paths"],
                        cfg["selected_fields"],
                        t,
                        old["retry_count"] + 1,
                        old["max_retries"],
                        key,
                        old["search_json"],
                        t,
                        t,
                    ),
                )
                event_id = str(uuid.uuid4())
                db.execute(
                    "INSERT INTO agenda_entries(id,title,event_type,start_at,end_at,timezone,project_id,source_id,job_id,owner_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        event_id,
                        "Retry Â· " + project["name"],
                        "scrape_run",
                        t,
                        (
                            datetime.fromisoformat(t.replace("Z", "+00:00"))
                            + timedelta(hours=1)
                        )
                        .isoformat(timespec="seconds")
                        .replace("+00:00", "Z"),
                        project["timezone"],
                        pid,
                        old["source_id"],
                        new_id,
                        project["owner_id"],
                        t,
                        t,
                    ),
                )
                audit(db, "job", new_id, "retry_created")
                return {
                    "job": rowdict(
                        db.execute(
                            "SELECT * FROM scrape_jobs WHERE id=?", (new_id,)
                        ).fetchone()
                    ),
                    "agendaEventId": event_id,
                }, 201
            return {"error": "Route not found"}, 404

    def readiness(self, db, pid):
        issues = []
        project = db.execute(
            "SELECT status FROM scrape_projects WHERE id=?", (pid,)
        ).fetchone()
        if project and project["status"] == "paused":
            issues.append(
                {
                    "code": "project_paused",
                    "message": "Resume the project before scheduling new runs.",
                }
            )
        rows = db.execute(
            "SELECT ps.*,s.name,s.allowed_paths source_paths,s.min_interval_seconds,s.terms_reviewed_at,s.robots_reviewed_at,s.policy_review_due source_due,s.adapter,s.base_url,s.terms_url FROM project_sources ps JOIN sources s ON s.id=ps.source_id WHERE ps.project_id=?",
            (pid,),
        ).fetchall()
        if not rows:
            issues.append(
                {
                    "code": "source_missing",
                    "message": "Link an approved source to this project.",
                }
            )
        for s in rows:
            if s["adapter"] not in ("synthetic-empty", "public-html", "ans-fhir"):
                issues.append(
                    {
                        "code": "adapter_unsupported",
                        "sourceId": s["source_id"],
                        "message": "This source adapter is not supported by the worker.",
                    }
                )
            if s["adapter"] == "public-html" and (
                not s["base_url"] or not s["terms_url"]
            ):
                issues.append(
                    {
                        "code": "source_configuration",
                        "sourceId": s["source_id"],
                        "message": "Set the exact HTTPS host and same-host terms link for this source.",
                    }
                )
            if s["adapter"] == "ans-fhir":
                cutoff = (
                    (datetime.now(timezone.utc) - timedelta(seconds=30))
                    .isoformat(timespec="seconds")
                    .replace("+00:00", "Z")
                )
                online = db.execute(
                    "SELECT COUNT(*) FROM worker_heartbeats WHERE last_seen_at>=?",
                    (cutoff,),
                ).fetchone()[0]
                configured = db.execute(
                    "SELECT COUNT(*) FROM worker_heartbeats WHERE last_seen_at>=? AND worker_version LIKE '%ans_fhir=ready%'",
                    (cutoff,),
                ).fetchone()[0]
                if online and not configured:
                    issues.append(
                        {
                            "code": "worker_api_key_missing",
                            "sourceId": s["source_id"],
                            "message": "The online worker has no ANS API key. Restart a worker with ANS_FHIR_API_KEY set before scheduling.",
                        }
                    )
            if not json.loads(s["selected_fields"]):
                issues.append(
                    {
                        "code": "fields_missing",
                        "sourceId": s["source_id"],
                        "message": "Select professional-information fields for "
                        + s["name"]
                        + ".",
                    }
                )
            if not json.loads(s["allowed_paths"]):
                issues.append(
                    {
                        "code": "scope_missing",
                        "sourceId": s["source_id"],
                        "message": "Set at least one allowed path for "
                        + s["name"]
                        + ".",
                    }
                )
            if (
                s["adapter"] == "ans-fhir"
                and set(json.loads(s["selected_fields"]))
                & {"practice_address", "practice_phone", "practice_email"}
                and "/fhir/v2/Organization" not in json.loads(s["allowed_paths"])
            ):
                issues.append(
                    {
                        "code": "organization_scope_missing",
                        "sourceId": s["source_id"],
                        "message": "Include the approved ANS Organization endpoint in this project scope to collect practice address or work contact details.",
                    }
                )
            if s["pacing_seconds"] < s["min_interval_seconds"]:
                issues.append(
                    {
                        "code": "pacing",
                        "sourceId": s["source_id"],
                        "message": "Pacing is below the approved minimum for "
                        + s["name"]
                        + ".",
                    }
                )
            due = min(
                [x for x in (s["policy_review_due"], s["source_due"]) if x],
                default=None,
            )
            if (
                not s["terms_reviewed_at"]
                or (s["adapter"] != "ans-fhir" and not s["robots_reviewed_at"])
                or not due
                or due < datetime.now(timezone.utc).date().isoformat()
            ):
                issues.append(
                    {
                        "code": "policy_expired",
                        "sourceId": s["source_id"],
                        "message": "Review terms and applicable access policy for "
                        + s["name"]
                        + " before scheduling.",
                    }
                )
        return {"ready": not issues, "issues": issues}


def scheduler():
    while True:
        try:
            with connect() as db:
                db.execute("BEGIN IMMEDIATE")
                due = db.execute(
                    "SELECT id FROM scrape_jobs WHERE status='scheduled' AND scheduled_at<=? AND cancel_requested=0",
                    (now(),),
                ).fetchall()
                for row in due:
                    db.execute(
                        "UPDATE scrape_jobs SET status='queued',updated_at=? WHERE id=? AND status='scheduled'",
                        (now(), row["id"]),
                    )
                db.execute("COMMIT")
        except Exception:
            pass
        time.sleep(4)


if __name__ == "__main__":
    migrate()
    threading.Thread(target=scheduler, daemon=True).start()
    host = os.environ.get("PROSPECTFLOW_HOST", "127.0.0.1")
    port = int(os.environ.get("PORT", "8000"))
    print("ProspectFlow listening at http://%s:%s (worker is offline)" % (host, port))
    ThreadingHTTPServer((host, port), Handler).serve_forever()
