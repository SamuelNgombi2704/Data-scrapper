"""Bounded, restartable worker for queued ProspectFlow scrape jobs.

The synthetic-empty adapter is intentionally no-op. The public-html adapter only
requests public HTTPS pages on the approved exact host, in configured paths, and
obeying that host's robots.txt. It has no login, cookie, proxy, or CAPTCHA bypass.
"""

import argparse
import hashlib
import ipaddress
import json
import os
import signal
import socket
import sqlite3
import threading
import uuid
import math
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from html.parser import HTMLParser
from urllib.error import HTTPError, URLError
from urllib.parse import (
    urljoin,
    urlsplit,
    urlunsplit,
    unquote,
    urlencode,
    quote,
    parse_qsl,
)
from urllib.request import HTTPRedirectHandler, Request, build_opener
from urllib.robotparser import RobotFileParser

from app import audit, connect, now, path_in_scope

VERSION = "0.3.0"
USER_AGENT = "ProspectFlowBot/0.1 (+https://localhost/prospectflow; permitted-public-page-collection)"
MAX_PAGE_BYTES = 2 * 1024 * 1024
MAX_ROBOTS_BYTES = 512 * 1024
MAX_PAGES = 50
MAX_REDIRECTS = 10
ANS_PRACTITIONER_PATH = "/fhir/v2/Practitioner"
ANS_ORGANIZATION_PATH = "/fhir/v2/Organization"
ANS_PAGE_PATH = "/fhir/v2/_page"
ANS_API_PATHS = [ANS_PRACTITIONER_PATH, ANS_ORGANIZATION_PATH, ANS_PAGE_PATH]
REQUEST_TIMEOUT = 15
HEARTBEAT_SECONDS = 5
LEASE_SECONDS = 45
STOP = threading.Event()
WORKER_ID = socket.gethostname() + ":" + str(uuid.uuid4())
WORKER_LOCK = threading.Lock()
ACTIVE = set()


def has_ans_api_key():
    return bool(os.environ.get("ANS_FHIR_API_KEY", "").strip())


class WorkerError(Exception):
    def __init__(self, message, retryable=False):
        super().__init__(message)
        self.retryable = retryable


class Cancelled(Exception):
    pass


class Paused(Exception):
    pass


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class PageParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.links = []
        self.meta = {}
        self.title = ""
        self._title = False
        self._jsonld = False
        self._script = []
        self.jsonld = []

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag.lower() == "a" and a.get("href"):
            self.links.append(a["href"])
        if tag.lower() == "title":
            self._title = True
        if tag.lower() == "meta":
            key = (a.get("property") or a.get("name") or "").lower()
            value = a.get("content")
            if key and value:
                self.meta[key] = value[:2000]
        if tag.lower() == "script" and "ld+json" in a.get("type", "").lower():
            self._jsonld = True
            self._script = []

    def handle_endtag(self, tag):
        if tag.lower() == "title":
            self._title = False
        if tag.lower() == "script" and self._jsonld:
            try:
                self.jsonld.append(json.loads("".join(self._script)))
            except (ValueError, TypeError):
                pass
            self._jsonld = False
            self._script = []

    def handle_data(self, data):
        if self._title:
            self.title = (self.title + " " + data.strip())[:500]
        if self._jsonld and sum(map(len, self._script)) < MAX_PAGE_BYTES:
            self._script.append(data)


def jsonld_people(node):
    found = []
    if isinstance(node, dict):
        typ = node.get("@type", [])
        typ = typ if isinstance(typ, list) else [typ]
        if any(str(x).lower() == "person" for x in typ):
            found.append(node)
        for value in node.values():
            found.extend(jsonld_people(value))
    elif isinstance(node, list):
        for value in node:
            found.extend(jsonld_people(value))
    return found


def text_value(value):
    if isinstance(value, dict):
        if any(
            key in value
            for key in (
                "streetAddress",
                "line",
                "postalCode",
                "addressLocality",
                "city",
                "addressRegion",
                "state",
                "addressCountry",
                "country",
            )
        ):
            lines = value.get("streetAddress") or value.get("line") or []
            if isinstance(lines, str):
                lines = [lines]
            parts = list(lines) + [
                value.get("postalCode"),
                value.get("addressLocality") or value.get("city"),
                value.get("addressRegion") or value.get("state"),
                value.get("addressCountry") or value.get("country"),
            ]
            value = ", ".join(str(part) for part in parts if part)
        else:
            value = (
                value.get("name") or value.get("@id") or value.get("addressLocality")
            )
    if isinstance(value, list):
        value = ", ".join(str(text_value(x)) for x in value if text_value(x))
    if value is None:
        return None
    value = " ".join(str(value).split())[:1000]
    return value or None


def extract_records(page, url, selected):
    people = []
    for node in page.jsonld:
        people.extend(jsonld_people(node))
    records = []
    captured = now()
    for person in people:
        company = person.get("worksFor") or person.get("affiliation")
        address = person.get("address") or person.get("homeLocation")
        practice = company if isinstance(company, dict) else {}
        values = {
            "name": person.get("name"),
            "role": person.get("jobTitle") or person.get("hasOccupation"),
            "title": person.get("jobTitle") or person.get("headline"),
            "company": company,
            "location": address,
            "professional_url": person.get("url") or url,
            "industry": person.get("industry") or person.get("knowsAbout"),
            "seniority": person.get("seniority"),
            "public_bio": person.get("description"),
            # Practice contact values are accepted only from the linked organization
            # object, never from person.telephone/email or home address fields.
            "practice_address": practice.get("address"),
            "practice_phone": practice.get("telephone"),
            "practice_email": practice.get("email"),
        }
        provenance = {}
        for field in selected:
            value = text_value(values.get(field))
            if value:
                provenance[field] = {
                    "value": value,
                    "sourceUrl": url,
                    "capturedAt": captured,
                }
        if provenance:
            records.append(provenance)
    return records


def normalize_url(url, host, allowed_paths):
    try:
        parsed = urlsplit(url)
    except ValueError:
        return None
    try:
        candidate = (
            (parsed.hostname or "").encode("idna").decode("ascii").lower().rstrip(".")
        )
    except UnicodeError:
        return None
    if (
        parsed.scheme != "https"
        or candidate != host
        or parsed.username
        or parsed.password
        or parsed.port not in (None, 443)
    ):
        return None
    path = unquote(parsed.path or "/")
    lowered = path.lower()
    if (
        "\\" in path
        or any(segment in (".", "..") for segment in path.split("/"))
        or "%2e" in lowered
        or "%2f" in lowered
        or "%5c" in lowered
    ):
        return None
    if not path_in_scope(path, allowed_paths):
        return None
    # Strip fragments; retain query values because some public profile routes use them.
    return urlunsplit(("https", host, path, parsed.query, ""))


def normalize_ans_page_url(url, host, allowed_paths):
    """Validate the exact ANS search endpoint and its opaque continuation endpoint."""
    normalized = normalize_url(url, host, allowed_paths)
    if not normalized:
        return None
    parsed = urlsplit(normalized)
    if parsed.path == ANS_PAGE_PATH:
        try:
            pairs = parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True)
        except ValueError:
            return None
        if len(pairs) != 1 or pairs[0][0] != "id" or not 1 <= len(pairs[0][1]) <= 4096:
            return None
    elif parsed.path == ANS_ORGANIZATION_PATH:
        try:
            pairs = parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True)
        except ValueError:
            return None
        params = dict(pairs)
        if (
            len(params) != len(pairs)
            or set(params) - {"_id", "_count"}
            or "_id" not in params
            or not re.fullmatch(r"[A-Za-z0-9.,_-]{1,4096}", params["_id"])
        ):
            return None
        if "_count" in params and params["_count"] != "50":
            return None
    elif parsed.path != ANS_PRACTITIONER_PATH:
        return None
    return normalized


def public_dns(host):
    try:
        answers = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
    except OSError as e:
        raise WorkerError("Source hostname could not be resolved.", True) from e
    addresses = {item[4][0] for item in answers}
    if not addresses:
        raise WorkerError("Source hostname has no address.", True)
    for value in addresses:
        try:
            addr = ipaddress.ip_address(value)
        except ValueError:
            raise WorkerError("Source hostname resolved to an invalid address.")
        if not addr.is_global:
            raise WorkerError(
                "Source hostname resolved to a non-public network address."
            )


def reserve_request(source_id, minimum_interval):
    with connect() as db:
        db.execute("BEGIN IMMEDIATE")
        source = db.execute(
            "SELECT last_request_at,min_interval_seconds FROM sources WHERE id=?",
            (source_id,),
        ).fetchone()
        if not source:
            raise WorkerError("Configured source was removed.")
        interval = max(int(minimum_interval), int(source["min_interval_seconds"]))
        if source["last_request_at"]:
            last = datetime.fromisoformat(
                source["last_request_at"].replace("Z", "+00:00")
            )
            wait = interval - (datetime.now(timezone.utc) - last).total_seconds()
            if wait > 0:
                db.rollback()
                return min(wait, interval)
        stamp = now()
        db.execute(
            "UPDATE sources SET last_request_at=? WHERE id=?", (stamp, source_id)
        )
        db.commit()
    return 0


def fetch(
    url,
    host,
    source_id,
    minimum_interval,
    max_bytes,
    extra_headers=None,
    auth_label=None,
):
    opener = build_opener(NoRedirect)
    redirects = 0
    visited_redirects = {url}
    while True:
        if STOP.is_set():
            raise Cancelled()
        delay = reserve_request(source_id, minimum_interval)
        if delay > 0:
            if STOP.wait(delay):
                raise Cancelled()
            continue
        public_dns(host)
        request_headers = {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/ld+json,text/plain;q=0.8",
            "Accept-Encoding": "identity",
        }
        request_headers.update(extra_headers or {})
        request = Request(url, headers=request_headers, method="GET")
        try:
            response = opener.open(request, timeout=REQUEST_TIMEOUT)
            status = response.status
            headers = response.headers
            payload = response.read(max_bytes + 1)
            response.close()
        except HTTPError as e:
            status = e.code
            headers = e.headers
            payload = e.read(min(max_bytes + 1, 4096))
            e.close()
            if status in (301, 302, 303, 307, 308):
                if redirects >= MAX_REDIRECTS:
                    raise WorkerError("Source exceeded the allowed redirect limit.")
                location = headers.get("Location")
                redirected = (
                    normalize_url(
                        urljoin(url, location or ""), host, [urlsplit(url).path]
                    )
                    if location
                    else None
                )
                if not redirected:
                    raise WorkerError(
                        "Source attempted a redirect outside the approved exact host or path."
                    )
                if redirected in visited_redirects:
                    raise WorkerError(
                        "Source returned a redirect loop at an approved endpoint."
                    )
                visited_redirects.add(redirected)
                redirects += 1
                url = redirected
                continue
            if status in (401, 403):
                if auth_label:
                    raise WorkerError(
                        auth_label
                        + " rejected the API key or application subscription. Verify the key in the ANS portal and restart the worker."
                    )
                raise WorkerError(
                    "Source denied public access. The worker will not bypass access controls."
                )
            if status in (404, 410):
                return status, headers, b""
            if status == 429 or status >= 500:
                raise WorkerError(
                    "Source returned a temporary HTTP error (" + str(status) + ").",
                    True,
                )
            raise WorkerError("Source returned HTTP " + str(status) + ".")
        except (TimeoutError, URLError, OSError) as e:
            raise WorkerError(
                "Network request failed: " + type(e).__name__ + ".", True
            ) from e
        if len(payload) > max_bytes:
            raise WorkerError(
                "Source response exceeded the configured 2 MB page limit."
            )
        return status, headers, payload


def load_robots(source, host, minimum_interval):
    robots_url = "https://" + host + "/robots.txt"
    status, headers, payload = fetch(
        robots_url, host, source["id"], minimum_interval, MAX_ROBOTS_BYTES
    )
    if status in (404, 410):
        lines = []
    elif status != 200:
        raise WorkerError(
            "Could not verify robots.txt; collection stopped.",
            status == 429 or status >= 500,
        )
    else:
        charset = "utf-8"
        match = __import__("re").search(
            r"charset=([\w-]+)", headers.get("Content-Type", ""), __import__("re").I
        )
        if match:
            charset = match.group(1)
        lines = payload.decode(charset, errors="replace").splitlines()
    parser = RobotFileParser(robots_url)
    parser.parse(lines)
    return parser


def job_context(job_id):
    with connect() as db:
        row = db.execute(
            "SELECT j.*,s.name source_name,s.host,s.adapter,s.base_url,s.terms_reviewed_at,s.robots_reviewed_at,s.policy_review_due,s.terms_url,s.min_interval_seconds,p.target_volume,p.status project_status,ps.pacing_seconds,ps.policy_review_due project_policy_due FROM scrape_jobs j JOIN sources s ON s.id=j.source_id JOIN scrape_projects p ON p.id=j.project_id JOIN project_sources ps ON ps.project_id=j.project_id AND ps.source_id=j.source_id WHERE j.id=?",
            (job_id,),
        ).fetchone()
        return dict(row) if row else None


def check_ready(job):
    today = datetime.now(timezone.utc).date().isoformat()
    due = min(
        [x for x in (job.get("project_policy_due"), job.get("policy_review_due")) if x],
        default=None,
    )
    if (
        not job.get("terms_reviewed_at")
        or (job["adapter"] != "ans-fhir" and not job.get("robots_reviewed_at"))
        or not due
        or due < today
    ):
        raise WorkerError("Source terms or policy review is missing or expired.")
    if job["adapter"] not in ("synthetic-empty", "public-html", "ans-fhir"):
        raise WorkerError("Unsupported source adapter.")
    if job["adapter"] == "public-html" and (
        not job.get("base_url") or not job.get("terms_url")
    ):
        raise WorkerError("Source origin or terms URL is missing.")
    if (
        job["adapter"] == "ans-fhir"
        and set(json.loads(job.get("selected_fields") or "[]"))
        & {"practice_address", "practice_phone", "practice_email"}
        and ANS_ORGANIZATION_PATH not in json.loads(job.get("selected_scope") or "[]")
    ):
        raise WorkerError(
            "Select the approved ANS Organization endpoint in project scope before collecting practice contacts."
        )
    if job["adapter"] == "ans-fhir" and not has_ans_api_key():
        raise WorkerError(
            "ANS_FHIR_API_KEY is missing from the worker environment. Add the key locally and restart the worker."
        )
    if job["project_status"] == "paused":
        raise Paused()


def job_control(job_id):
    with connect() as db:
        row = db.execute(
            "SELECT j.cancel_requested,p.status project_status FROM scrape_jobs j JOIN scrape_projects p ON p.id=j.project_id WHERE j.id=?",
            (job_id,),
        ).fetchone()
    if not row or STOP.is_set() or row["cancel_requested"]:
        return "cancel"
    return "pause" if row["project_status"] == "paused" else "continue"


def requeue_paused(job_id):
    with connect() as db:
        db.execute(
            "UPDATE scrape_jobs SET status='queued',worker_id=NULL,heartbeat_at=NULL,updated_at=? WHERE id=? AND status='running'",
            (now(), job_id),
        )


def save_progress(job_id, pages, visited, queue, records):
    checkpoint = json.dumps(
        {"pages": pages, "visited": visited[-100:], "queue": queue[: MAX_PAGES * 4]},
        ensure_ascii=False,
    )
    progress = min(95, int(pages * 100 / max(1, min(MAX_PAGES, records or MAX_PAGES))))
    with connect() as db:
        db.execute(
            "UPDATE scrape_jobs SET progress=?,checkpoint=?,records_found=(SELECT COUNT(*) FROM scrape_records WHERE job_id=?),heartbeat_at=?,updated_at=? WHERE id=? AND status='running' AND worker_id=?",
            (progress, checkpoint, job_id, now(), now(), job_id, WORKER_ID),
        )


def save_record(job, source_url, fields, payload, stable_entity_key=None):
    stamp = now()
    digest = hashlib.sha256(payload).hexdigest()
    entity_key = (
        stable_entity_key
        or hashlib.sha256(
            json.dumps(fields, sort_keys=True, ensure_ascii=False).encode()
        ).hexdigest()
    )
    record_id = str(uuid.uuid4())
    with connect() as db:
        cursor = db.execute(
            "INSERT OR IGNORE INTO scrape_records(id,job_id,project_id,source_id,source_url,content_hash,entity_key,fields_json,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,'needs_review',?,?)",
            (
                record_id,
                job["id"],
                job["project_id"],
                job["source_id"],
                source_url,
                digest,
                entity_key,
                json.dumps(fields, ensure_ascii=False),
                stamp,
                stamp,
            ),
        )
        return cursor.rowcount == 1


def fhir_display(codings):
    for coding in codings or []:
        if coding.get("display"):
            return coding["display"]
        if coding.get("code"):
            return coding["code"]
    return None


def ans_fhir_value(resource, field, role=None, organization=None):
    if field == "name":
        names = resource.get("name") or []
        if names:
            item = names[0]
            if item.get("text"):
                return item["text"]
            parts = (
                item.get("prefix", [])
                + item.get("given", [])
                + ([item["family"]] if item.get("family") else [])
                + item.get("suffix", [])
            )
            return " ".join(x for x in parts if isinstance(x, str)).strip()
    if field == "professional_id":
        for identifier in resource.get("identifier", []):
            system = (identifier.get("system") or "").lower()
            if "rpps" in system or not system:
                return identifier.get("value")
    if field == "company" and role:
        return (organization or {}).get("name") or (role.get("organization") or {}).get(
            "display"
        )
    if field == "practice_address" and organization:
        return text_value(organization.get("address") or [])
    if field in ("practice_phone", "practice_email") and organization:
        wanted = "phone" if field == "practice_phone" else "email"
        for point in organization.get("telecom", []):
            if (
                not isinstance(point, dict)
                or point.get("system") != wanted
                or point.get("use") not in (None, "work")
            ):
                continue
            if wanted == "email" and any(
                "mailbox-mss" in str(ext.get("url", ""))
                for ext in point.get("extension", [])
                if isinstance(ext, dict)
            ):
                continue
            if point.get("value"):
                return point["value"]
    if field == "location" and role:
        locations = role.get("location") or []
        labels = [x.get("display", "") for x in locations if x.get("display")]
        return "; ".join(dict.fromkeys(labels)) or None
    if field in ("specialty", "role") and role:
        specialties = role.get("specialty") or []
        specialty_values = [
            fhir_display(item.get("coding", [])) for item in specialties
        ]
        specialty_values = [x for x in specialty_values if x]
        if field == "specialty" and specialty_values:
            return "; ".join(dict.fromkeys(specialty_values))
        role_codes = role.get("code") or []
        role_values = [fhir_display(item.get("coding", [])) for item in role_codes]
        role_values = [x for x in role_values if x]
        if field == "role" and role_values:
            return "; ".join(dict.fromkeys(role_values))
    if field in ("role", "specialty"):
        qualifications = resource.get("qualification", [])
        values = []
        for qualification in qualifications:
            code = qualification.get("code", {})
            value = fhir_display(code.get("coding", [])) or code.get("text")
            if value:
                values.append(value)
        if values:
            return "; ".join(dict.fromkeys(values))
    if field == "professional_url":
        return "https://gateway.api.esante.gouv.fr/fhir/v2/Practitioner/" + str(
            resource.get("id", "")
        )
    return None


def crawl_ans_fhir(job):
    key = os.environ.get("ANS_FHIR_API_KEY", "").strip()
    if not key:
        raise WorkerError("ANS_FHIR_API_KEY is missing from the worker environment.")
    query = json.loads(job.get("search_json") or "{}")
    if not any(query.get(k) for k in ("name", "identifier", "qualification-code")):
        raise WorkerError(
            "Choose an ANS name, RPPS identifier or qualification code filter."
        )
    params = dict(query)
    params["_count"] = "50"
    params["_revinclude"] = "PractitionerRole:practitioner"
    root_url = job["base_url"].rstrip("/") + "/Practitioner?" + urlencode(params)
    approved = json.loads(job["selected_scope"])
    selected = json.loads(job["selected_fields"])
    target = max(1, min(MAX_PAGES * 50, int(job.get("target_volume") or 50)))
    checkpoint = json.loads(job.get("checkpoint") or "{}")
    queue = list(checkpoint.get("queue") or [])
    url = queue[0] if queue else root_url
    seen = set(checkpoint.get("visited") or [])
    with connect() as db:
        collected = db.execute(
            "SELECT COUNT(*) FROM scrape_records WHERE job_id=?", (job["id"],)
        ).fetchone()[0]
    page = int(checkpoint.get("pages", 0))
    headers = {"ESANTE-API-KEY": key, "Accept": "application/fhir+json"}
    while url and page < MAX_PAGES and collected < target:
        control = job_control(job["id"])
        if control == "cancel":
            raise Cancelled()
        if control == "pause":
            raise Paused()
        normalized = normalize_ans_page_url(url, job["host"], approved)
        if not normalized:
            raise WorkerError(
                "ANS API returned a pagination URL outside the approved Practitioner or _page endpoints."
            )
        status, response_headers, payload = fetch(
            normalized,
            job["host"],
            job["source_id"],
            max(int(job["pacing_seconds"]), int(job["min_interval_seconds"])),
            MAX_PAGE_BYTES,
            headers,
            "ANS FHIR",
        )
        if status != 200:
            raise WorkerError(
                "ANS API returned HTTP " + str(status) + ".",
                status == 429 or status >= 500,
            )
        try:
            bundle = json.loads(payload.decode("utf-8"))
        except (ValueError, UnicodeError) as error:
            raise WorkerError("ANS API returned invalid FHIR JSON.") from error
        if bundle.get("resourceType") != "Bundle":
            raise WorkerError("ANS API response was not a FHIR Bundle.")
        resources = [
            entry.get("resource", {})
            for entry in bundle.get("entry", [])
            if isinstance(entry, dict)
        ]
        practitioners = {
            r.get("id"): r
            for r in resources
            if r.get("resourceType") == "Practitioner" and r.get("id")
        }
        roles = [r for r in resources if r.get("resourceType") == "PractitionerRole"]
        organizations = {}
        if (
            set(selected) & {"practice_address", "practice_phone", "practice_email"}
            and ANS_ORGANIZATION_PATH in approved
        ):
            organization_ids = sorted(
                {
                    str(role.get("organization", {}).get("reference", "")).rsplit(
                        "/", 1
                    )[-1]
                    for role in roles
                    if str(
                        role.get("organization", {}).get("reference", "")
                    ).startswith("Organization/")
                }
            )
            if organization_ids:
                org_url = (
                    job["base_url"].rstrip("/")
                    + "/Organization?"
                    + urlencode({"_id": ",".join(organization_ids), "_count": "50"})
                )
                org_normalized = normalize_ans_page_url(org_url, job["host"], approved)
                if not org_normalized:
                    raise WorkerError(
                        "ANS Organization lookup is outside the approved project scope."
                    )
                org_status, org_headers, org_payload = fetch(
                    org_normalized,
                    job["host"],
                    job["source_id"],
                    max(int(job["pacing_seconds"]), int(job["min_interval_seconds"])),
                    MAX_PAGE_BYTES,
                    headers,
                    "ANS FHIR",
                )
                if org_status != 200:
                    raise WorkerError(
                        "ANS Organization lookup returned HTTP "
                        + str(org_status)
                        + ".",
                        org_status == 429 or org_status >= 500,
                    )
                try:
                    org_bundle = json.loads(org_payload.decode("utf-8"))
                except (ValueError, UnicodeError) as error:
                    raise WorkerError(
                        "ANS Organization lookup returned invalid FHIR JSON."
                    ) from error
                organizations = {
                    x.get("id"): x
                    for x in (
                        e.get("resource", {})
                        for e in org_bundle.get("entry", [])
                        if isinstance(e, dict)
                    )
                    if x.get("resourceType") == "Organization" and x.get("id")
                }
        for practitioner_id, practitioner in practitioners.items():
            source_updated_at = (practitioner.get("meta") or {}).get("lastUpdated")
            if not isinstance(source_updated_at, str) or not source_updated_at.strip():
                continue
            matched = [
                r
                for r in roles
                if str(r.get("practitioner", {}).get("reference", "")).endswith(
                    "/" + practitioner_id
                )
            ]
            for role in matched or [None]:
                organization_id = str(
                    (role or {}).get("organization", {}).get("reference", "")
                ).rsplit("/", 1)[-1]
                organization = organizations.get(organization_id)
                stable_id = next(
                    (
                        x.get("value")
                        for x in practitioner.get("identifier", [])
                        if x.get("value")
                    ),
                    practitioner_id,
                )
                role_id = (role or {}).get("id", "")
                entity_key = hashlib.sha256(
                    (str(stable_id) + "|" + str(role_id)).encode("utf-8")
                ).hexdigest()
                if entity_key in seen:
                    continue
                values = {}
                for field in selected:
                    value = ans_fhir_value(practitioner, field, role, organization)
                    if value:
                        is_organization_field = (
                            field
                            in {"practice_address", "practice_phone", "practice_email"}
                            and organization is not None
                        )
                        resource_type = (
                            "Organization" if is_organization_field else "Practitioner"
                        )
                        resource_id = (
                            organization_id
                            if is_organization_field
                            else practitioner_id
                        )
                        provenance_url = (
                            job["base_url"].rstrip("/")
                            + "/"
                            + resource_type
                            + "/"
                            + quote(str(resource_id), safe="")
                        )
                        updated = (
                            ((organization or {}).get("meta") or {}).get("lastUpdated")
                            if is_organization_field
                            else source_updated_at
                        )
                        values[field] = {
                            "value": str(value)[:1000],
                            "sourceUrl": provenance_url,
                            "source": "ANS " + resource_type,
                            "sourceUpdatedAt": updated or source_updated_at,
                            "capturedAt": now(),
                        }
                if not values:
                    continue
                seen.add(entity_key)
                provenance_url = (
                    job["base_url"].rstrip("/")
                    + "/Practitioner/"
                    + quote(str(practitioner_id), safe="")
                )
                if save_record(job, provenance_url, values, payload, entity_key):
                    collected += 1
                if collected >= target:
                    break
            if collected >= target:
                break
        page += 1
        next_url = next(
            (
                x.get("url")
                for x in bundle.get("link", [])
                if x.get("relation") == "next"
            ),
            None,
        )
        save_progress(
            job["id"],
            page,
            list(seen),
            [next_url] if next_url and collected < target else [],
            target,
        )
        url = next_url
    if url and page >= MAX_PAGES and collected < target:
        raise WorkerError(
            "ANS result reached the 50-page safety limit; narrow the filter or lower the project target volume."
        )


def finish(job_id, status, issue=None):
    stamp = now()
    with connect() as db:
        job = db.execute("SELECT * FROM scrape_jobs WHERE id=?", (job_id,)).fetchone()
        if not job:
            return
        if status == "completed":
            found = db.execute(
                "SELECT COUNT(*) FROM scrape_records WHERE job_id=? AND status='needs_review'",
                (job_id,),
            ).fetchone()[0]
            run_status = "needs_review" if found else "completed"
            event_status = run_status
            db.execute(
                "UPDATE scrape_jobs SET status=?,progress=100,records_found=(SELECT COUNT(*) FROM scrape_records WHERE job_id=?),completed_at=?,heartbeat_at=?,issues='[]',updated_at=? WHERE id=?",
                (run_status, job_id, stamp, stamp, stamp, job_id),
            )
        elif status == "cancelled":
            event_status = "cancelled"
            db.execute(
                "UPDATE scrape_jobs SET status='cancelled',cancelled_at=?,heartbeat_at=?,updated_at=? WHERE id=?",
                (stamp, stamp, stamp, job_id),
            )
        else:
            event_status = "failed"
            issues = json.loads(job["issues"] or "[]")
            issues.append({"message": str(issue or "Worker failed")[:500], "at": stamp})
            db.execute(
                "UPDATE scrape_jobs SET status='failed',issues=?,heartbeat_at=?,updated_at=? WHERE id=?",
                (json.dumps(issues, ensure_ascii=False), stamp, stamp, job_id),
            )
        db.execute(
            "UPDATE agenda_entries SET status=?,updated_at=? WHERE job_id=?",
            (event_status, stamp, job_id),
        )
        audit(db, "job", job_id, "worker_" + status)


def fail_or_retry(job_id, error):
    stamp = now()
    with connect() as db:
        row = db.execute(
            "SELECT retry_count,max_retries FROM scrape_jobs WHERE id=?", (job_id,)
        ).fetchone()
        if not row:
            return
        issue = str(error)[:500]
        issues = [{"message": issue, "at": stamp}]
        if (
            getattr(error, "retryable", False)
            and row["retry_count"] < row["max_retries"]
        ):
            retry = row["retry_count"] + 1
            delay = min(900, 30 * (2 ** (retry - 1)))
            scheduled = (
                (datetime.now(timezone.utc) + timedelta(seconds=delay))
                .isoformat(timespec="seconds")
                .replace("+00:00", "Z")
            )
            db.execute(
                "UPDATE scrape_jobs SET status='scheduled',retry_count=?,scheduled_at=?,next_retry_at=?,worker_id=NULL,heartbeat_at=NULL,issues=?,updated_at=? WHERE id=?",
                (retry, scheduled, scheduled, json.dumps(issues), stamp, job_id),
            )
            db.execute(
                "UPDATE agenda_entries SET status='scheduled',start_at=?,end_at=?,updated_at=? WHERE job_id=?",
                (
                    scheduled,
                    (
                        datetime.fromisoformat(scheduled.replace("Z", "+00:00"))
                        + timedelta(hours=1)
                    )
                    .isoformat(timespec="seconds")
                    .replace("+00:00", "Z"),
                    stamp,
                    job_id,
                ),
            )
            audit(db, "job", job_id, "retry_scheduled")
        else:
            db.execute(
                "UPDATE scrape_jobs SET status='failed',issues=?,worker_id=NULL,heartbeat_at=NULL,updated_at=? WHERE id=?",
                (json.dumps(issues), stamp, job_id),
            )
            db.execute(
                "UPDATE agenda_entries SET status='failed',updated_at=? WHERE job_id=?",
                (stamp, job_id),
            )
            audit(db, "job", job_id, "worker_failed")


def crawl(job_id):
    job = job_context(job_id)
    if not job:
        return
    with WORKER_LOCK:
        ACTIVE.add(job_id)
    try:
        check_ready(job)
        control = job_control(job_id)
        if control == "cancel":
            return finish(job_id, "cancelled")
        if control == "pause":
            return requeue_paused(job_id)
        if job["adapter"] == "synthetic-empty":
            return finish(job_id, "completed")
        if job["adapter"] == "ans-fhir":
            crawl_ans_fhir(job)
            return finish(job_id, "completed")
        approved = json.loads(job["selected_scope"])
        selected = json.loads(job["selected_fields"])
        base = job["base_url"].rstrip("/")
        base_host = urlsplit(base).hostname
        if base_host != job["host"]:
            raise WorkerError("Configured URL does not match the approved exact host.")
        try:
            target = max(1, min(MAX_PAGES, int(job.get("target_volume") or MAX_PAGES)))
        except (ValueError, TypeError):
            target = MAX_PAGES
        checkpoint = json.loads(job["checkpoint"] or "{}")
        queue = list(checkpoint.get("queue") or [])
        if not queue:
            for scope in approved:
                seed = scope[:-1] if scope.endswith("/*") else scope
                normalized = normalize_url(base + seed, job["host"], approved)
                if normalized:
                    queue.append(normalized)
        visited = set(checkpoint.get("visited") or [])
        pages = int(checkpoint.get("pages", 0))
        robots = load_robots(job, job["host"], job["pacing_seconds"])
        effective_interval = max(
            int(job["pacing_seconds"]),
            int(job["min_interval_seconds"]),
            int(robots.crawl_delay(USER_AGENT) or 0),
        )
        request_rate = robots.request_rate(USER_AGENT)
        if request_rate and request_rate.requests > 0:
            effective_interval = max(
                effective_interval,
                int(math.ceil(request_rate.seconds / request_rate.requests)),
            )
        while queue and pages < target and len(visited) < MAX_PAGES:
            control = job_control(job_id)
            if control == "cancel":
                return finish(job_id, "cancelled")
            if control == "pause":
                return requeue_paused(job_id)
            url = queue.pop(0)
            url = normalize_url(url, job["host"], approved)
            if not url or url in visited:
                continue
            if not robots.can_fetch(USER_AGENT, url):
                visited.add(url)
                pages += 1
                save_progress(job_id, pages, list(visited), queue, target)
                continue
            status, headers, payload = fetch(
                url, job["host"], job["source_id"], effective_interval, MAX_PAGE_BYTES
            )
            visited.add(url)
            pages += 1
            if status == 200:
                charset = "utf-8"
                content_type = headers.get("Content-Type", "")
                import re

                match = re.search(r"charset=([\w-]+)", content_type, re.I)
                if match:
                    charset = match.group(1)
                parser = PageParser()
                try:
                    parser.feed(payload.decode(charset, errors="replace"))
                except LookupError:
                    parser.feed(payload.decode("utf-8", errors="replace"))
                for fields in extract_records(parser, url, selected):
                    save_record(job, url, fields, payload)
                for href in parser.links:
                    candidate = normalize_url(urljoin(url, href), job["host"], approved)
                    if (
                        candidate
                        and candidate not in visited
                        and candidate not in queue
                        and len(queue) < MAX_PAGES * 4
                    ):
                        queue.append(candidate)
            save_progress(job_id, pages, list(visited), queue, target)
        finish(job_id, "completed")
    except Cancelled:
        finish(job_id, "cancelled")
    except Paused:
        with connect() as db:
            db.execute(
                "UPDATE scrape_jobs SET status='queued',worker_id=NULL,heartbeat_at=NULL,updated_at=? WHERE id=? AND status='running'",
                (now(), job_id),
            )
    except WorkerError as e:
        fail_or_retry(job_id, e)
    except Exception as e:
        fail_or_retry(
            job_id, WorkerError("Unexpected worker error: " + type(e).__name__, True)
        )
    finally:
        with WORKER_LOCK:
            ACTIVE.discard(job_id)


def recover_stale_jobs():
    stale = (
        (datetime.now(timezone.utc) - timedelta(seconds=LEASE_SECONDS))
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )
    stamp = now()
    with connect() as db:
        db.execute("BEGIN IMMEDIATE")
        rows = db.execute(
            "SELECT id,cancel_requested,retry_count,max_retries FROM scrape_jobs WHERE status='running' AND COALESCE(heartbeat_at,started_at,created_at)<?",
            (stale,),
        ).fetchall()
        for row in rows:
            if row["cancel_requested"]:
                db.execute(
                    "UPDATE scrape_jobs SET status='cancelled',cancelled_at=?,worker_id=NULL,heartbeat_at=NULL,updated_at=? WHERE id=?",
                    (stamp, stamp, row["id"]),
                )
                db.execute(
                    "UPDATE agenda_entries SET status='cancelled',updated_at=? WHERE job_id=?",
                    (stamp, row["id"]),
                )
            elif row["retry_count"] < row["max_retries"]:
                due = (
                    (datetime.now(timezone.utc) + timedelta(seconds=30))
                    .isoformat(timespec="seconds")
                    .replace("+00:00", "Z")
                )
                db.execute(
                    "UPDATE scrape_jobs SET status='scheduled',retry_count=retry_count+1,scheduled_at=?,next_retry_at=?,worker_id=NULL,heartbeat_at=NULL,updated_at=? WHERE id=?",
                    (due, due, stamp, row["id"]),
                )
                db.execute(
                    "UPDATE agenda_entries SET status='scheduled',start_at=?,end_at=?,updated_at=? WHERE job_id=?",
                    (
                        due,
                        (
                            datetime.fromisoformat(due.replace("Z", "+00:00"))
                            + timedelta(hours=1)
                        )
                        .isoformat(timespec="seconds")
                        .replace("+00:00", "Z"),
                        stamp,
                        row["id"],
                    ),
                )
            else:
                db.execute(
                    "UPDATE scrape_jobs SET status='failed',worker_id=NULL,heartbeat_at=NULL,updated_at=? WHERE id=?",
                    (stamp, row["id"]),
                )
                db.execute(
                    "UPDATE agenda_entries SET status='failed',updated_at=? WHERE job_id=?",
                    (stamp, row["id"]),
                )
        db.execute(
            "DELETE FROM worker_heartbeats WHERE last_seen_at<?",
            (
                (datetime.now(timezone.utc) - timedelta(days=7))
                .isoformat(timespec="seconds")
                .replace("+00:00", "Z"),
            ),
        )
        db.commit()


def claim_job():
    stamp = now()
    with connect() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute(
            "SELECT j.id FROM scrape_jobs j JOIN scrape_projects p ON p.id=j.project_id JOIN sources s ON s.id=j.source_id WHERE j.status='queued' AND j.cancel_requested=0 AND p.status<>'paused' AND (s.adapter<>'ans-fhir' OR ?=1) ORDER BY COALESCE(j.scheduled_at,j.created_at),j.created_at LIMIT 1",
            (int(has_ans_api_key()),),
        ).fetchone()
        if not row:
            db.commit()
            return None
        job_id = row["id"]
        changed = db.execute(
            "UPDATE scrape_jobs SET status='running',started_at=COALESCE(started_at,?),worker_id=?,heartbeat_at=?,attempt_count=attempt_count+1,updated_at=? WHERE id=? AND status='queued'",
            (stamp, WORKER_ID, stamp, stamp, job_id),
        ).rowcount
        if changed != 1:
            db.rollback()
            return None
        db.execute(
            "UPDATE worker_heartbeats SET active_jobs=active_jobs+1,last_seen_at=? WHERE id=?",
            (stamp, WORKER_ID),
        )
        db.commit()
        return job_id


def heartbeat_loop():
    while not STOP.wait(HEARTBEAT_SECONDS):
        with WORKER_LOCK:
            active = list(ACTIVE)
        stamp = now()
        try:
            with connect() as db:
                db.execute(
                    "UPDATE worker_heartbeats SET last_seen_at=?,active_jobs=? WHERE id=?",
                    (stamp, len(active), WORKER_ID),
                )
                if active:
                    db.executemany(
                        "UPDATE scrape_jobs SET heartbeat_at=? WHERE id=? AND status='running' AND worker_id=?",
                        [(stamp, jid, WORKER_ID) for jid in active],
                    )
        except sqlite3.Error:
            pass


def run(concurrency):
    stamp = now()
    ans_ready = has_ans_api_key()
    worker_version = VERSION + ";ans_fhir=" + ("ready" if ans_ready else "missing")
    with connect() as db:
        db.execute(
            "INSERT OR REPLACE INTO worker_heartbeats(id,hostname,started_at,last_seen_at,active_jobs,worker_version) VALUES(?,?,?,?,0,?)",
            (WORKER_ID, socket.gethostname(), stamp, stamp, worker_version),
        )
    signal.signal(signal.SIGINT, lambda *_: STOP.set())
    signal.signal(signal.SIGTERM, lambda *_: STOP.set())
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    print(
        "ProspectFlow worker "
        + WORKER_ID
        + " online; concurrency="
        + str(concurrency)
        + ".",
        flush=True,
    )
    if not ans_ready:
        print("ANS API key is not configured; ANS jobs will remain queued.", flush=True)
    with ThreadPoolExecutor(
        max_workers=concurrency, thread_name_prefix="scrape"
    ) as pool:
        futures = {}
        while not STOP.is_set():
            recover_stale_jobs()
            done = [f for f in futures if f.done()]
            for future in done:
                job_id = futures.pop(future, None)
                try:
                    future.result()
                except Exception as error:
                    print(
                        "Job "
                        + str(job_id)
                        + " worker task crashed: "
                        + type(error).__name__,
                        flush=True,
                    )
            while len(futures) < concurrency and not STOP.is_set():
                jid = claim_job()
                if not jid:
                    break
                future = pool.submit(crawl, jid)
                futures[future] = jid
            STOP.wait(1)
        for future in futures:
            future.result()
    with connect() as db:
        db.execute("DELETE FROM worker_heartbeats WHERE id=?", (WORKER_ID,))
    print("ProspectFlow worker stopped.", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ProspectFlow durable scrape worker")
    parser.add_argument(
        "--concurrency",
        type=int,
        default=int(os.environ.get("PROSPECTFLOW_WORKER_CONCURRENCY", "2")),
    )
    args = parser.parse_args()
    if not 1 <= args.concurrency <= 4:
        parser.error("concurrency must be between 1 and 4")
    run(args.concurrency)
