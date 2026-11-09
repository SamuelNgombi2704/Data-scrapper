# ProspectFlow

ProspectFlow coordinates permitted data collection through two workspaces: **Agenda** and **Projects**. This repository is a dependency-free starter built with Python's standard library and SQLite so the first workflows can run locally without a service account.

## Run locally

Requires Python 3.8 or newer.

```powershell
python app.py
```

In a second PowerShell window, from this folder, start the separate scrape worker:

```powershell
python worker.py --concurrency 2
```

Both programs use `data/prospectflow.sqlite3` by default. If you set `PROSPECTFLOW_DB`, set the same path in both terminals. The web app keeps scheduled and queued jobs durable while the worker is offline; the worker claims due jobs atomically and publishes progress and results back to the same database.

Open <http://127.0.0.1:8000>. The first visit prompts you to create an owner account; later visits show sign-in. The API binds to loopback by default. Set `PROSPECTFLOW_HOST` only when you intentionally want to expose it on a network. SQLite data is stored in `data/prospectflow.sqlite3`.

The `.example` source is a **synthetic, empty demo adapter**. It does not fetch live pages. Add a permitted source from Sources with its exact HTTPS host, allowed paths, same-host terms URL and policy attestations. The `public-html` worker only requests public HTTPS pages on that exact host and within approved paths, checks robots.txt each run, honors source and robots pacing, limits each run to 50 pages by default, and extracts selected professional fields from recognized public Person JSON-LD. It does not support logins, cookies, paywall access, CAPTCHA or access-control bypass. Every extracted field retains its source URL for review.

The ANS FHIR connector runs at one request per second and stores only public professional fields selected for the project. It records the RPPS source and ANS `meta.lastUpdated` date with each field for reuse attribution. Keep the API key in the worker environment only. Each project run is bounded at 2,500 records and may return fewer, depending on ANS results. Users reusing RPPS data are responsible for GDPR obligations, including informing people and handling applicable rights requests.


## Included in this starter

### ANS Annuaire Santé FHIR

The Sources page can add the official ANS FHIR directory connector. In a project, link it and configure a bounded query by professional name, RPPS identifier, or ANS qualification code. Select only the public professional fields needed for the project. The worker requests the official HTTPS FHIR endpoint, uses the ESANTE-API-KEY header, follows pagination only on the exact ANS host and the fixed Practitioner or opaque `_page` continuation paths, caps a run at 50 API pages and the project's target volume, applies source pacing, and stores each field with the source API URL for review. It does not collect restricted Annuaire Santé data.

Obtain the key from a subscribed ANS Gravitee application. Keep it out of chat, source code, SQLite and browser storage. In the PowerShell window used to run the worker, set it for that process only:

    $secureKey = Read-Host "ANS API key" -AsSecureString
    $keyPointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureKey)
    try {
      $env:ANS_FHIR_API_KEY = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($keyPointer)
      python worker.py --concurrency 2
    } finally {
      Remove-Item Env:ANS_FHIR_API_KEY -ErrorAction SilentlyContinue
      [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($keyPointer)
    }

The web app may run without the key; the worker must be restarted in the PowerShell process that has ANS_FHIR_API_KEY set. If a key was pasted into chat or another shared surface, revoke it in Gravitee and use a new one.

Doctolib's current robots.txt disallows doctor profile paths and API paths for general crawlers. ProspectFlow will not fetch those paths or attempt another access method; use the ANS directory or another source that explicitly permits the required collection.

- Project and project-source persistence with foreign keys, scoped readiness checks and paginated APIs.
- Agenda event CRUD, project and run links, filters, overlap warnings and list/month/week/day views.
- SQLite-backed scheduled jobs with a transactional due-job claim and idempotency key, a separately run bounded worker, checkpoints, cancellation, retries with backoff, and worker heartbeats.
- A paginated, field-oriented review table that keeps source provenance and supports verify, resolve and flag actions. The review view can export the current search/status filter as a bounded Excel-compatible CSV (up to 10,000 rows), including source links and available fields.
- In-project data preparation that normalizes professional fields, scores completeness, flags possible duplicates without deleting data, and keeps field provenance. The analysis view summarizes review, quality, specialty, role and location counts; CSV export defaults to 1,000 records and is capped at 10,000.
- OpenAPI route outline and shared Zod input/output contracts in `contracts.ts`.
- A responsive full-page interface with project progression, agenda quick-add, event detail, and explicit confirmation before cancelling a linked run.

## Deliberate boundaries

This is a local development scaffold, not a production deployment. Authentication supports one owner account and has no account recovery flow. The Python server performs request validation directly; `contracts.ts` provides matching Zod schemas for TypeScript clients (`npm install` to use them). The public HTML adapter is intentionally conservative and depends on pages publishing structured Person data; it is not a general-purpose page parser. Run `worker.py` separately to execute jobs. Do not put data from sources you are not authorized to access in it. Recurrence is omitted until its time-zone and daylight-saving behavior can be implemented safely.

## API

See `openapi.yaml`. Except for `/api/health` and the documented auth endpoints, API routes require a valid server-side session cookie. Database timestamps are stored as UTC ISO-8601 values. Agenda events carry an IANA timezone for display. The browser currently displays the system's IANA timezone. After `npm install`, run `npm run check:contracts` to type-check the shared Zod contracts.

## Next implementation slices

Before production use, add managed multi-user identity and roles, a dedicated durable queue/worker service, stronger deployment controls, broader extractor validation, complete audit history, and accessibility verification.

After a permitted source run adds records, open the project's **Preparation** tab and prepare up to 1,000 records per pass. **Analysis & reports** shows quality and category summaries and exports prepared, non-duplicate records to CSV. The app reports actual counts and does not generate filler rows; reaching 1,000 depends on source results and the selected filter.
The review table displays practice contact fields only when a permitted source provides them. For public HTML sources, `practice_address`, `practice_phone` and `practice_email` are taken only from the linked organization object in public Person structured data; direct person email/phone and home addresses are not collected. For ANS FHIR, those fields can be selected from linked Organization records; ANS secure health messaging addresses are excluded. Select contact fields only if the source terms explicitly permit collection and professional reuse. The app does not scrape Doctolib for contact harvesting. CSV exports preserve the source URL and field provenance; verify source terms and applicable notice/objection requirements before reusing contact data.

