import hashlib
import json
import os
import sqlite3
import time
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


app = FastAPI(title="Job Finder API", version="1.0")

# -----------------------------
# CONFIG (edit as needed)
# -----------------------------
DB_PATH = os.getenv("JOB_DB_PATH", "jobs.db")

ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID", "YOUR_APP_ID")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY", "YOUR_APP_KEY")

COUNTRY = os.getenv("ADZUNA_COUNTRY", "us")
QUERY_WHAT = os.getenv("ADZUNA_WHAT", "director software engineering")
QUERY_WHAT_OR = os.getenv(
    "ADZUNA_WHAT_OR", "medical device medtech industrial automation robotics"
)
WHERE = os.getenv("ADZUNA_WHERE", "Utah")
RESULTS_PER_PAGE = int(os.getenv("ADZUNA_RESULTS_PER_PAGE", "50"))
MAX_PAGES = int(os.getenv("ADZUNA_MAX_PAGES", "50"))

REQUEST_TIMEOUT_SEC = 30
SLEEP_BETWEEN_PAGES_SEC = 0.25

# -----------------------------
# STATE MACHINE (simple)
# -----------------------------
STATE_NEW = "NEW"
STATE_DESC_READY = "DESC_READY"
STATE_SCORED = "SCORED"
STATE_APPLIED = "APPLIED"
STATE_IGNORED = "IGNORED"


class ScrapeRequest(BaseModel):
    db_path: Optional[str] = None
    app_id: Optional[str] = None
    app_key: Optional[str] = None
    country: Optional[str] = None
    what: Optional[str] = None
    what_or: Optional[str] = None
    where_text: Optional[str] = None
    results_per_page: Optional[int] = None
    max_pages: Optional[int] = None
    request_timeout_sec: Optional[int] = None
    sleep_between_pages_sec: Optional[float] = None


class ShortlistRequest(BaseModel):
    db_path: Optional[str] = None
    tag: Optional[str] = None
    location: Optional[str] = None
    keywords: Optional[List[str]] = None
    days: Optional[int] = None
    replace: Optional[bool] = None
    limit: Optional[int] = None
    only_unscored: Optional[bool] = None


class JobsQueryRequest(BaseModel):
    db_path: Optional[str] = None
    tag: Optional[str] = None
    location: Optional[str] = None
    keywords: Optional[List[str]] = None
    days: Optional[int] = None
    states: Optional[List[str]] = None
    only_unscored: Optional[bool] = None
    require_description: Optional[bool] = None
    limit: Optional[int] = None
    offset: Optional[int] = None


class DescribeRequest(BaseModel):
    db_path: Optional[str] = None
    job_key: str
    description_text: str
    description_source: Optional[str] = None


class MatchScoreRequest(BaseModel):
    db_path: Optional[str] = None
    job_key: str
    match_score: float


class PipelineRequest(BaseModel):
    scrape: Optional[ScrapeRequest] = None
    shortlist: Optional[ShortlistRequest] = None


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_job_key(job: Dict[str, Any]) -> str:
    adz_id = job.get("id")
    if adz_id:
        return f"adzuna:{adz_id}"
    url = (job.get("redirect_url") or "").strip()
    h = hashlib.sha256(url.encode("utf-8")).hexdigest()[:24]
    return f"urlhash:{h}"


def init_db(con: sqlite3.Connection) -> None:
    cur = con.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS runs (
        run_id INTEGER PRIMARY KEY AUTOINCREMENT,
        ran_at TEXT NOT NULL,
        country TEXT NOT NULL,
        what TEXT NOT NULL,
        what_or TEXT,
        where_text TEXT,
        params_json TEXT NOT NULL,
        pages_fetched INTEGER NOT NULL,
        result_count INTEGER NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        job_key TEXT PRIMARY KEY,
        adzuna_id TEXT,
        title TEXT,
        company TEXT,
        location TEXT,
        created TEXT,
        redirect_url TEXT,

        description_text TEXT,
        description_source TEXT,
        state TEXT NOT NULL,

        first_seen_at TEXT NOT NULL,
        last_seen_at TEXT NOT NULL,

        raw_json TEXT
    )
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_last_seen ON jobs(last_seen_at)")
    con.commit()


def ensure_columns(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    cols = {row[1] for row in cur.execute("PRAGMA table_info(jobs)").fetchall()}

    def add(col_sql: str) -> None:
        try:
            cur.execute(col_sql)
        except sqlite3.OperationalError:
            pass

    if "shortlist_tag" not in cols:
        add("ALTER TABLE jobs ADD COLUMN shortlist_tag TEXT")
    if "shortlisted_at" not in cols:
        add("ALTER TABLE jobs ADD COLUMN shortlisted_at TEXT")
    if "match_score" not in cols:
        add("ALTER TABLE jobs ADD COLUMN match_score REAL")

    con.commit()


def fetch_page(
    page: int,
    session: requests.Session,
    *,
    app_id: str,
    app_key: str,
    country: str,
    results_per_page: int,
    what: str,
    what_or: str,
    where_text: str,
    request_timeout_sec: int,
) -> Dict[str, Any]:
    url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"
    params = {
        "app_id": app_id,
        "app_key": app_key,
        "results_per_page": results_per_page,
        "what": what,
    }
    if what_or:
        params["what_or"] = what_or
    if where_text.strip():
        params["where"] = where_text.strip()

    r = session.get(url, params=params, timeout=request_timeout_sec)
    r.raise_for_status()
    return r.json()


def upsert_job(con: sqlite3.Connection, job: Dict[str, Any], now: str) -> str:
    key = stable_job_key(job)

    title = job.get("title")
    company = (job.get("company") or {}).get("display_name")
    location = (job.get("location") or {}).get("display_name")
    created = job.get("created")
    redirect_url = job.get("redirect_url")

    api_desc = job.get("description")
    desc_text = api_desc.strip() if isinstance(api_desc, str) and api_desc.strip() else None
    desc_source = "api" if desc_text else None

    cur = con.cursor()
    cur.execute("SELECT state, description_text FROM jobs WHERE job_key = ?", (key,))
    row = cur.fetchone()

    if row is None:
        state = STATE_DESC_READY if desc_text else STATE_NEW
        cur.execute("""
            INSERT INTO jobs (
                job_key, adzuna_id, title, company, location, created, redirect_url,
                description_text, description_source, state,
                first_seen_at, last_seen_at, raw_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            key, job.get("id"), title, company, location, created, redirect_url,
            desc_text, desc_source, state,
            now, now, json.dumps(job, ensure_ascii=False)
        ))
        return "inserted"

    existing_state, existing_desc = row[0], row[1]

    promoted_state = existing_state
    if existing_state == STATE_NEW and (existing_desc is None) and desc_text:
        promoted_state = STATE_DESC_READY

    preserve_states = {STATE_SCORED, STATE_APPLIED, STATE_IGNORED}
    final_state = existing_state if existing_state in preserve_states else promoted_state

    final_desc = existing_desc if existing_desc else desc_text
    final_desc_source = "api" if (final_desc and not existing_desc and desc_text) else None

    cur.execute("""
        UPDATE jobs SET
            adzuna_id=?,
            title=?,
            company=?,
            location=?,
            created=?,
            redirect_url=?,
            raw_json=?,
            last_seen_at=?,
            description_text=?,
            description_source=COALESCE(description_source, ?),
            state=?
        WHERE job_key=?
    """, (
        job.get("id"),
        title,
        company,
        location,
        created,
        redirect_url,
        json.dumps(job, ensure_ascii=False),
        now,
        final_desc,
        final_desc_source,
        final_state,
        key
    ))
    return "updated"


def run_scrape(
    *,
    db_path: str = DB_PATH,
    app_id: str = ADZUNA_APP_ID,
    app_key: str = ADZUNA_APP_KEY,
    country: str = COUNTRY,
    what: str = QUERY_WHAT,
    what_or: str = QUERY_WHAT_OR,
    where_text: str = WHERE,
    results_per_page: int = RESULTS_PER_PAGE,
    max_pages: int = MAX_PAGES,
    request_timeout_sec: int = REQUEST_TIMEOUT_SEC,
    sleep_between_pages_sec: float = SLEEP_BETWEEN_PAGES_SEC,
) -> Dict[str, Any]:
    if "YOUR_APP_ID" in app_id or "YOUR_APP_KEY" in app_key:
        raise ValueError("Set ADZUNA_APP_ID and ADZUNA_APP_KEY (env vars recommended).")

    con = sqlite3.connect(db_path)
    init_db(con)

    run_started = utc_now()
    inserted = 0
    updated = 0
    pages_fetched = 0
    total_returned = 0

    params_snapshot = {
        "country": country,
        "what": what,
        "what_or": what_or,
        "where": where_text.strip(),
        "results_per_page": results_per_page,
        "max_pages": max_pages
    }

    session = requests.Session()

    all_results: List[Dict[str, Any]] = []

    for page in range(1, max_pages + 1):
        data = fetch_page(
            page,
            session=session,
            app_id=app_id,
            app_key=app_key,
            country=country,
            results_per_page=results_per_page,
            what=what,
            what_or=what_or,
            where_text=where_text,
            request_timeout_sec=request_timeout_sec,
        )
        results = data.get("results", [])
        pages_fetched += 1

        if not results:
            break

        all_results.extend(results)
        total_returned += len(results)

        time.sleep(sleep_between_pages_sec)

    now = utc_now()
    for job in all_results:
        outcome = upsert_job(con, job, now=now)
        if outcome == "inserted":
            inserted += 1
        else:
            updated += 1

    cur = con.cursor()
    cur.execute("""
        INSERT INTO runs (ran_at, country, what, what_or, where_text, params_json, pages_fetched, result_count)
        VALUES (?,?,?,?,?,?,?,?)
    """, (
        run_started,
        country,
        what,
        what_or,
        where_text.strip() or None,
        json.dumps(params_snapshot, separators=(",", ":")),
        pages_fetched,
        total_returned
    ))
    con.commit()
    con.close()

    return {
        "run_started": run_started,
        "pages_fetched": pages_fetched,
        "total_returned": total_returned,
        "inserted": inserted,
        "updated": updated,
        "db_path": db_path,
    }


def run_shortlist(
    *,
    db_path: str = DB_PATH,
    tag: str = "DEFAULT",
    location: str = "",
    keywords: Optional[List[str]] = None,
    days: int = 7,
    replace: bool = False,
    limit: int = 200,
    only_unscored: bool = False,
) -> Dict[str, Any]:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    ensure_columns(con)
    cur = con.cursor()

    now = utc_now_iso()
    tag = tag.strip() or "DEFAULT"

    if replace:
        cur.execute(
            "UPDATE jobs SET shortlist_tag=NULL, shortlisted_at=NULL WHERE shortlist_tag=?",
            (tag,)
        )

    where = []
    params = []

    where.append("state NOT IN ('APPLIED','IGNORED')")

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    cutoff_iso = cutoff.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    where.append("first_seen_at >= ?")
    params.append(cutoff_iso)

    if location.strip():
        where.append("location LIKE ?")
        params.append(f"%{location.strip()}%")

    for kw in keywords or []:
        kw = kw.strip()
        if kw:
            where.append("title LIKE ?")
            params.append(f"%{kw}%")

    cols = {row[1] for row in cur.execute("PRAGMA table_info(jobs)").fetchall()}
    if only_unscored and "match_score" in cols:
        where.append("match_score IS NULL")

    where_sql = " AND ".join(where) if where else "1=1"

    sql = f"""
    SELECT job_key
    FROM jobs
    WHERE {where_sql}
    ORDER BY
        CASE state WHEN 'DESC_READY' THEN 0 WHEN 'NEW' THEN 1 ELSE 2 END,
        first_seen_at DESC
    LIMIT ?
    """
    rows = cur.execute(sql, params + [limit]).fetchall()
    keys = [r["job_key"] for r in rows]

    marked = 0
    for k in keys:
        cur.execute(
            "UPDATE jobs SET shortlist_tag=?, shortlisted_at=? WHERE job_key=?",
            (tag, now, k)
        )
        marked += 1

    con.commit()

    total = cur.execute(
        "SELECT COUNT(*) FROM jobs WHERE shortlist_tag=?",
        (tag,)
    ).fetchone()[0]

    con.close()

    return {
        "tag": tag,
        "marked": marked,
        "total": total,
        "db_path": db_path,
    }


def query_jobs(
    *,
    db_path: str = DB_PATH,
    tag: Optional[str] = None,
    location: str = "",
    keywords: Optional[List[str]] = None,
    days: Optional[int] = None,
    states: Optional[List[str]] = None,
    only_unscored: bool = False,
    require_description: bool = False,
    limit: int = 200,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    ensure_columns(con)
    cur = con.cursor()

    where = []
    params = []

    if tag:
        where.append("shortlist_tag = ?")
        params.append(tag)

    if days is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        cutoff_iso = cutoff.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        where.append("first_seen_at >= ?")
        params.append(cutoff_iso)

    if location.strip():
        where.append("location LIKE ?")
        params.append(f"%{location.strip()}%")

    for kw in keywords or []:
        kw = kw.strip()
        if kw:
            where.append("title LIKE ?")
            params.append(f"%{kw}%")

    if states:
        placeholders = ",".join(["?"] * len(states))
        where.append(f"state IN ({placeholders})")
        params.extend(states)

    cols = {row[1] for row in cur.execute("PRAGMA table_info(jobs)").fetchall()}
    if only_unscored and "match_score" in cols:
        where.append("match_score IS NULL")

    if require_description:
        where.append("description_text IS NOT NULL")

    where_sql = " AND ".join(where) if where else "1=1"

    sql = f"""
    SELECT
        job_key,
        title,
        company,
        location,
        created,
        redirect_url,
        description_text,
        description_source,
        state,
        first_seen_at,
        last_seen_at,
        shortlist_tag,
        shortlisted_at
    FROM jobs
    WHERE {where_sql}
    ORDER BY
        CASE state WHEN 'DESC_READY' THEN 0 WHEN 'NEW' THEN 1 ELSE 2 END,
        first_seen_at DESC
    LIMIT ? OFFSET ?
    """

    rows = cur.execute(sql, params + [limit, offset]).fetchall()
    con.close()

    return [dict(row) for row in rows]


def update_description(
    *,
    db_path: str,
    job_key: str,
    description_text: str,
    description_source: str,
) -> Dict[str, Any]:
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("SELECT state, description_text FROM jobs WHERE job_key=?", (job_key,))
    row = cur.fetchone()

    if row is None:
        con.close()
        raise ValueError("Job not found")

    existing_state, existing_desc = row[0], row[1]

    preserve_states = {STATE_SCORED, STATE_APPLIED, STATE_IGNORED}
    final_state = existing_state
    if existing_state not in preserve_states:
        if existing_state == STATE_NEW and description_text:
            final_state = STATE_DESC_READY

    final_desc = existing_desc if existing_desc else description_text
    final_source = description_source if final_desc == description_text else None

    cur.execute(
        """
        UPDATE jobs SET
            description_text=?,
            description_source=COALESCE(description_source, ?),
            state=?
        WHERE job_key=?
        """,
        (final_desc, final_source, final_state, job_key),
    )
    con.commit()
    con.close()

    return {
        "job_key": job_key,
        "description_source": description_source,
        "state": final_state,
    }


def update_match_score(
    *,
    db_path: str,
    job_key: str,
    match_score: float,
) -> Dict[str, Any]:
    con = sqlite3.connect(db_path)
    ensure_columns(con)
    cur = con.cursor()
    cur.execute("SELECT job_key FROM jobs WHERE job_key=?", (job_key,))
    if cur.fetchone() is None:
        con.close()
        raise ValueError("Job not found")

    cur.execute(
        "UPDATE jobs SET match_score=? WHERE job_key=?",
        (match_score, job_key),
    )
    con.commit()
    con.close()

    return {
        "job_key": job_key,
        "match_score": match_score,
    }


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/scrape")
def scrape_endpoint(req: ScrapeRequest) -> dict:
    try:
        result = run_scrape(
            db_path=req.db_path if req.db_path is not None else DB_PATH,
            app_id=req.app_id if req.app_id is not None else ADZUNA_APP_ID,
            app_key=req.app_key if req.app_key is not None else ADZUNA_APP_KEY,
            country=req.country if req.country is not None else COUNTRY,
            what=req.what if req.what is not None else QUERY_WHAT,
            what_or=req.what_or if req.what_or is not None else QUERY_WHAT_OR,
            where_text=req.where_text if req.where_text is not None else WHERE,
            results_per_page=(
                req.results_per_page if req.results_per_page is not None else RESULTS_PER_PAGE
            ),
            max_pages=req.max_pages if req.max_pages is not None else MAX_PAGES,
            request_timeout_sec=(
                req.request_timeout_sec
                if req.request_timeout_sec is not None
                else REQUEST_TIMEOUT_SEC
            ),
            sleep_between_pages_sec=(
                req.sleep_between_pages_sec
                if req.sleep_between_pages_sec is not None
                else SLEEP_BETWEEN_PAGES_SEC
            ),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return result


@app.post("/shortlist")
def shortlist_endpoint(req: ShortlistRequest) -> dict:
    result = run_shortlist(
        db_path=req.db_path if req.db_path is not None else DB_PATH,
        tag=req.tag if req.tag is not None else "DEFAULT",
        location=req.location if req.location is not None else "",
        keywords=req.keywords if req.keywords is not None else [],
        days=req.days if req.days is not None else 7,
        replace=req.replace if req.replace is not None else False,
        limit=req.limit if req.limit is not None else 200,
        only_unscored=req.only_unscored if req.only_unscored is not None else False,
    )
    return result


@app.post("/jobs/query")
def jobs_query_endpoint(req: JobsQueryRequest) -> dict:
    result = query_jobs(
        db_path=req.db_path if req.db_path is not None else DB_PATH,
        tag=req.tag,
        location=req.location if req.location is not None else "",
        keywords=req.keywords if req.keywords is not None else [],
        days=req.days,
        states=req.states,
        only_unscored=req.only_unscored if req.only_unscored is not None else False,
        require_description=req.require_description if req.require_description is not None else False,
        limit=req.limit if req.limit is not None else 200,
        offset=req.offset if req.offset is not None else 0,
    )
    return {"results": result, "count": len(result)}


@app.post("/jobs/describe")
def jobs_describe_endpoint(req: DescribeRequest) -> dict:
    source = req.description_source or "scrape"
    try:
        result = update_description(
            db_path=req.db_path if req.db_path is not None else DB_PATH,
            job_key=req.job_key,
            description_text=req.description_text,
            description_source=source,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return result


@app.post("/jobs/match-score")
def jobs_match_score_endpoint(req: MatchScoreRequest) -> dict:
    try:
        result = update_match_score(
            db_path=req.db_path if req.db_path is not None else DB_PATH,
            job_key=req.job_key,
            match_score=req.match_score,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return result


@app.post("/pipeline")
def pipeline_endpoint(req: PipelineRequest) -> dict:
    scrape_req = req.scrape or ScrapeRequest()
    shortlist_req = req.shortlist or ShortlistRequest()

    scrape_result = scrape_endpoint(scrape_req)
    shortlist_result = shortlist_endpoint(shortlist_req)

    return {
        "scrape": scrape_result,
        "shortlist": shortlist_result,
    }
