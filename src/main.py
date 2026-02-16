import hashlib
import json
import os
import sqlite3
import time
import traceback
from pathlib import Path
from datetime import UTC, datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from pydantic import BaseModel, Field


app = FastAPI(title="Load Finder API", version="1.0")


BASE_DIR = Path(__file__).resolve().parent
ICON_PATH = BASE_DIR / "assets" / "icons" / "truck_loads_icon.ico"
TEMPLATE_HTML = (BASE_DIR / "templates" / "index.html").read_text(encoding="utf-8")
TIMING_LOGS = os.getenv("TIMING_LOGS") == "1"
TIMING_LOG_PATH = os.getenv("TIMING_LOG_PATH", "timing.log")


@app.get("/favicon.ico")
def favicon() -> FileResponse:
    return FileResponse(ICON_PATH)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def write_timing(entry: Dict[str, Any]) -> None:
    if not TIMING_LOGS:
        return
    payload = {
        "ts": utc_now_iso(),
        **entry,
    }
    with open(TIMING_LOG_PATH, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, separators=(",", ":")) + "\n")


def server_error_log_path() -> Path:
    db_path = os.getenv("LOADS_DB_PATH", DB_PATH)
    try:
        base_dir = Path(db_path).resolve().parent
    except Exception:
        base_dir = Path.cwd()
    return base_dir / "error.log"


def write_server_error(message: str) -> Path:
    log_path = server_error_log_path()
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    with open(log_path, "a", encoding="utf-8") as handle:
        handle.write(message)
        if not message.endswith("\n"):
            handle.write("\n")
    return log_path


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    if isinstance(exc, HTTPException):
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
    trace = traceback.format_exc()
    log_path = write_server_error(trace)
    return JSONResponse(
        status_code=500,
        content={"detail": f"Internal server error. See log at {log_path}"},
    )


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return TEMPLATE_HTML

# -----------------------------
# CONFIG (edit as needed)
# -----------------------------
DB_PATH = os.getenv("LOADS_DB_PATH", "loads.db")
SAMPLE_LOADS_PATH = os.getenv(
    "SAMPLE_LOADS_PATH",
    str(BASE_DIR.parent / "data" / "sample_loads.json"),
)

RATE_MIN = 0.0
RATE_MAX = 3000.0
D2P_MIN = 0.0
D2P_MAX = 40.0
RATE_WEIGHT = 0.7
D2P_WEIGHT = 0.3
D2P_MISSING_PENALTY = 2.0

# -----------------------------
# STATE MACHINE (simple)
# -----------------------------
STATE_NEW = "NEW"
STATE_READY = "READY"
STATE_SCORED = "SCORED"
STATE_APPLIED = "APPLIED"
STATE_IGNORED = "IGNORED"


class ScrapeRequest(BaseModel):
    db_path: Optional[str] = None
    sample_path: Optional[str] = None
    overwrite: Optional[bool] = None


class ShortlistRequest(BaseModel):
    db_path: Optional[str] = None
    tag: Optional[str] = None
    date: Optional[str] = None
    o_city: Optional[str] = Field(default=None, alias="O-City")
    o_st: Optional[str] = Field(default=None, alias="O-St")
    d_city: Optional[str] = Field(default=None, alias="D-City")
    d_st: Optional[str] = Field(default=None, alias="D-St")
    o_dh_max: Optional[int] = Field(default=None, alias="O-DH")
    d_dh_max: Optional[int] = Field(default=None, alias="D-DH")
    replace: Optional[bool] = None
    limit: Optional[int] = None
    only_unscored: Optional[bool] = None

    class Config:
        allow_population_by_field_name = True


class LoadsQueryRequest(BaseModel):
    db_path: Optional[str] = None
    tag: Optional[str] = None
    date: Optional[str] = None
    o_city: Optional[str] = Field(default=None, alias="O-City")
    o_st: Optional[str] = Field(default=None, alias="O-St")
    d_city: Optional[str] = Field(default=None, alias="D-City")
    d_st: Optional[str] = Field(default=None, alias="D-St")
    o_dh_max: Optional[int] = Field(default=None, alias="O-DH")
    d_dh_max: Optional[int] = Field(default=None, alias="D-DH")
    states: Optional[List[str]] = None
    only_unscored: Optional[bool] = None
    limit: Optional[int] = None
    offset: Optional[int] = None

    class Config:
        allow_population_by_field_name = True


class ScoreLoadsRequest(BaseModel):
    db_path: Optional[str] = None
    tag: Optional[str] = None
    only_unscored: Optional[bool] = None
    limit: Optional[int] = None


class PipelineRequest(BaseModel):
    scrape: Optional[ScrapeRequest] = None
    shortlist: Optional[ShortlistRequest] = None


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def stable_load_key(load: Dict[str, Any]) -> str:
    key_parts = [
        str(load.get("O-City") or ""),
        str(load.get("O-St") or ""),
        str(load.get("D-City") or ""),
        str(load.get("D-St") or ""),
        str(load.get("Pickup") or ""),
        str(load.get("Company") or ""),
        str(load.get("Rate") or ""),
        str(load.get("Distance") or ""),
    ]
    raw = "|".join(key_parts)
    h = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
    return f"load:{h}"


def normalize_pickup(value: Optional[str]) -> str:
    today = datetime.now(timezone.utc).date()
    if value is None:
        return today.isoformat()
    text = value.strip()
    if not text or text.upper() == "TODAY":
        return today.isoformat()
    for fmt in ("%m/%d", "%m/%d/%y", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(text, fmt).date()
            if fmt in ("%m/%d",):
                parsed = parsed.replace(year=today.year)
            return parsed.isoformat()
        except ValueError:
            continue
    return today.isoformat()


def normalize_date_filter(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    if text.upper() == "TODAY":
        return datetime.now(timezone.utc).date().isoformat()
    return normalize_pickup(text)


def to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def parse_rate(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip().replace("$", "").replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_d2p(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def math_match_score(load: Dict[str, Any]) -> float:
    rate = parse_rate(load.get("Rate"))
    d2p = parse_d2p(load.get("D2P"))

    rate_norm = 0.0
    if rate is not None:
        rate_norm = clamp((rate - RATE_MIN) / (RATE_MAX - RATE_MIN), 0.0, 1.0)

    if d2p is None:
        d2p_norm = 0.0
        missing_d2p = True
    else:
        d2p_norm = clamp(1.0 - ((d2p - D2P_MIN) / (D2P_MAX - D2P_MIN)), 0.0, 1.0)
        missing_d2p = False

    blended = (RATE_WEIGHT * rate_norm) + (D2P_WEIGHT * d2p_norm)
    score = blended * 10.0
    if missing_d2p:
        score -= D2P_MISSING_PENALTY
    return round(clamp(score, 0.0, 10.0), 1)


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
    CREATE TABLE IF NOT EXISTS loads (
        load_key TEXT PRIMARY KEY,
        "O-City" TEXT,
        "O-St" TEXT,
        "D-City" TEXT,
        "D-St" TEXT,
        "O-DH" INTEGER,
        "D-DH" INTEGER,
        "Distance" INTEGER,
        "Rate" TEXT,
        "RPM" TEXT,
        "Weight" INTEGER,
        "Length" INTEGER,
        "Equip" TEXT,
        "Mode" TEXT,
        "Pickup" TEXT,
        "Company" TEXT,
        "Updated" TEXT,
        "D2P" TEXT,

        state TEXT NOT NULL,
        first_seen_at TEXT NOT NULL,
        last_seen_at TEXT NOT NULL,
        raw_json TEXT
    )
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_loads_state ON loads(state)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_loads_last_seen ON loads(last_seen_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_loads_pickup ON loads(\"Pickup\")")
    con.commit()


def ensure_columns(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    cols = {row[1] for row in cur.execute("PRAGMA table_info(loads)").fetchall()}

    def add(col_sql: str) -> None:
        try:
            cur.execute(col_sql)
        except sqlite3.OperationalError:
            pass

    if "shortlist_tag" not in cols:
        add("ALTER TABLE loads ADD COLUMN shortlist_tag TEXT")
    if "shortlisted_at" not in cols:
        add("ALTER TABLE loads ADD COLUMN shortlisted_at TEXT")
    if "match_score" not in cols:
        add("ALTER TABLE loads ADD COLUMN match_score REAL")

    con.commit()


def upsert_load(con: sqlite3.Connection, load: Dict[str, Any], now: str) -> str:
    key = stable_load_key(load)

    pickup = normalize_pickup(load.get("Pickup"))
    cur = con.cursor()
    cur.execute("SELECT state FROM loads WHERE load_key = ?", (key,))
    row = cur.fetchone()

    fields = {
        "O-City": load.get("O-City"),
        "O-St": load.get("O-St"),
        "D-City": load.get("D-City"),
        "D-St": load.get("D-St"),
        "O-DH": to_int(load.get("O-DH")),
        "D-DH": to_int(load.get("D-DH")),
        "Distance": to_int(load.get("Distance")),
        "Rate": load.get("Rate"),
        "RPM": load.get("RPM"),
        "Weight": to_int(load.get("Weight")),
        "Length": to_int(load.get("Length")),
        "Equip": load.get("Equip"),
        "Mode": load.get("Mode"),
        "Pickup": pickup,
        "Company": load.get("Company"),
        "Updated": load.get("Updated"),
        "D2P": load.get("D2P"),
    }

    if row is None:
        state = STATE_READY
        cur.execute("""
            INSERT INTO loads (
                load_key, "O-City", "O-St", "D-City", "D-St", "O-DH", "D-DH",
                "Distance", "Rate", "RPM", "Weight", "Length", "Equip", "Mode",
                "Pickup", "Company", "Updated", "D2P",
                state, first_seen_at, last_seen_at, raw_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            key,
            fields["O-City"],
            fields["O-St"],
            fields["D-City"],
            fields["D-St"],
            fields["O-DH"],
            fields["D-DH"],
            fields["Distance"],
            fields["Rate"],
            fields["RPM"],
            fields["Weight"],
            fields["Length"],
            fields["Equip"],
            fields["Mode"],
            fields["Pickup"],
            fields["Company"],
            fields["Updated"],
            fields["D2P"],
            state,
            now,
            now,
            json.dumps(load, ensure_ascii=False)
        ))
        return "inserted"

    preserve_states = {STATE_SCORED, STATE_APPLIED, STATE_IGNORED}
    existing_state = row[0]
    final_state = existing_state if existing_state in preserve_states else STATE_READY

    cur.execute("""
        UPDATE loads SET
            "O-City"=?,
            "O-St"=?,
            "D-City"=?,
            "D-St"=?,
            "O-DH"=?,
            "D-DH"=?,
            "Distance"=?,
            "Rate"=?,
            "RPM"=?,
            "Weight"=?,
            "Length"=?,
            "Equip"=?,
            "Mode"=?,
            "Pickup"=?,
            "Company"=?,
            "Updated"=?,
            "D2P"=?,
            raw_json=?,
            last_seen_at=?,
            state=?
        WHERE load_key=?
    """, (
        fields["O-City"],
        fields["O-St"],
        fields["D-City"],
        fields["D-St"],
        fields["O-DH"],
        fields["D-DH"],
        fields["Distance"],
        fields["Rate"],
        fields["RPM"],
        fields["Weight"],
        fields["Length"],
        fields["Equip"],
        fields["Mode"],
        fields["Pickup"],
        fields["Company"],
        fields["Updated"],
        fields["D2P"],
        json.dumps(load, ensure_ascii=False),
        now,
        final_state,
        key,
    ))
    return "updated"


def run_scrape(
    *,
    db_path: str = DB_PATH,
    sample_path: str = SAMPLE_LOADS_PATH,
    overwrite: bool = False,
) -> Dict[str, Any]:
    start = time.perf_counter()
    con = sqlite3.connect(db_path)
    init_db(con)
    ensure_columns(con)

    run_started = utc_now()
    inserted = 0
    updated = 0

    params_snapshot = {
        "sample_path": sample_path,
    }

    with open(sample_path, "r", encoding="utf-8") as handle:
        loads = json.load(handle)

    now = utc_now()
    if overwrite:
        con.execute("DELETE FROM loads")
        con.commit()
    for load in loads:
        outcome = upsert_load(con, load, now=now)
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
        "sample",
        "loads",
        None,
        None,
        json.dumps(params_snapshot, separators=(",", ":")),
        1,
        len(loads),
    ))
    con.commit()
    total_in_db = con.execute("SELECT COUNT(*) FROM loads").fetchone()[0]
    con.close()
    write_timing({"layer": "server", "op": "scrape", "ms": int((time.perf_counter() - start) * 1000)})
    return {
        "run_started": run_started,
        "pages_fetched": 1,
        "total_returned": len(loads),
        "inserted": inserted,
        "updated": updated,
        "total_in_db": total_in_db,
        "db_path": db_path,
        "sample_path": sample_path,
    }


def run_shortlist(
    *,
    db_path: str = DB_PATH,
    tag: str = "DEFAULT",
    date: Optional[str] = None,
    o_city: str = "",
    o_st: str = "",
    d_city: str = "",
    d_st: str = "",
    o_dh_max: Optional[int] = None,
    d_dh_max: Optional[int] = None,
    replace: bool = False,
    limit: int = 200,
    only_unscored: bool = False,
) -> Dict[str, Any]:
    start = time.perf_counter()
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    ensure_columns(con)
    cur = con.cursor()

    now = utc_now_iso()
    tag = tag.strip() or "DEFAULT"

    if replace:
        cur.execute(
            "UPDATE loads SET shortlist_tag=NULL, shortlisted_at=NULL WHERE shortlist_tag=?",
            (tag,)
        )

    where = []
    params = []

    where.append("state NOT IN ('APPLIED','IGNORED')")

    date_filter = normalize_date_filter(date)
    if date_filter:
        where.append("\"Pickup\" = ?")
        params.append(date_filter)

    if o_city.strip():
        where.append("UPPER(\"O-City\") = UPPER(?)")
        params.append(o_city.strip())
    if o_st.strip():
        where.append("UPPER(\"O-St\") = UPPER(?)")
        params.append(o_st.strip())
    if d_city.strip():
        where.append("UPPER(\"D-City\") = UPPER(?)")
        params.append(d_city.strip())
    if d_st.strip():
        where.append("UPPER(\"D-St\") = UPPER(?)")
        params.append(d_st.strip())

    if o_dh_max is not None:
        where.append("\"O-DH\" <= ?")
        params.append(o_dh_max)
    if d_dh_max is not None:
        where.append("\"D-DH\" <= ?")
        params.append(d_dh_max)

    cols = {row[1] for row in cur.execute("PRAGMA table_info(loads)").fetchall()}
    if only_unscored and "match_score" in cols:
        where.append("match_score IS NULL")

    where_sql = " AND ".join(where) if where else "1=1"

    sql = f"""
    SELECT load_key
    FROM loads
    WHERE {where_sql}
    ORDER BY
        CASE state WHEN 'READY' THEN 0 WHEN 'NEW' THEN 1 ELSE 2 END,
        first_seen_at DESC
    LIMIT ?
    """
    rows = cur.execute(sql, params + [limit]).fetchall()
    keys = [r["load_key"] for r in rows]

    marked = 0
    for k in keys:
        cur.execute(
            "UPDATE loads SET shortlist_tag=?, shortlisted_at=? WHERE load_key=?",
            (tag, now, k)
        )
        marked += 1

    con.commit()

    total = cur.execute(
        "SELECT COUNT(*) FROM loads WHERE shortlist_tag=?",
        (tag,)
    ).fetchone()[0]

    con.close()
    write_timing({"layer": "server", "op": "shortlist", "ms": int((time.perf_counter() - start) * 1000)})
    return {
        "tag": tag,
        "marked": marked,
        "total": total,
        "db_path": db_path,
    }


def query_loads(
    *,
    db_path: str = DB_PATH,
    tag: Optional[str] = None,
    date: Optional[str] = None,
    o_city: str = "",
    o_st: str = "",
    d_city: str = "",
    d_st: str = "",
    o_dh_max: Optional[int] = None,
    d_dh_max: Optional[int] = None,
    states: Optional[List[str]] = None,
    only_unscored: bool = False,
    limit: int = 200,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    start = time.perf_counter()
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    ensure_columns(con)
    cur = con.cursor()

    where = []
    params = []

    if tag:
        where.append("shortlist_tag = ?")
        params.append(tag)

    date_filter = normalize_date_filter(date)
    if date_filter:
        where.append("\"Pickup\" = ?")
        params.append(date_filter)

    if o_city.strip():
        where.append("UPPER(\"O-City\") = UPPER(?)")
        params.append(o_city.strip())
    if o_st.strip():
        where.append("UPPER(\"O-St\") = UPPER(?)")
        params.append(o_st.strip())
    if d_city.strip():
        where.append("UPPER(\"D-City\") = UPPER(?)")
        params.append(d_city.strip())
    if d_st.strip():
        where.append("UPPER(\"D-St\") = UPPER(?)")
        params.append(d_st.strip())

    if o_dh_max is not None:
        where.append("\"O-DH\" <= ?")
        params.append(o_dh_max)
    if d_dh_max is not None:
        where.append("\"D-DH\" <= ?")
        params.append(d_dh_max)

    if states:
        placeholders = ",".join(["?"] * len(states))
        where.append(f"state IN ({placeholders})")
        params.extend(states)

    cols = {row[1] for row in cur.execute("PRAGMA table_info(loads)").fetchall()}
    if only_unscored and "match_score" in cols:
        where.append("match_score IS NULL")

    where_sql = " AND ".join(where) if where else "1=1"

    sql = f"""
    SELECT
        load_key,
        "O-City",
        "O-St",
        "D-City",
        "D-St",
        "O-DH",
        "D-DH",
        "Distance",
        "Rate",
        "RPM",
        "Weight",
        "Length",
        "Equip",
        "Mode",
        "Pickup",
        "Company",
        "Updated",
        "D2P",
        state,
        first_seen_at,
        last_seen_at,
        shortlist_tag,
        shortlisted_at,
        match_score
    FROM loads
    WHERE {where_sql}
    ORDER BY
        CASE WHEN match_score IS NULL THEN 1 ELSE 0 END,
        match_score DESC,
        CASE state WHEN 'READY' THEN 0 WHEN 'NEW' THEN 1 ELSE 2 END,
        first_seen_at DESC
    LIMIT ? OFFSET ?
    """

    rows = cur.execute(sql, params + [limit, offset]).fetchall()
    con.close()
    results = [dict(row) for row in rows]
    write_timing({"layer": "server", "op": "query", "ms": int((time.perf_counter() - start) * 1000)})
    return results


def score_tagged_loads(
    *,
    db_path: str,
    tag: str,
    only_unscored: bool = False,
    limit: int = 200,
) -> Dict[str, Any]:
    start = time.perf_counter()
    if not tag:
        raise ValueError("Tag is required")

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    ensure_columns(con)
    cur = con.cursor()

    where = ["shortlist_tag = ?"]
    params: List[Any] = [tag]

    if only_unscored:
        where.append("match_score IS NULL")

    where_sql = " AND ".join(where)
    sql = f"""
    SELECT load_key, "Rate", "D2P"
    FROM loads
    WHERE {where_sql}
    ORDER BY
        CASE state WHEN 'READY' THEN 0 WHEN 'NEW' THEN 1 ELSE 2 END,
        first_seen_at DESC
    LIMIT ?
    """
    rows = cur.execute(sql, params + [limit]).fetchall()

    updates = []
    for row in rows:
        score = math_match_score(dict(row))
        updates.append((score, row["load_key"]))

    if updates:
        cur.executemany(
            "UPDATE loads SET match_score=? WHERE load_key=?",
            updates,
        )
    con.commit()
    con.close()
    write_timing({"layer": "server", "op": "score", "ms": int((time.perf_counter() - start) * 1000)})
    return {
        "tag": tag,
        "scored": len(updates),
    }




@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/shutdown")
def shutdown() -> dict:
    cb = getattr(app.state, "shutdown_cb", None)
    if callable(cb):
        cb()
        return {"status": "shutting_down"}
    return {"status": "unavailable"}


@app.post("/scrape")
def scrape_endpoint(req: ScrapeRequest) -> dict:
    try:
        result = run_scrape(
            db_path=req.db_path if req.db_path is not None else DB_PATH,
            sample_path=req.sample_path if req.sample_path is not None else SAMPLE_LOADS_PATH,
            overwrite=req.overwrite if req.overwrite is not None else False,
        )
    except (ValueError, FileNotFoundError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return result


@app.post("/shortlist")
def shortlist_endpoint(req: ShortlistRequest) -> dict:
    result = run_shortlist(
        db_path=req.db_path if req.db_path is not None else DB_PATH,
        tag=req.tag if req.tag is not None else "DEFAULT",
        date=req.date,
        o_city=req.o_city if req.o_city is not None else "",
        o_st=req.o_st if req.o_st is not None else "",
        d_city=req.d_city if req.d_city is not None else "",
        d_st=req.d_st if req.d_st is not None else "",
        o_dh_max=req.o_dh_max,
        d_dh_max=req.d_dh_max,
        replace=req.replace if req.replace is not None else False,
        limit=req.limit if req.limit is not None else 200,
        only_unscored=req.only_unscored if req.only_unscored is not None else False,
    )
    return result


@app.post("/loads/query")
def loads_query_endpoint(req: LoadsQueryRequest) -> dict:
    result = query_loads(
        db_path=req.db_path if req.db_path is not None else DB_PATH,
        tag=req.tag,
        date=req.date,
        o_city=req.o_city if req.o_city is not None else "",
        o_st=req.o_st if req.o_st is not None else "",
        d_city=req.d_city if req.d_city is not None else "",
        d_st=req.d_st if req.d_st is not None else "",
        o_dh_max=req.o_dh_max,
        d_dh_max=req.d_dh_max,
        states=req.states,
        only_unscored=req.only_unscored if req.only_unscored is not None else False,
        limit=req.limit if req.limit is not None else 200,
        offset=req.offset if req.offset is not None else 0,
    )
    return {"results": result, "count": len(result)}


@app.post("/loads/score")
def loads_score_endpoint(req: ScoreLoadsRequest) -> dict:
    try:
        result = score_tagged_loads(
            db_path=req.db_path if req.db_path is not None else DB_PATH,
            tag=req.tag.strip() if req.tag is not None else "",
            only_unscored=req.only_unscored if req.only_unscored is not None else False,
            limit=req.limit if req.limit is not None else 200,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return result




@app.post("/pipeline")
def pipeline_endpoint(req: PipelineRequest) -> dict:
    start = time.perf_counter()
    scrape_req = req.scrape or ScrapeRequest()
    shortlist_req = req.shortlist or ShortlistRequest()

    scrape_result = scrape_endpoint(scrape_req)
    shortlist_result = shortlist_endpoint(shortlist_req)

    response = {
        "scrape": scrape_result,
        "shortlist": shortlist_result,
    }
    write_timing({"layer": "server", "op": "pipeline", "ms": int((time.perf_counter() - start) * 1000)})
    return response


class ClientTimingRequest(BaseModel):
    label: str
    ms: int


@app.post("/timing")
def timing_endpoint(req: ClientTimingRequest) -> dict:
    write_timing({"layer": "client", "op": req.label, "ms": req.ms})
    return {"status": "ok"}
