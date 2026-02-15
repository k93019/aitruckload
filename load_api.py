import hashlib
import json
import os
import sqlite3
from datetime import UTC, datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field


app = FastAPI(title="Load Finder API", version="1.0")


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return """<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Load Finder</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f5f2;
      --card: #ffffff;
      --ink: #1d1d1f;
      --muted: #6b6b6f;
      --accent: #0b5ed7;
      --accent-ink: #ffffff;
      --border: #d9d9de;
      --shadow: 0 10px 30px rgba(0, 0, 0, 0.08);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Source Sans 3", "IBM Plex Sans", "Segoe UI", sans-serif;
      color: var(--ink);
      background: radial-gradient(circle at top left, #ffffff 0%, #f0efe9 45%, #eceae5 100%);
    }
    header {
      padding: 28px 24px 12px;
      max-width: 1200px;
      margin: 0 auto;
    }
    h1 {
      margin: 0 0 6px;
      font-size: 32px;
      letter-spacing: 0.2px;
    }
    p.subtitle {
      margin: 0;
      color: var(--muted);
      font-size: 16px;
    }
    .grid {
      display: grid;
      gap: 16px;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      padding: 12px 24px 24px;
      max-width: 1200px;
      margin: 0 auto;
    }
    .results {
      padding: 0 16px 24px;
      max-width: min(1600px, 90vw);
      margin: 0 auto 24px;
    }
    section {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 20px;
      box-shadow: var(--shadow);
    }
    section h2 {
      margin: 0 0 10px;
      font-size: 20px;
    }
    .controls {
      display: grid;
      gap: 12px;
      grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
    }
    label {
      display: grid;
      gap: 6px;
      font-size: 13px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.6px;
    }
    input, select {
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 12px 12px;
      font-size: 16px;
      background: #fff;
      color: var(--ink);
    }
    .actions {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 12px;
    }
    button {
      border: none;
      border-radius: 10px;
      padding: 14px 18px;
      font-weight: 700;
      font-size: 16px;
      cursor: pointer;
      background: var(--accent);
      color: var(--accent-ink);
      transition: transform 0.15s ease, box-shadow 0.15s ease;
    }
    button.secondary {
      background: #e9eef8;
      color: #12325f;
    }
    button:hover {
      transform: translateY(-1px);
      box-shadow: 0 8px 18px rgba(11, 94, 215, 0.18);
    }
    .status {
      margin-top: 10px;
      font-size: 14px;
      color: var(--muted);
      min-height: 18px;
    }
    .table-wrap {
      overflow-x: auto;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
      line-height: 1.4;
    }
    th, td {
      border-bottom: 1px solid var(--border);
      padding: 10px 12px;
      text-align: left;
      white-space: nowrap;
    }
    th {
      background: #f3f4f6;
      font-size: 13px;
      color: #2b2b2f;
      position: sticky;
      top: 0;
    }
    tbody tr:nth-child(even) {
      background: #fafafa;
    }
    .pill {
      display: inline-block;
      padding: 2px 8px;
      border-radius: 999px;
      background: #eff3ff;
      color: #2a4aa8;
      font-size: 12px;
    }
    @media (max-width: 720px) {
      header { padding: 20px 16px 8px; }
      .grid { padding: 8px 16px 16px; }
    }
  </style>
</head>
<body>
  <header>
    <h1>Load Finder</h1>
    <p class=\"subtitle\">Retrieve sample data, apply filters, and review results with match scores.</p>
  </header>

  <div class=\"grid\">
    <section>
      <h2>Retrieve data</h2>
      <p class=\"subtitle\">Load sample data into the database.</p>
      <div class=\"actions\">
        <button id=\"btn-scrape\">Retrieve data</button>
      </div>
      <div class=\"status\" id=\"status-scrape\"></div>
    </section>

    <section>
      <h2>Process data with filters</h2>
      <div class=\"controls\">
        <label>Tag<input id=\"tag\" value=\"DEFAULT\" /></label>
        <label>Date<input id=\"date\" value=\"TODAY\" /></label>
        <label>O-City<input id=\"o_city\" value=\"Houston\" /></label>
        <label>O-St<input id=\"o_st\" value=\"TX\" /></label>
        <label>D-City<input id=\"d_city\" value=\"San Antonio\" /></label>
        <label>D-St<input id=\"d_st\" value=\"TX\" /></label>
        <label>O-DH max<input id=\"o_dh\" type=\"number\" min=\"0\" step=\"1\" value=\"75\" /></label>
        <label>D-DH max<input id=\"d_dh\" type=\"number\" min=\"0\" step=\"1\" value=\"100\" /></label>
        <label>Limit<input id=\"limit\" type=\"number\" min=\"1\" step=\"1\" value=\"50\" /></label>
        <label>Only unscored
          <select id=\"only_unscored\">
            <option value=\"true\" selected>True</option>
            <option value=\"false\">False</option>
          </select>
        </label>
        <label>Replace tag
          <select id=\"replace\">
            <option value=\"true\" selected>True</option>
            <option value=\"false\">False</option>
          </select>
        </label>
        <label>Min match score
          <input id=\"min_score\" type=\"number\" min=\"0\" step=\"0.1\" value=\"5\" />
        </label>
      </div>
      <div class=\"actions\">
        <button id=\"btn-shortlist\" class=\"secondary\">Run filters</button>
        <button id=\"btn-query\">Load results</button>
      </div>
      <div class=\"status\" id=\"status-shortlist\"></div>
    </section>

  </div>

  <div class=\"results\">
    <section>
      <h2>Results</h2>
      <div class=\"status\" id=\"status-results\"></div>
      <div class=\"table-wrap\">
        <table id=\"results-table\">
          <thead>
            <tr>
              <th>Load Key</th>
              <th>O-City</th>
              <th>O-St</th>
              <th>D-City</th>
              <th>D-St</th>
              <th>Pickup</th>
              <th>Distance</th>
              <th>Rate</th>
              <th>RPM</th>
              <th>Equip</th>
              <th>Company</th>
              <th>Match</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </section>
  </div>

  <script>
    const statusScrape = document.getElementById("status-scrape");
    const statusShortlist = document.getElementById("status-shortlist");
    const statusResults = document.getElementById("status-results");
    const tableBody = document.querySelector("#results-table tbody");

    function clampNonNegative(value, fallback) {
      if (!Number.isFinite(value)) return fallback;
      return Math.max(0, value);
    }

    function payloadFromForm() {
      const oDh = clampNonNegative(parseInt(document.getElementById("o_dh").value, 10), 0);
      const dDh = clampNonNegative(parseInt(document.getElementById("d_dh").value, 10), 0);
      const limit = clampNonNegative(parseInt(document.getElementById("limit").value, 10), 1) || 1;
      return {
        tag: document.getElementById("tag").value,
        date: document.getElementById("date").value,
        "O-City": document.getElementById("o_city").value,
        "O-St": document.getElementById("o_st").value,
        "D-City": document.getElementById("d_city").value,
        "D-St": document.getElementById("d_st").value,
        "O-DH": oDh,
        "D-DH": dDh,
        limit: limit,
        only_unscored: document.getElementById("only_unscored").value === "true",
        replace: document.getElementById("replace").value === "true"
      };
    }

    function minScore() {
      const raw = document.getElementById("min_score").value;
      const parsed = parseFloat(raw);
      return clampNonNegative(parsed, 0);
    }

    async function postJson(path, payload) {
      const res = await fetch(path, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || res.statusText);
      }
      return res.json();
    }

    function renderTable(rows) {
      tableBody.innerHTML = "";
      rows.forEach(row => {
        const tr = document.createElement("tr");
        const match = row.match_score == null ? "" : row.match_score.toFixed(1);
        tr.innerHTML = [
          row.load_key,
          row["O-City"],
          row["O-St"],
          row["D-City"],
          row["D-St"],
          row["Pickup"],
          row["Distance"],
          row["Rate"],
          row["RPM"],
          row["Equip"],
          row["Company"],
          match ? `<span class=\"pill\">${match}</span>` : ""
        ].map(val => `<td>${val ?? ""}</td>`).join("");
        tableBody.appendChild(tr);
      });
    }

    document.getElementById("btn-scrape").addEventListener("click", async () => {
      statusScrape.textContent = "Running scrape...";
      try {
        const result = await postJson("/scrape", {});
        statusScrape.textContent = `Scrape complete. Inserted ${result.inserted}, updated ${result.updated}.`;
      } catch (err) {
        statusScrape.textContent = `Scrape failed: ${err.message}`;
      }
    });

    document.getElementById("btn-shortlist").addEventListener("click", async () => {
      statusShortlist.textContent = "Applying filters...";
      try {
        const payload = payloadFromForm();
        const result = await postJson("/shortlist", payload);
        statusShortlist.textContent = `Shortlisted ${result.marked}. Total tagged: ${result.total}.`;
      } catch (err) {
        statusShortlist.textContent = `Shortlist failed: ${err.message}`;
      }
    });

    document.getElementById("btn-query").addEventListener("click", async () => {
      statusResults.textContent = "Loading results...";
      try {
        const payload = payloadFromForm();
        const result = await postJson("/loads/query", payload);
        const threshold = minScore();
        const rows = (result.results || []).filter(r => {
          if (r.match_score == null) return false;
          return r.match_score >= threshold;
        });
        renderTable(rows);
        statusResults.textContent = `Showing ${rows.length} of ${result.count} results (min match score ${threshold}).`;
      } catch (err) {
        statusResults.textContent = `Load failed: ${err.message}`;
      }
    });
  </script>
</body>
</html>
"""

# -----------------------------
# CONFIG (edit as needed)
# -----------------------------
DB_PATH = os.getenv("LOADS_DB_PATH", "loads.db")
SAMPLE_LOADS_PATH = os.getenv("SAMPLE_LOADS_PATH", "sample_loads.json")

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


class MatchScoreRequest(BaseModel):
    db_path: Optional[str] = None
    load_key: str
    match_score: float


class PipelineRequest(BaseModel):
    scrape: Optional[ScrapeRequest] = None
    shortlist: Optional[ShortlistRequest] = None


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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
) -> Dict[str, Any]:
    con = sqlite3.connect(db_path)
    init_db(con)

    run_started = utc_now()
    inserted = 0
    updated = 0

    params_snapshot = {
        "sample_path": sample_path,
    }

    with open(sample_path, "r", encoding="utf-8") as handle:
        loads = json.load(handle)

    now = utc_now()
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
    con.close()

    return {
        "run_started": run_started,
        "pages_fetched": 1,
        "total_returned": len(loads),
        "inserted": inserted,
        "updated": updated,
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
        CASE state WHEN 'READY' THEN 0 WHEN 'NEW' THEN 1 ELSE 2 END,
        first_seen_at DESC
    LIMIT ? OFFSET ?
    """

    rows = cur.execute(sql, params + [limit, offset]).fetchall()
    con.close()

    return [dict(row) for row in rows]


def update_match_score(
    *,
    db_path: str,
    load_key: str,
    match_score: float,
) -> Dict[str, Any]:
    con = sqlite3.connect(db_path)
    ensure_columns(con)
    cur = con.cursor()
    cur.execute("SELECT load_key FROM loads WHERE load_key=?", (load_key,))
    if cur.fetchone() is None:
        con.close()
        raise ValueError("Load not found")

    cur.execute(
        "UPDATE loads SET match_score=? WHERE load_key=?",
        (match_score, load_key),
    )
    con.commit()
    con.close()

    return {
        "load_key": load_key,
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
            sample_path=req.sample_path if req.sample_path is not None else SAMPLE_LOADS_PATH,
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


@app.post("/loads/match-score")
def loads_match_score_endpoint(req: MatchScoreRequest) -> dict:
    try:
        result = update_match_score(
            db_path=req.db_path if req.db_path is not None else DB_PATH,
            load_key=req.load_key,
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
