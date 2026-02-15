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
      font-size: 17px;
      color: var(--ink);
      background: radial-gradient(circle at top left, #ffffff 0%, #f0efe9 45%, #eceae5 100%);
    }
    header {
      padding: 28px 24px 16px;
      width: 100%;
      max-width: 100vw;
      margin: 0 auto;
    }
    .header-inner {
      display: flex;
      gap: 16px;
      align-items: center;
      justify-content: space-between;
      flex-wrap: wrap;
    }
    .header-main {
      min-width: 240px;
    }
    .header-actions {
      display: flex;
      gap: 12px;
      align-items: center;
      justify-content: flex-end;
      flex-wrap: wrap;
    }
    .header-actions label {
      font-size: 13px;
      text-transform: none;
      letter-spacing: 0;
    }
    .header-actions .status {
      margin-top: 0;
      min-width: 240px;
      text-align: right;
    }
    .header-actions .auto-refresh-status {
      font-size: 13px;
      color: var(--muted);
      white-space: nowrap;
    }
    h1 {
      margin: 0 0 6px;
      font-size: 34px;
      letter-spacing: 0.2px;
    }
    p.subtitle {
      margin: 0;
      color: var(--muted);
      font-size: 17px;
    }
    .filters {
      padding: 0 24px 24px;
      width: 100%;
      max-width: 100vw;
      margin: 0 auto 8px;
    }
    .results {
      padding: 0 24px 24px;
      width: 100%;
      max-width: 100vw;
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
      font-size: 22px;
    }
    .controls {
      display: grid;
      gap: 12px;
      grid-template-columns: repeat(3, minmax(180px, 1fr));
    }
    label {
      display: grid;
      gap: 6px;
      font-size: 14px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.6px;
    }
    input, select {
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 12px 12px;
      font-size: 17px;
      background: #fff;
      color: var(--ink);
    }
    .actions {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 12px;
    }
    .hint {
      margin: 8px 0 0;
      color: var(--muted);
      font-size: 14px;
    }
    details.advanced {
      margin-top: 12px;
      padding-top: 8px;
      border-top: 1px dashed var(--border);
    }
    details.advanced summary {
      cursor: pointer;
      font-weight: 600;
      color: #2b2b2f;
    }
    .advanced-controls {
      margin-top: 10px;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    }
    button {
      border: none;
      border-radius: 10px;
      padding: 14px 18px;
      font-weight: 700;
      font-size: 17px;
      cursor: pointer;
      background: var(--accent);
      color: var(--accent-ink);
      transition: transform 0.15s ease, box-shadow 0.15s ease;
    }
    button:disabled {
      opacity: 0.45;
      cursor: not-allowed;
      box-shadow: none;
      transform: none;
    }
    body.auto-refresh-active button {
      opacity: 0.35;
    }
    body[aria-busy="true"] input,
    body[aria-busy="true"] select,
    body[aria-busy="true"] textarea {
      opacity: 0.6;
    }
    #btn-scrape {
      min-width: 210px;
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
      font-size: 15px;
      color: var(--muted);
      min-height: 18px;
    }
    .table-wrap {
      overflow-x: auto;
    }
    table {
      width: 100%;
      min-width: 1200px;
      border-collapse: collapse;
      font-size: 15px;
      line-height: 1.5;
    }
    th, td {
      border-bottom: 1px solid var(--border);
      padding: 12px 14px;
      text-align: left;
      white-space: nowrap;
    }
    th {
      background: #f3f4f6;
      font-size: 14px;
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
      font-size: 13px;
    }
    @media (max-width: 720px) {
      header { padding: 20px 16px 8px; }
      .filters { padding: 0 16px 16px; }
      .results { padding: 0 16px 20px; }
    }
    @media (max-width: 980px) {
      .header-actions { justify-content: flex-start; }
      .header-actions .status { text-align: left; min-width: 0; }
      .controls { grid-template-columns: repeat(2, minmax(160px, 1fr)); }
    }
    @media (max-width: 640px) {
      .controls { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <header>
    <div class=\"header-inner\">
      <div class=\"header-main\">
        <h1>Truck load finder</h1>
        <p class=\"subtitle\">Retrieves data from the web, applies filters, and shows results.</p>
      </div>
      <div class=\"header-actions\">
        <label>Overwrite existing data
          <select id=\"overwrite\">
            <option value=\"false\" selected>False</option>
            <option value=\"true\">True</option>
          </select>
        </label>
        <label>Auto-refresh
          <select id=\"auto_refresh\">
            <option value=\"0\" selected>Off</option>
            <option value=\"20\">20 s</option>
            <option value=\"60\">1 min</option>
            <option value=\"600\">10 min</option>
          </select>
        </label>
        <button id=\"btn-scrape\">Retrieve data</button>
        <div class=\"auto-refresh-status\" id=\"auto-refresh-status\"></div>
        <div class=\"status\" id=\"status-scrape\"></div>
      </div>
    </div>
  </header>

  <div class=\"filters\">
    <section>
      <h2>Process data with filters</h2>
      <div class=\"controls\">
        <label>Tag<input id=\"tag\" value=\"\" /></label>
        <label>Date<input id=\"date\" value=\"\" /></label>
        <label>Limit<input id=\"limit\" type=\"number\" min=\"1\" step=\"1\" value=\"50\" /></label>
        <label>O-City<input id=\"o_city\" value=\"\" /></label>
        <label>O-St<input id=\"o_st\" value=\"\" /></label>
        <label>O-DH max<input id=\"o_dh\" type=\"number\" min=\"0\" step=\"1\" /></label>
        <label>D-City<input id=\"d_city\" value=\"\" /></label>
        <label>D-St<input id=\"d_st\" value=\"\" /></label>
        <label>D-DH max<input id=\"d_dh\" type=\"number\" min=\"0\" step=\"1\" /></label>
        <label>Show unscored
          <select id=\"show_unscored\">
            <option value=\"true\" selected>True</option>
            <option value=\"false\">False</option>
          </select>
        </label>
        <label>Min match score
          <input id=\"min_score\" type=\"number\" min=\"0\" step=\"0.1\" value=\"0\" />
        </label>
      </div>
      <details class=\"advanced\">
        <summary>Advanced</summary>
        <div class=\"controls advanced-controls\">
          <label>Tag only unscored
            <select id=\"only_unscored\">
              <option value=\"true\">True</option>
              <option value=\"false\" selected>False</option>
            </select>
          </label>
          <label>Clear tag first
            <select id=\"replace\">
              <option value=\"true\" selected>True</option>
              <option value=\"false\">False</option>
            </select>
          </label>
        </div>
      </details>
      <div class=\"actions\">
        <button id=\"btn-shortlist\" class=\"secondary\">Set filters</button>
      </div>
      <p class=\"hint\">Set filters: Applies the above filters.</p>
      <p class=\"hint\" id=\"status-working\"></p>
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
    const statusWorking = document.getElementById("status-working");
    const tableBody = document.querySelector("#results-table tbody");
    const autoRefreshStatus = document.getElementById("auto-refresh-status");
    const scrapeButton = document.getElementById("btn-scrape");
    const scrapeButtonLabel = scrapeButton.textContent;
    let refreshTimer = null;
    let countdownTimer = null;
    let nextRefreshAt = null;
    let hasRunFilters = false;
    let isRunning = false;

    function clampNonNegative(value, fallback) {
      if (!Number.isFinite(value)) return fallback;
      return Math.max(0, value);
    }

    function optionalInt(value) {
      const text = String(value ?? "").trim();
      if (!text) return null;
      const parsed = parseInt(text, 10);
      if (!Number.isFinite(parsed)) return null;
      return Math.max(0, parsed);
    }

    function payloadFromForm() {
      const oDh = optionalInt(document.getElementById("o_dh").value);
      const dDh = optionalInt(document.getElementById("d_dh").value);
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

    function refreshIntervalSeconds() {
      const raw = document.getElementById("auto_refresh").value;
      const parsed = parseInt(raw, 10);
      return Number.isFinite(parsed) ? parsed : 0;
    }

    function updateAutoRefreshStatus({ running } = { running: false }) {
      const seconds = refreshIntervalSeconds();
      if (seconds <= 0) {
        autoRefreshStatus.textContent = "";
        return;
      }
      autoRefreshStatus.textContent = "";
    }

    function formatCountdown(seconds) {
      const safeSeconds = Math.max(0, Math.floor(seconds));
      const minutes = Math.floor(safeSeconds / 60);
      const remaining = safeSeconds % 60;
      return `${String(minutes).padStart(2, "0")}:${String(remaining).padStart(2, "0")}`;
    }

    function updateCountdownLabel() {
      if (!nextRefreshAt) {
        return;
      }
      const remainingSeconds = Math.max(0, Math.ceil((nextRefreshAt - Date.now()) / 1000));
      scrapeButton.textContent = `Next refresh in ${formatCountdown(remainingSeconds)}`;
    }

    function clearCountdown() {
      if (countdownTimer) {
        clearInterval(countdownTimer);
        countdownTimer = null;
      }
      nextRefreshAt = null;
    }

    function updateAutoRefreshControls() {
      const seconds = refreshIntervalSeconds();
      if (scrapeButton) {
        scrapeButton.disabled = seconds > 0;
      }
      if (seconds <= 0) {
        clearCountdown();
        scrapeButton.textContent = scrapeButtonLabel;
        return;
      }
      if (!hasRunFilters) {
        scrapeButton.textContent = "Auto refresh pending";
        return;
      }
      updateCountdownLabel();
    }

    function setAutoRefresh() {
      if (refreshTimer) {
        clearInterval(refreshTimer);
        refreshTimer = null;
      }
      const seconds = refreshIntervalSeconds();
      if (seconds > 0 && hasRunFilters) {
        nextRefreshAt = Date.now() + seconds * 1000;
        updateCountdownLabel();
        if (!countdownTimer) {
          countdownTimer = setInterval(updateCountdownLabel, 1000);
        }
        refreshTimer = setInterval(() => {
          runFiltersAndLoad({ runScrape: true });
        }, seconds * 1000);
      } else {
        clearCountdown();
      }
      updateAutoRefreshStatus({ running: false });
      updateAutoRefreshControls();
    }

    function setBusyState(isBusy) {
      const controls = document.querySelectorAll("button, input, select, textarea");
      controls.forEach(control => {
        control.disabled = isBusy;
      });
      document.body.setAttribute("aria-busy", isBusy ? "true" : "false");
      if (statusWorking) {
        statusWorking.textContent = isBusy ? "Working..." : "";
      }
      if (!isBusy) {
        updateAutoRefreshControls();
      }
    }

    async function runScrape() {
      statusScrape.textContent = "Running scrape...";
      const overwrite = document.getElementById("overwrite").value === "true";
      const result = await postJson("/scrape", { overwrite });
      statusScrape.textContent = `Retrieved ${result.total_returned}. Inserted ${result.inserted}, updated ${result.updated}. Total in DB: ${result.total_in_db}.`;
      return result;
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

    async function runFiltersAndLoad({ runScrape: shouldScrape = false } = {}) {
      if (isRunning) return;
      isRunning = true;
      if (shouldScrape) {
        document.body.classList.add("auto-refresh-active");
        clearCountdown();
        scrapeButton.textContent = "Refreshing...";
      }
      setBusyState(true);
      updateAutoRefreshStatus({ running: true });
      statusShortlist.textContent = shouldScrape
        ? "Refreshing data, tagging, and scoring..."
        : "Tagging and scoring...";
      try {
        if (shouldScrape) {
          await runScrape();
        }
        const payload = payloadFromForm();
        const shortlistResult = await postJson("/shortlist", payload);
        const tag = shortlistResult.tag;
        const scoreResult = await postJson("/loads/score", {
          tag,
          only_unscored: payload.only_unscored,
          limit: payload.limit
        });
        const queryPayload = {
          ...payload,
          tag,
          only_unscored: false
        };
        const result = await postJson("/loads/query", queryPayload);
        const threshold = minScore();
        const showUnscored = document.getElementById("show_unscored").value === "true";
        let hiddenUnscored = 0;
        const sorted = (result.results || []).slice().sort((a, b) => {
          const aScore = a.match_score;
          const bScore = b.match_score;
          if (aScore == null && bScore == null) return 0;
          if (aScore == null) return 1;
          if (bScore == null) return -1;
          return bScore - aScore;
        });
        const rows = sorted.filter(r => {
          if (r.match_score == null) {
            if (!showUnscored) hiddenUnscored += 1;
            return showUnscored;
          }
          return r.match_score >= threshold;
        });
        renderTable(rows);
        statusShortlist.textContent = `Tagged ${shortlistResult.marked}. Total tagged: ${shortlistResult.total}. Scored ${scoreResult.scored}.`;
        const hiddenText = !showUnscored && hiddenUnscored
          ? ` ${hiddenUnscored} hidden (unscored).`
          : "";
        statusResults.textContent = `Showing ${rows.length} of ${result.count} results (min match score ${threshold}).${hiddenText}`;
        hasRunFilters = true;
        setAutoRefresh();
        updateAutoRefreshControls();
      } catch (err) {
        statusShortlist.textContent = `Run filters failed: ${err.message}`;
      } finally {
        isRunning = false;
        setBusyState(false);
        updateAutoRefreshStatus({ running: false });
        if (shouldScrape) {
          document.body.classList.remove("auto-refresh-active");
        }
      }
    }

    document.getElementById("btn-scrape").addEventListener("click", async () => {
      try {
        await runScrape();
      } catch (err) {
        statusScrape.textContent = `Scrape failed: ${err.message}`;
      }
    });

    document.getElementById("btn-shortlist").addEventListener("click", async () => {
      await runFiltersAndLoad({ runScrape: false });
    });

    document.getElementById("auto_refresh").addEventListener("change", () => {
      setAutoRefresh();
    });

    updateAutoRefreshControls();
  </script>
</body>
</html>
"""

# -----------------------------
# CONFIG (edit as needed)
# -----------------------------
DB_PATH = os.getenv("LOADS_DB_PATH", "loads.db")
SAMPLE_LOADS_PATH = os.getenv("SAMPLE_LOADS_PATH", "sample_loads.json")

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
        CASE WHEN match_score IS NULL THEN 1 ELSE 0 END,
        match_score DESC,
        CASE state WHEN 'READY' THEN 0 WHEN 'NEW' THEN 1 ELSE 2 END,
        first_seen_at DESC
    LIMIT ? OFFSET ?
    """

    rows = cur.execute(sql, params + [limit, offset]).fetchall()
    con.close()

    return [dict(row) for row in rows]


def score_tagged_loads(
    *,
    db_path: str,
    tag: str,
    only_unscored: bool = False,
    limit: int = 200,
) -> Dict[str, Any]:
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

    return {
        "tag": tag,
        "scored": len(updates),
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
    scrape_req = req.scrape or ScrapeRequest()
    shortlist_req = req.shortlist or ShortlistRequest()

    scrape_result = scrape_endpoint(scrape_req)
    shortlist_result = shortlist_endpoint(shortlist_req)

    return {
        "scrape": scrape_result,
        "shortlist": shortlist_result,
    }
