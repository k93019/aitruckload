import argparse
import os
import re
import time
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


class TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: List[str] = []

    def handle_data(self, data: str) -> None:
        if data:
            self.parts.append(data)


def html_to_text(html: str) -> str:
    parser = TextExtractor()
    parser.feed(html)
    text = " ".join(parser.parts)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def fetch_description(url: str, timeout_sec: int = 30, max_chars: int = 8000) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; JobFinderBot/1.0)"
    }
    resp = requests.get(url, headers=headers, timeout=timeout_sec, allow_redirects=True)
    resp.raise_for_status()
    text = html_to_text(resp.text)
    if len(text) > max_chars:
        text = text[:max_chars]
    return text


def parse_score(text: str) -> float:
    match = re.search(r"(\d+(?:\.\d+)?)", text)
    if not match:
        raise ValueError(f"No numeric score found in LLM response: {text!r}")
    score = float(match.group(1))
    if score < 0:
        score = 0.0
    if score > 10:
        score = 10.0
    return score


def call_openai_score(
    *,
    api_key: str,
    model: str,
    kb_text: str,
    job: Dict[str, Any],
    description_text: str,
) -> float:
    system_msg = "Return ONLY a number from 0 to 10. No extra text."
    user_msg = (
        "You are scoring job fit based on the knowledge base.\n"
        "Return only a single number from 0 to 10.\n\n"
        "Knowledge base:\n"
        f"{kb_text}\n\n"
        "Job:\n"
        f"Title: {job.get('title')}\n"
        f"Company: {job.get('company')}\n"
        f"Location: {job.get('location')}\n"
        f"URL: {job.get('redirect_url')}\n\n"
        "Job description:\n"
        f"{description_text}\n\n"
        "Scoring rubric:\n"
        "- 0-3: Poor fit or unrelated.\n"
        "- 4-6: Partial fit with notable gaps.\n"
        "- 7-8: Good fit.\n"
        "- 9-10: Excellent fit.\n"
    )

    payload = {
        "model": model,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": system_msg},
            {"role": "user", "content": user_msg},
        ],
    }

    resp = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    content = data["choices"][0]["message"]["content"].strip()
    return parse_score(content)


def post_json(api_base: str, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{api_base.rstrip('/')}{path}"
    resp = requests.post(url, json=payload, timeout=60)
    resp.raise_for_status()
    return resp.json()


def load_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Run job matching agent pipeline")
    ap.add_argument("--api-base", default="http://127.0.0.1:8000", help="Job API base URL")
    ap.add_argument("--tag", default="DEFAULT", help="Shortlist tag to pull")
    ap.add_argument("--location", default="", help="Location filter for shortlist")
    ap.add_argument("--keyword", action="append", default=[], help="Keyword filter (repeatable)")
    ap.add_argument("--days", type=int, default=7, help="Only include jobs from last N days")
    ap.add_argument("--limit", type=int, default=50, help="Max jobs to score")
    ap.add_argument("--replace", action="store_true", help="Replace shortlist tag before run")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing match_score")
    ap.add_argument("--kb-path", default="accomplishment_inventory.md", help="Knowledge base file")
    ap.add_argument("--model", default="gpt-4.1-mini", help="OpenAI model")
    ap.add_argument("--skip-scrape", action="store_true", help="Skip external API scrape")
    args = ap.parse_args()

    api_key = os.getenv("CHATGPT_API_KEY")
    if not api_key:
        raise SystemExit("Set CHATGPT_API_KEY env var with your OpenAI API key.")

    kb_path = Path(args.kb_path)
    if not kb_path.exists():
        raise SystemExit(f"Knowledge base not found: {kb_path}")

    kb_text = load_text(kb_path)

    scrape_result: Optional[Dict[str, Any]] = None
    if not args.skip_scrape:
        scrape_result = post_json(args.api_base, "/scrape", {})
    shortlist_result = post_json(
        args.api_base,
        "/shortlist",
        {
            "tag": args.tag,
            "location": args.location,
            "keywords": args.keyword,
            "days": args.days,
            "replace": args.replace,
            "limit": args.limit,
            "only_unscored": not args.overwrite,
        },
    )

    query_result = post_json(
        args.api_base,
        "/jobs/query",
        {
            "tag": args.tag,
            "days": args.days,
            "location": args.location,
            "keywords": args.keyword,
            "only_unscored": not args.overwrite,
            "require_description": False,
            "limit": args.limit,
        },
    )

    jobs = query_result.get("results", [])
    described = 0
    scored = 0
    skipped = 0

    for job in jobs:
        description_text = job.get("description_text")
        if not description_text:
            url = job.get("redirect_url")
            if not url:
                skipped += 1
                continue
            try:
                description_text = fetch_description(url)
            except requests.RequestException:
                skipped += 1
                continue

            post_json(
                args.api_base,
                "/jobs/describe",
                {
                    "job_key": job["job_key"],
                    "description_text": description_text,
                    "description_source": "scrape",
                },
            )
            described += 1

        try:
            score = call_openai_score(
                api_key=api_key,
                model=args.model,
                kb_text=kb_text,
                job=job,
                description_text=description_text,
            )
        except requests.RequestException:
            skipped += 1
            continue

        post_json(
            args.api_base,
            "/jobs/match-score",
            {
                "job_key": job["job_key"],
                "match_score": score,
            },
        )
        scored += 1
        time.sleep(0.2)

    if scrape_result is not None:
        print("Scrape:", scrape_result)
    print("Shortlist:", shortlist_result)
    print("Queried jobs:", len(jobs))
    print(f"Descriptions scraped: {described}")
    print(f"Scored: {scored}")
    print(f"Skipped: {skipped}")


if __name__ == "__main__":
    main()
