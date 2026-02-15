import argparse
import hashlib
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, Optional

import requests


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
    instructions: str,
    load: Dict[str, Any],
) -> float:
    system_msg = "Return ONLY a number from 0 to 10. No extra text."
    user_msg = (
        "You are scoring truck load fit based on the instructions.\n"
        "Return only a single number from 0 to 10.\n\n"
        "Instructions:\n"
        f"{instructions}\n\n"
        "Load:\n"
        f"O-City: {load.get('O-City')}\n"
        f"O-St: {load.get('O-St')}\n"
        f"D-City: {load.get('D-City')}\n"
        f"D-St: {load.get('D-St')}\n"
        f"O-DH: {load.get('O-DH')}\n"
        f"D-DH: {load.get('D-DH')}\n"
        f"Distance: {load.get('Distance')}\n"
        f"Rate: {load.get('Rate')}\n"
        f"RPM: {load.get('RPM')}\n"
        f"Weight: {load.get('Weight')}\n"
        f"Length: {load.get('Length')}\n"
        f"Equip: {load.get('Equip')}\n"
        f"Mode: {load.get('Mode')}\n"
        f"Pickup: {load.get('Pickup')}\n"
        f"Company: {load.get('Company')}\n"
        f"Updated: {load.get('Updated')}\n"
        f"D2P: {load.get('D2P')}\n"
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


def mock_score(load_key: str) -> float:
    digest = hashlib.sha256(load_key.encode("utf-8")).hexdigest()
    score = (int(digest[:8], 16) % 101) / 10
    return round(score, 1)


def main() -> None:
    ap = argparse.ArgumentParser(description="Run load matching agent pipeline")
    ap.add_argument("--api-base", default="http://127.0.0.1:8000", help="Load API base URL")
    ap.add_argument("--tag", default="DEFAULT", help="Shortlist tag to pull")
    ap.add_argument("--date", default="TODAY", help="Pickup date filter (YYYY-MM-DD or TODAY)")
    ap.add_argument("--o-city", default="Houston", help="Origin city")
    ap.add_argument("--o-st", default="TX", help="Origin state")
    ap.add_argument("--d-city", default="San Antonio", help="Destination city")
    ap.add_argument("--d-st", default="TX", help="Destination state")
    ap.add_argument("--o-dh", type=int, default=75, help="Max origin deadhead")
    ap.add_argument("--d-dh", type=int, default=100, help="Max destination deadhead")
    ap.add_argument("--limit", type=int, default=50, help="Max loads to score")
    ap.add_argument("--replace", action="store_true", help="Replace shortlist tag before run")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing match_score")
    ap.add_argument("--instructions-path", default="load_scoring_instructions.txt", help="Optional scoring instructions file")
    ap.add_argument("--model", default="gpt-4.1-mini", help="OpenAI model")
    ap.add_argument("--skip-scrape", action="store_true", help="Skip sample data ingest")
    ap.add_argument("--mock-llm", action="store_true", help="Use deterministic mock scoring")
    args = ap.parse_args()

    api_key = os.getenv("CHATGPT_API_KEY")
    use_mock = args.mock_llm or not api_key
    api_key = api_key or ""

    instructions = "Provide a numeric match score from 0 to 10 for this load."
    instructions_path = Path(args.instructions_path)
    if instructions_path.exists():
        instructions = load_text(instructions_path)

    scrape_result: Optional[Dict[str, Any]] = None
    if not args.skip_scrape:
        scrape_result = post_json(args.api_base, "/scrape", {})
    shortlist_result = post_json(
        args.api_base,
        "/shortlist",
        {
            "tag": args.tag,
            "date": args.date,
            "O-City": args.o_city,
            "O-St": args.o_st,
            "D-City": args.d_city,
            "D-St": args.d_st,
            "O-DH": args.o_dh,
            "D-DH": args.d_dh,
            "replace": args.replace,
            "limit": args.limit,
            "only_unscored": not args.overwrite,
        },
    )

    query_result = post_json(
        args.api_base,
        "/loads/query",
        {
            "tag": args.tag,
            "date": args.date,
            "O-City": args.o_city,
            "O-St": args.o_st,
            "D-City": args.d_city,
            "D-St": args.d_st,
            "O-DH": args.o_dh,
            "D-DH": args.d_dh,
            "only_unscored": not args.overwrite,
            "limit": args.limit,
        },
    )

    loads = query_result.get("results", [])
    scored = 0
    skipped = 0

    for load in loads:
        try:
            if use_mock:
                score = mock_score(load["load_key"])
            else:
                score = call_openai_score(
                    api_key=api_key,
                    model=args.model,
                    instructions=instructions,
                    load=load,
                )
        except requests.RequestException:
            skipped += 1
            continue

        post_json(
            args.api_base,
            "/loads/match-score",
            {
                "load_key": load["load_key"],
                "match_score": score,
            },
        )
        scored += 1
        time.sleep(0.2)

    if scrape_result is not None:
        print("Scrape:", scrape_result)
    if use_mock and not args.mock_llm:
        print("Using mock scoring (CHATGPT_API_KEY not set)")
    print("Shortlist:", shortlist_result)
    print("Queried loads:", len(loads))
    print(f"Scored: {scored}")
    print(f"Skipped: {skipped}")


if __name__ == "__main__":
    main()
