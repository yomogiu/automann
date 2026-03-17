from __future__ import annotations

import argparse
import curses
import json

from libs.config import get_settings

from .client import APIClient
from .ui import run_dashboard


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Life system operator console")
    parser.add_argument(
        "--once",
        choices=["health", "runs", "reports", "artifacts", "dashboard"],
        help="Fetch one API payload and print JSON instead of launching the TUI.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = get_settings()
    client = APIClient(settings)
    if args.once:
        if args.once == "dashboard":
            payload = client.fetch_dashboard()
        else:
            payload = client.get_json(f"/{args.once}")
        print(json.dumps(payload, indent=2, default=str))
        return 0
    return int(curses.wrapper(run_dashboard, client, settings))


if __name__ == "__main__":
    raise SystemExit(main())
