from __future__ import annotations

import argparse
from pathlib import Path

from libs.config import get_settings
from libs.db import bootstrap_life_database


def ensure_directories() -> None:
    settings = get_settings()
    for path in (settings.artifact_root, settings.report_root, settings.runtime_root):
        Path(path).mkdir(parents=True, exist_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Bootstrap automann directories and the automann database")
    parser.add_argument("--skip-db", action="store_true", help="Skip automann database initialization")
    args = parser.parse_args()

    ensure_directories()
    if not args.skip_db:
        bootstrap_life_database(get_settings())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
