from __future__ import annotations

import curses
from typing import Any

from .agentboard_bridge import agentboard_available, launch_agentboard
from .client import APIClient


TAB_ORDER = ["runs", "reports", "artifacts", "control"]


def _truncate(text: str, width: int) -> str:
    if len(text) <= width:
        return text
    return text[: max(0, width - 1)] + "…"


def _prompt(stdscr: curses.window, label: str) -> str:
    h, w = stdscr.getmaxyx()
    curses.echo()
    try:
        stdscr.move(h - 2, 2)
        stdscr.clrtoeol()
        stdscr.addstr(h - 2, 2, _truncate(label, w - 4))
        value = stdscr.getstr(h - 1, 2, max(1, w - 4)).decode("utf-8").strip()
        return value
    finally:
        curses.noecho()


def _render_rows(stdscr: curses.window, rows: list[str], *, start_row: int = 3) -> None:
    h, w = stdscr.getmaxyx()
    for idx, row in enumerate(rows[: max(0, h - start_row - 3)]):
        stdscr.addstr(start_row + idx, 2, _truncate(row, w - 4))


def run_dashboard(stdscr: curses.window, client: APIClient, settings) -> int:
    curses.curs_set(0)
    stdscr.nodelay(False)
    stdscr.keypad(True)

    selected_tab = 0
    status_line = "Press r to refresh, d daily brief, p paper review, s draft, b agentboard, q quit."
    state: dict[str, Any] = {}

    def refresh() -> None:
        nonlocal state, status_line
        try:
            state = client.fetch_dashboard()
            status_line = "Dashboard refreshed."
        except Exception as exc:  # pragma: no cover - requires live API
            state = {"health": {"status": "error"}, "runs": [], "reports": [], "artifacts": []}
            status_line = f"Refresh failed: {exc}"

    refresh()

    while True:
        stdscr.erase()
        h, w = stdscr.getmaxyx()
        title = "Auto Mann Operator Console"
        stdscr.addstr(0, 2, _truncate(title, w - 4), curses.A_BOLD)

        tabs = " | ".join(
            f"[{idx + 1}] {name.upper()}" if idx != selected_tab else f"[{idx + 1}] {name.upper()} *"
            for idx, name in enumerate(TAB_ORDER)
        )
        stdscr.addstr(1, 2, _truncate(tabs, w - 4))

        current = TAB_ORDER[selected_tab]
        rows: list[str] = []
        if current == "runs":
            runs = state.get("runs", [])
            rows = [
                f"{item['status']:<10} {item['flow_name']:<22} {item['id']}"
                for item in runs
            ] or ["No runs yet."]
        elif current == "reports":
            reports = state.get("reports", [])
            rows = [
                f"{item['report_type']:<16} {item['title']}"
                for item in reports
            ] or ["No reports yet."]
        elif current == "artifacts":
            artifacts = state.get("artifacts", [])
            rows = [
                f"{item['kind']:<18} {item['path']}"
                for item in artifacts
            ] or ["No artifacts yet."]
        else:
            health = state.get("health", {})
            rows = [
                f"API status: {health.get('status', 'unknown')}",
                f"Prefect API: {health.get('prefect_api_url', 'n/a')}",
                f"Auto Mann DB: {health.get('life_database_url', 'n/a')}",
                f"Agentboard bridge: {'available' if agentboard_available(settings) else 'missing'}",
            ]

        _render_rows(stdscr, rows)
        stdscr.addstr(h - 2, 2, _truncate(status_line, w - 4))
        stdscr.refresh()

        key = stdscr.getch()
        if key in (ord("q"), 27):
            return 0
        if key in (ord("1"), ord("2"), ord("3"), ord("4")):
            selected_tab = int(chr(key)) - 1
            continue
        if key == ord("r"):
            refresh()
            continue
        if key == ord("d"):
            try:
                client.post_json("/commands/daily-brief", {})
                status_line = "Triggered daily brief."
                refresh()
            except Exception as exc:  # pragma: no cover - requires live API
                status_line = f"Daily brief failed: {exc}"
            continue
        if key == ord("p"):
            paper_id = _prompt(stdscr, "Paper ID:")
            if paper_id:
                try:
                    client.post_json("/commands/paper-review", {"paper_id": paper_id})
                    status_line = f"Triggered paper review for {paper_id}."
                    refresh()
                except Exception as exc:  # pragma: no cover - requires live API
                    status_line = f"Paper review failed: {exc}"
            continue
        if key == ord("s"):
            theme = _prompt(stdscr, "Draft theme:")
            if theme:
                try:
                    client.post_json("/commands/draft-article", {"theme": theme})
                    status_line = f"Triggered draft for '{theme}'."
                    refresh()
                except Exception as exc:  # pragma: no cover - requires live API
                    status_line = f"Draft failed: {exc}"
            continue
        if key == ord("b"):
            if not agentboard_available(settings):
                status_line = "TODO-agentboard not found."
                continue
            curses.endwin()
            launch_agentboard(settings)
            stdscr.refresh()
            status_line = "Returned from TODO-agentboard."

