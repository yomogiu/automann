from __future__ import annotations

import subprocess

from libs.config import Settings


def agentboard_available(settings: Settings) -> bool:
    return (settings.agentboard_path / "todo.py").exists()


def launch_agentboard(settings: Settings) -> int:
    return subprocess.run(
        ["python3", "todo.py", "tui"],
        cwd=settings.agentboard_path,
        check=False,
    ).returncode
