from __future__ import annotations

from typing import Any

import httpx

from libs.config import Settings


class APIClient:
    def __init__(self, settings: Settings):
        self.base_url = f"http://{settings.api_host}:{settings.api_port}"

    def get_json(self, path: str) -> dict[str, Any]:
        response = httpx.get(f"{self.base_url}{path}", timeout=10.0)
        response.raise_for_status()
        return response.json()

    def post_json(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        response = httpx.post(f"{self.base_url}{path}", json=payload, timeout=30.0)
        response.raise_for_status()
        return response.json()

    def fetch_dashboard(self) -> dict[str, Any]:
        return {
            "health": self.get_json("/health"),
            "runs": self.get_json("/runs?limit=15").get("runs", []),
            "reports": self.get_json("/reports?limit=15").get("reports", []),
            "artifacts": self.get_json("/artifacts?limit=15").get("artifacts", []),
        }
