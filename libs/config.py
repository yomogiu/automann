from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


ROOT_DIR = Path(__file__).resolve().parents[1]


def _sqlite_url(path: Path, *, driver: str) -> str:
    return f"sqlite+{driver}:///{path.resolve()}"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=ROOT_DIR / ".env",
        env_prefix="LIFE_",
        extra="ignore",
    )

    environment: str = Field(default="development")
    api_host: str = Field(default="127.0.0.1")
    api_port: int = Field(default=8000)

    artifact_root: Path = Field(default=ROOT_DIR / "data" / "artifacts")
    report_root: Path = Field(default=ROOT_DIR / "data" / "reports")
    runtime_root: Path = Field(default=ROOT_DIR / "data" / "runtime")
    life_database_url: str = Field(
        default_factory=lambda: _sqlite_url(ROOT_DIR / "data" / "runtime" / "life.db", driver="pysqlite"),
        validation_alias="LIFE_DATABASE_URL",
    )
    prefect_database_url: str = Field(
        default_factory=lambda: _sqlite_url(ROOT_DIR / "data" / "runtime" / "prefect.db", driver="aiosqlite"),
        validation_alias="LIFE_PREFECT_DATABASE_URL",
    )

    prefect_api_url: str = Field(default="http://127.0.0.1:4200/api")
    prefect_ui_url: str = Field(default="http://127.0.0.1:4200")
    prefect_server_host: str = Field(default="127.0.0.1")
    prefect_server_port: int = Field(default=4200)
    prefect_profile_name: str = Field(default="life-system")

    github_owner: str = Field(default="")
    github_repo: str = Field(default="")
    github_branch: str = Field(default="main")
    github_release_prefix: str = Field(default="life-report")

    ollama_base_url: str = Field(default="http://127.0.0.1:11434")
    agentboard_path: Path = Field(default=ROOT_DIR.parent / "TODO-agentboard")
    embedding_dimensions: int = Field(default=1536)


def get_settings() -> Settings:
    return Settings()
