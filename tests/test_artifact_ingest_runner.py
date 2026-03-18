from __future__ import annotations

import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from libs.config import get_settings
from libs.contracts.models import WorkerStatus
from workers.artifact_ingest_runner.runner import ArtifactIngestRunner


class _FakeHttpResponse:
    def __init__(self, *, url: str, text: str, media_type: str = "text/html") -> None:
        self.url = url
        self.text = text
        self.content = text.encode("utf-8")
        self.headers = {"content-type": media_type}

    def raise_for_status(self) -> None:
        return None


class _FakePdfPage:
    def __init__(self, text: str | None) -> None:
        self._text = text

    def extract_text(self) -> str | None:
        return self._text


class _FakePdfReader:
    def __init__(self, _stream, pages: list[_FakePdfPage]) -> None:
        self.pages = pages


class ArtifactIngestRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.env_overrides = {
            "LIFE_ARTIFACT_ROOT": str(self.root / "artifacts"),
            "LIFE_REPORT_ROOT": str(self.root / "reports"),
            "LIFE_RUNTIME_ROOT": str(self.root / "runtime"),
            "LIFE_DATABASE_URL": f"sqlite+pysqlite:///{self.root / 'runtime' / 'life.db'}",
        }
        self.previous_env = {key: os.environ.get(key) for key in self.env_overrides}
        for key, value in self.env_overrides.items():
            os.environ[key] = value

        self.runner = ArtifactIngestRunner(get_settings())

    def tearDown(self) -> None:
        for key, previous in self.previous_env.items():
            if previous is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = previous
        self.temp_dir.cleanup()

    def test_url_html_ingest_extracts_text_and_normalizes_canonical_url(self) -> None:
        html = """
        <html>
          <head>
            <title>StatsCan Labour Outlook</title>
            <meta name="author" content="Jane Analyst" />
            <meta property="article:published_time" content="2025-02-01T12:00:00Z" />
          </head>
          <body>
            <article><h1>StatsCan Labour Outlook</h1><p>Employment data suggests some clerical work is exposed to AI.</p></article>
          </body>
        </html>
        """

        with patch(
            "workers.artifact_ingest_runner.runner.httpx.get",
            return_value=_FakeHttpResponse(
                url="https://example.com/statscan/jobs?utm_source=feed&fbclid=tracking#overview",
                text=html,
            ),
        ):
            with patch("workers.artifact_ingest_runner.runner.trafilatura") as mock_trafilatura:
                mock_trafilatura.extract.return_value = "Employment data suggests some clerical work is exposed to AI."
                result = self.runner.run(
                    {
                        "items": [
                            {
                                "input_kind": "url",
                                "url": "https://example.com/statscan/jobs?utm_source=feed&fbclid=tracking#overview",
                            }
                        ],
                        "metadata": {},
                    }
                )

        self.assertEqual(result.status, WorkerStatus.COMPLETED)
        item = result.structured_outputs["items"][0]
        self.assertEqual(item["status"], "completed")
        self.assertEqual(item["canonical_uri"], "https://example.com/statscan/jobs")
        self.assertEqual(item["source_type"], "html")
        self.assertEqual(item["title"], "StatsCan Labour Outlook")
        self.assertEqual(item["author"], "Jane Analyst")
        self.assertEqual(item["published_at"], "2025-02-01T12:00:00Z")
        self.assertGreaterEqual(item["chunk_count"], 1)
        self.assertIn("example.com", item["tags"])

    def test_pdf_ingest_emits_page_aware_chunks(self) -> None:
        pdf_path = self.root / "inputs" / "report.pdf"
        pdf_path.parent.mkdir(parents=True, exist_ok=True)
        pdf_path.write_bytes(b"%PDF-1.4 placeholder")

        def fake_pdf_reader(stream):  # noqa: ANN001
            return _FakePdfReader(
                stream,
                [
                    _FakePdfPage("Visa acquisition history page one"),
                    _FakePdfPage("Visa acquisition history page two"),
                ],
            )

        with patch("workers.artifact_ingest_runner.runner.PdfReader", side_effect=fake_pdf_reader):
            result = self.runner.run(
                {
                    "items": [
                        {
                            "input_kind": "file",
                            "file_path": str(pdf_path),
                        }
                    ],
                    "metadata": {},
                }
            )

        self.assertEqual(result.status, WorkerStatus.COMPLETED)
        item = result.structured_outputs["items"][0]
        self.assertEqual(item["status"], "completed")
        self.assertEqual(item["source_type"], "pdf")
        self.assertEqual(item["canonical_uri"], pdf_path.resolve().as_uri())
        self.assertEqual(item["chunk_count"], 2)
        self.assertEqual([chunk["metadata"]["page"] for chunk in item["chunks"]], [1, 2])

    def test_pdf_without_extractable_text_is_marked_unsupported(self) -> None:
        pdf_path = self.root / "inputs" / "scan.pdf"
        pdf_path.parent.mkdir(parents=True, exist_ok=True)
        pdf_path.write_bytes(b"%PDF-1.4 placeholder")

        def fake_pdf_reader(stream):  # noqa: ANN001
            return _FakePdfReader(stream, [_FakePdfPage(None), _FakePdfPage("")])

        with patch("workers.artifact_ingest_runner.runner.PdfReader", side_effect=fake_pdf_reader):
            result = self.runner.run(
                {
                    "items": [
                        {
                            "input_kind": "file",
                            "file_path": str(pdf_path),
                        }
                    ],
                    "metadata": {},
                }
            )

        self.assertEqual(result.status, WorkerStatus.FAILED)
        item = result.structured_outputs["items"][0]
        self.assertEqual(item["status"], "unsupported")
        self.assertIn("unsupported_pdf_no_extractable_text", item["error"])
