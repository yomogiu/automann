from __future__ import annotations

import json
from pathlib import Path
import unittest

from workers.codex_runner import load_paper_review_prompt


class PaperAnnotationsSchemaTests(unittest.TestCase):
    def test_schema_has_required_top_level_sections(self) -> None:
        schema_path = Path(__file__).resolve().parents[1] / "schemas" / "paper-annotations.schema.json"
        schema = json.loads(schema_path.read_text(encoding="utf-8"))

        self.assertEqual(schema["$schema"], "https://json-schema.org/draft/2020-12/schema")
        self.assertEqual(
            schema["required"],
            ["paper", "report", "concepts", "blocks", "annotations"],
        )

    def test_schema_requires_report_sections_concepts_and_annotation_fields(self) -> None:
        schema_path = Path(__file__).resolve().parents[1] / "schemas" / "paper-annotations.schema.json"
        schema = json.loads(schema_path.read_text(encoding="utf-8"))

        report_required = schema["properties"]["report"]["required"]
        concept_required = schema["properties"]["concepts"]["items"]["required"]
        annotation_required = schema["properties"]["annotations"]["items"]["required"]
        evidence_required = (
            schema["properties"]["annotations"]["items"]["properties"]["evidence"]["items"]["required"]
        )

        self.assertEqual(
            report_required,
            [
                "document_type",
                "primary_focus",
                "one_sentence_summary",
                "executive_summary",
                "problem_and_scope",
                "method_and_architecture",
                "training_and_data",
                "evaluation_and_evidence",
                "novelty_and_lineage",
                "limitations_and_risks",
                "practical_takeaways",
                "open_questions",
            ],
        )
        self.assertEqual(concept_required, ["concept_id", "name", "summary"])
        self.assertEqual(
            annotation_required,
            [
                "annotation_id",
                "block_id",
                "anchor_text",
                "label",
                "kind",
                "analysis",
                "concept_ids",
                "confidence",
                "evidence",
            ],
        )
        self.assertEqual(evidence_required, ["quote", "reason", "source_ref"])
        self.assertEqual(
            schema["properties"]["paper"]["properties"]["source_kind"]["enum"],
            ["arxiv_html", "arxiv_text"],
        )

    def test_prompt_mentions_annotation_contract_sections(self) -> None:
        prompt = load_paper_review_prompt()

        self.assertIn('"report"', prompt)
        self.assertIn('"executive_summary"', prompt)
        self.assertIn('"concepts"', prompt)
        self.assertIn('"blocks"', prompt)
        self.assertIn('"annotations"', prompt)
        self.assertIn('"anchor_text"', prompt)
        self.assertIn('"mechanistic_explanation"', prompt)
        self.assertIn('"source_ref": string | null', prompt)
        self.assertIn("do not emit separate `cs50`", prompt.lower())

    def test_schema_avoids_codex_unsupported_format_keywords(self) -> None:
        schema_path = Path(__file__).resolve().parents[1] / "schemas" / "paper-annotations.schema.json"
        schema_text = schema_path.read_text(encoding="utf-8")

        self.assertNotIn('"format": "uri"', schema_text)
