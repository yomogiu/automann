from __future__ import annotations

import json
from pathlib import Path
import unittest


class PaperAnnotationsSchemaTests(unittest.TestCase):
    def test_schema_has_required_top_level_sections(self) -> None:
        schema_path = Path(__file__).resolve().parents[1] / "schemas" / "paper-annotations.schema.json"
        schema = json.loads(schema_path.read_text(encoding="utf-8"))

        self.assertEqual(schema["$schema"], "https://json-schema.org/draft/2020-12/schema")
        self.assertEqual(
            schema["required"],
            ["paper", "concepts", "blocks", "annotations"],
        )

    def test_schema_requires_three_learning_lenses_and_annotation_fields(self) -> None:
        schema_path = Path(__file__).resolve().parents[1] / "schemas" / "paper-annotations.schema.json"
        schema = json.loads(schema_path.read_text(encoding="utf-8"))

        concept_required = schema["properties"]["concepts"]["items"]["required"]
        perspective_required = (
            schema["properties"]["concepts"]["items"]["properties"]["perspectives"]["required"]
        )
        annotation_required = schema["properties"]["annotations"]["items"]["required"]

        self.assertEqual(concept_required, ["concept_id", "name", "summary", "perspectives"])
        self.assertEqual(
            perspective_required,
            ["cs50_student", "senior_engineer", "staff_ml_engineer"],
        )
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
        self.assertEqual(
            schema["properties"]["paper"]["properties"]["source_kind"]["enum"],
            ["arxiv_html", "arxiv_text"],
        )
