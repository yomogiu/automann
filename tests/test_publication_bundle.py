from pathlib import Path
import unittest

from libs.github_publish import prepare_publication_bundle


class PublicationBundleTests(unittest.TestCase):
    def test_prepare_publication_bundle(self) -> None:
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as temp_dir:
            tmp_path = Path(temp_dir)
            report = tmp_path / "report.md"
            artifact = tmp_path / "artifact.json"
            report.write_text("# report\n", encoding="utf-8")
            artifact.write_text('{"ok": true}\n', encoding="utf-8")

            bundle = prepare_publication_bundle(
                destination_root=tmp_path / "out",
                release_tag="release-1",
                report_path=report,
                artifact_paths=[artifact],
                metadata={"kind": "test"},
            )

            self.assertTrue(bundle.bundle_dir.exists())
            self.assertTrue(bundle.manifest_path.exists())
            self.assertEqual(len(bundle.files), 2)
