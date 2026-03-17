import unittest

from libs.retrieval.chunking import chunk_text


class ChunkingTests(unittest.TestCase):
    def test_chunk_text_splits_long_input(self) -> None:
        text = "\n\n".join(["alpha beta gamma delta" * 50, "second paragraph" * 20])
        chunks = chunk_text(text, target_chars=120, overlap_chars=20)
        self.assertGreaterEqual(len(chunks), 2)
        self.assertEqual(chunks[0]["ordinal"], 0)
        self.assertTrue(all(chunk["token_count"] > 0 for chunk in chunks))
