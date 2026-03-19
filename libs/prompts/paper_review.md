Analyze this arXiv paper as a technical reader companion and produce both:
- a full-paper analytical report
- annotation-ready local span notes for an annotated HTML reading view

Goal:
- Help a serious technical reader understand what this document is doing, how it works, what is genuinely novel, what evidence supports the claims, what limitations matter, and what the practical implications are.
- Make the result useful to a serious learner, not just an expert skimming for headlines.
- The output must feel like a complete paper analysis first, and an annotation layer second.

Important intent:
- Do not turn every concept or annotated sentence into a 3-level pedagogical ladder.
- Use audience calibration implicitly to keep explanations clear and technically honest, but do not emit separate `cs50`, `senior engineer`, and `staff ml engineer` breakdowns for each concept or sentence.
- Concepts should be concise anchors for cross-linking, not mini-essays.

Framing rules:
- Do not assume the document is a standard research paper with one neatly scoped problem statement.
- First infer what kind of document this is from the provided text: research paper, technical report, benchmark report, system description, survey, safety report, or another document type.
- Keep paper claims and your analytical inferences separate inside the prose when relevant.
- Use only the supplied blocks. Do not invent sections, results, citations, tables, or evidence that are not grounded in the provided text.

Coverage rules:
- Cover the whole document represented by the supplied blocks, not just a few representative spans.
- Build the top-level report as a coherent paper analysis that addresses:
  - what the paper is and what it is trying to do
  - the scope and problem framing
  - the method / architecture / system design
  - the training and data story when present
  - the evaluation and evidentiary strength
  - novelty versus recombination and lineage
  - limitations, risks, and uncertainty
  - practical takeaways and open questions
- When the source includes major sections such as abstract, introduction, method, training, evaluation, limitation, or conclusion, make sure the report reflects all of them when evidence is available.
- For annotations, cover the most important spans across the document, with at least one meaningful annotation from each major phase or section family that appears in the provided blocks.

Output contract:
- Return ONLY valid JSON.
- Do not wrap the JSON in markdown fences.
- Do not add commentary before or after the JSON.
- The top-level object must match this schema exactly:

```typescript
{
  "paper": {
    "paper_id": string,
    "title": string,
    "source_url": string,
    "source_kind": "arxiv_html" | "arxiv_text"
  },
  "report": {
    "document_type": string,
    "primary_focus": string,
    "one_sentence_summary": string,
    "executive_summary": string[],
    "problem_and_scope": string[],
    "method_and_architecture": string[],
    "training_and_data": string[],
    "evaluation_and_evidence": string[],
    "novelty_and_lineage": string[],
    "limitations_and_risks": string[],
    "practical_takeaways": string[],
    "open_questions": string[]
  },
  "concepts": [
    {
      "concept_id": string,
      "name": string,
      "summary": string
    }
  ],
  "blocks": [
    {
      "block_id": string,
      "section_label": string,
      "order": number,
      "text": string
    }
  ],
  "annotations": [
    {
      "annotation_id": string,
      "block_id": string,
      "anchor_text": string,
      "label": string,
      "kind": "extracted_claim" | "model_inference" | "mechanistic_explanation" | "method_detail" | "limitation" | "implication",
      "analysis": string,
      "concept_ids": string[],
      "confidence": number,
      "evidence": [
        {
          "quote": string,
          "reason": string,
          "source_ref": string | null
        }
      ]
    }
  ]
}
```

Required behavior:
- `paper`:
  - Copy the paper metadata from the input payload.
- `report`:
  - This is the main paper analysis, not an afterthought.
  - `document_type` should classify the document based on content, not title alone.
  - `primary_focus` should be concise, such as `system architecture`, `benchmark evaluation`, `reasoning extension`, `scaling study`, or similar.
  - `one_sentence_summary` should be a dense but readable technical sentence.
  - `executive_summary` should contain 6 to 10 high-signal bullets.
  - Every other report section should contain 3 to 8 dense bullets when the evidence exists.
  - If a section is weakly supported by the supplied blocks, say so directly instead of fabricating detail.
- `blocks`:
  - Reproduce the provided blocks exactly and in the same order.
  - Do not rewrite, summarize, merge, split, or reorder block text.
  - Do not omit provided blocks.
- `concepts`:
  - Extract 6 to 12 core concepts that are genuinely useful for understanding the paper.
  - Prefer concepts tied to mechanism, architecture, training recipe, evaluation framing, lineage, failure modes, or limitations.
  - Each concept summary should be concise and technically specific.
  - Do not create audience-specific sub-breakdowns for each concept.
- `annotations`:
  - Create 15 to 30 high-signal annotations across the supplied blocks when the material supports that density.
  - Prefer 1 to 3 annotations per important block, not every sentence.
  - `anchor_text` must appear verbatim as a contiguous substring inside the referenced block text.
  - `label` should be short and useful in a reading UI.
  - `analysis` should explain why the span matters, separating extracted paper claims from your inference where relevant.
  - `concept_ids` must reference existing concept ids.
  - `evidence` should point to nearby quoted text and explain why it supports the annotation.
  - Always include `source_ref`; use a concrete local anchor when available (for example `Abstract`, `Introduction`, `Figure 2`, `Table 3`, `block b-0007`) and use `null` when no tighter anchor is justified from the supplied text.
  - Use `extracted_claim` for claims attributable to the authors.
  - Use `model_inference` only for your own analytical judgment.
  - Use `mechanistic_explanation` for architecture or training logic.
  - Use `method_detail` for implementation or protocol details.
  - Use `limitation` for caveats, missing evidence, or explicit constraints.
  - Use `implication` for practical or strategic takeaways.

Quality bar:
- Be precise, technical, and readable.
- Prefer mechanism-level explanation over generic summary language.
- Distinguish novelty from recombination when the paper supports that distinction.
- Surface weak evidence, unclear comparisons, or likely unstated limitations when justified by the supplied text.
- Do not hallucinate missing details.
- If something is unclear from the supplied blocks, say so in the relevant report section or annotation instead of inventing it.
