Analyze this arXiv paper as a technical reader companion.

Goal:
Produce a comprehensive, technically rigorous report that helps me understand:
- what this document is trying to do
- its main technical content, mechanism, or contribution
- what is actually novel versus standard practice
- how it fits into the development of prior models and LLM techniques
- what the evidence establishes
- what limitations and open questions remain

Important framing rule:
- Do not assume the document is a standard research paper with a single clearly defined “core problem”.
- First determine what kind of document this is, such as:
  - research paper
  - technical report
  - benchmark or evaluation report
  - system description
  - survey
  - position paper
  - safety report or system card
  - other
- Adapt the analysis accordingly.
- If the document is not organized around a single “core problem”, analyze its primary objective, scope, thesis, design, or evaluation target instead.
- In all cases, explain what the document is doing, how it does it, what is new or important, and where it sits in the lineage of earlier models and techniques.

Output contract:
- Return ONLY valid JSON
- Do not wrap in markdown
- Do not add commentary before or after the JSON
- The top-level object must match this schema exactly:

```typescript
{
  "paper": {
    "title": string,
    "authors": string[],
    "year": number,
    "venue_or_source": string,
    "arxiv_id": string | null,
    "document_type": string,
    "primary_focus": string
  },
  "report": [
    {
      "kind": string,
      "title": string,
      "summary": string,
      "details": string[],
      "evidence": string[],
      "paper_claims": string[],
      "inferences": string[]
    }
  ],
  "comparison_table": [
    {
      "model_or_paper": string,
      "core_idea": string,
      "training_setup": string,
      "key_innovation": string,
      "strengths": string[],
      "weaknesses": string[],
      "difference_from_target_paper": string
    }
  ],
  "glossary": [
    {
      "term": string,
      "definition": string
    }
  ],
  "open_questions": string[]
}
```

Analytical requirement:
Populate the "report" array with these exact sections, in this exact order, each as one object with the required fields above:

1. `"kind": "executive_summary"`
- 5 to 10 dense bullets in details
- State the main claim or purpose, why it matters, and the core result or takeaway
- If the document is not making a single central claim, summarize its main deliverables or conclusions instead

2. `"kind": "paper_in_one_sentence"`
- One dense technical sentence in summary
- details should unpack the sentence components

3. `"kind": "problem_formulation"`
- For standard research papers:
  - What problem is being solved
  - Why prior approaches were insufficient
  - What assumptions, constraints, and task framing matter
- For technical reports, benchmarks, surveys, system descriptions, or safety reports:
  - What the primary objective is
  - What scope is covered
  - What questions, capabilities, or design targets structure the document
  - What assumptions, constraints, and framing choices matter
- Do not force an artificial “problem statement” if the document is not structured that way

4. `"kind": "method_breakdown"`
- Explain the architecture, training procedure, inference procedure, objective, evaluation protocol, or system design, depending on what the document actually contains
- Define all major components and their interactions
- Explain important equations in plain English
- Explain important figures and tables mechanistically
- If the document is light on methods and heavy on evaluation, make that explicit

5. `"kind": "novel_contributions"`
- Separate:
  - genuinely new ideas
  - recombinations of known techniques
  - implementation, scaling, or data choices
  - reporting, benchmarking, or systems contributions
- If novelty is modest or mostly integrative, say so directly

6. `"kind": "lineage_and_historical_context"`
- Place the document in the development of earlier models and LLM techniques
- Identify which prior ideas it builds on, replaces, systematizes, evaluates, or rejects
- Explain the progression of ideas, not just citations
- For technical reports, also explain how the report reflects or consolidates an existing line of development

7. `"kind": "experimental_analysis"`
- Summarize datasets, baselines, metrics, ablations, evaluation setups, or empirical case studies, depending on the document type
- State what the evidence actually establishes
- Identify weak evidence, confounds, missing controls, or unclear comparisons
- If the document is primarily descriptive and not strongly experimental, say so explicitly

8. `"kind": "limitations_and_caveats"`
- Separate author-stated limitations from your own inference
- Call out likely unstated limitations when justified
- Include mismatches between claims and evidence when present

9. `"kind": "practical_interpretation"`
- Explain what this means for someone building or studying LLM systems
- Identify which ideas look reusable, scalable, fragile, benchmark-specific, policy-specific, or mostly historical
- For technical reports, explain what practitioners should actually take away from the document

10. `"kind": "reading_notes"`
- Include the minimum prerequisite concepts needed to read the document well
- Include likely points of confusion for a technically literate reader
- If the document assumes substantial prior context, surface that explicitly

Comparison table requirement:
- Fill comparison_table with the most relevant predecessor and contemporary papers, model families, benchmarks, or system reports
- Prefer direct ancestors, nearest competitors, and items explicitly relevant in related work or framing
- Use historically meaningful comparisons, not filler entries
- If the document is a technical report, compare both:
  - the underlying model or system lineage
  - the reporting style or evaluation framing when relevant

Evidence requirement:
- In each report section, use "evidence" for section numbers, equation numbers, figure numbers, table numbers, appendix references, or concrete textual anchors when available
- If the document omits key context, say so explicitly
- If a claim is weakly supported, say so explicitly
- If the document structure is unusual, say which sections supply the relevant evidence

Claim discipline:
- Use "paper_claims" only for claims attributable to the authors
- Use "inferences" only for your own analytical judgments
- Never merge the two

Lineage discipline:
- When discussing prior models and LLM techniques, explain causal or conceptual lineage:
  - what changed
  - why it changed
  - what bottleneck it addressed
  - what tradeoff it introduced
- Distinguish between:
  - architectural lineage
  - training-method lineage
  - evaluation lineage
  - deployment or systems lineage
  when relevant

Document classification discipline:
- Set "document_type" to the most appropriate category based on the content, not the title alone
- Set "primary_focus" to a concise description such as:
  - algorithmic method
  - scaling study
  - benchmark evaluation
  - capability report
  - safety characterization
  - system architecture
  - survey of techniques
  - position or agenda
  - other
- Use this classification to guide the analysis

Quality bar:
- Be precise, technical, and readable
- Avoid generic summary language
- Prefer mechanism-level explanation over high-level prose
- Do not hallucinate missing details
- When uncertain, state uncertainty explicitly in the relevant field

The only allowed values for "kind" are:

```json
[
  "executive_summary",
  "paper_in_one_sentence",
  "problem_formulation",
  "method_breakdown",
  "novel_contributions",
  "lineage_and_historical_context",
  "experimental_analysis",
  "limitations_and_caveats",
  "practical_interpretation",
  "reading_notes"
]
```

1. `"kind": "executive_summary"`
2. `"kind": "paper_in_one_sentence"`
3. `"kind": "problem_formulation"`
4. `"kind": "method_breakdown"`
5. `"kind": "novel_contributions"`
6. `"kind": "lineage_and_historical_context"`
7. `"kind": "experimental_analysis"`
8. `"kind": "limitations_and_caveats"`
9. `"kind": "practical_interpretation"`
10. `"kind": "reading_notes"`