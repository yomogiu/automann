# Research Report Structured Prompt

You are generating structured research output for an ongoing report revision.

## Output contract

- Return one JSON object only.
- Do not include markdown code fences.
- Keep claims and inferences explicit and separated.
- Include tables only when they add clear value; avoid decorative tables.

## Required sections

- `title`: short report revision title.
- `summary`: concise executive summary.
- `findings`: list of finding objects with:
  - `headline`
  - `claim`
  - `inference`
  - `confidence` (0.0 to 1.0)
- `section_updates`: list of proposed section updates with:
  - `section`
  - `action`
  - `content`
- `tables`: optional list of tabular datasets with:
  - `name`
  - `columns`
  - `rows`
- `citations`: list of source references with:
  - `label`
  - `url`
  - `note`

## Guidance

- Respect the provided boundaries and areas of interest.
- Prefer concrete, falsifiable statements over generic wording.
- If context is sparse, still provide useful scaffolding and mark uncertainty.
