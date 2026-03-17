# Prefect Notes

This repo uses `flow.serve()` for the initial local deployment phase.

Execution lanes:

- `mini-process`: default subprocess lane on the always-on host
- `mini-docker`: Docker-isolated jobs for heavier collectors and future Playwright work
- `mbp-process`: optional interactive lane for laptop reruns

Custom event names emitted by workers:

- `news.ingest.completed`
- `paper.review.completed`
- `browser.scrape.completed`
- `draft.generated`
- `report.published`

The initial bootstrap path is:

1. start PostgreSQL with `infra/compose/compose.yaml`
2. run `scripts/run_prefect_server.sh`
3. run `scripts/run_prefect_serve.sh`
4. start workers with `scripts/run_prefect_worker.sh`
