# api_async_scrape
Collection of async web API scraping utilities.

## Notebook-first workflow

Use `start.ipynb` as the new primary entrypoint (local Jupyter or Google Colab).

- In Colab: the notebook detects Colab, clones this repository, and installs dependencies.
- In local Jupyter: it installs minimal local dependencies and uses your current repo.

## Environment-driven configuration

The generic async API client reads context from `.env` (template: `env_sample`):

- base URL and endpoints
- auth header style
- response keys (`rows`, `totalPages`, etc.)
- project and payload field mappings (for example project ID column names)

This lets one notebook/client work across different APIs without hardcoding schema details in code.

## Reusable client

Generic async client/runtime helpers are in `utils/downloader_api.py`.
