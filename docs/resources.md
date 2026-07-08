# Defining resources

A **resource** is one stream of records from a source — one API endpoint, one file feed, one scrape target. Each resource becomes one Dagster asset and one table (folder of files) in the destination. This page covers the three ways to define them, from most to least declarative, plus transformations and incremental loading.

## Option 1: declarative REST (`rest_api_source`)

Best default for JSON REST APIs. You describe the API; dlt handles requests, pagination, retries, and streaming. [`mdharura/loader.py`](../src/datasources/defs/mdharura/loader.py) is a working example — it pulls EBS signals from the m-Dharura Data Export API:

```python
from dlt.sources.rest_api import rest_api_source

source = rest_api_source(
    {
        "client": {
            "base_url": "https://api.m-dharura.health.go.ke/v1/",
            # for authenticated APIs — token comes from .dlt/secrets.toml:
            # "auth": {"type": "bearer",
            #          "token": dlt.secrets["datasources.mdharura.api_token"]},
            "paginator": {
                "type": "page_number",   # ?page=1,2,3... until `pages` is reached
                "base_page": 1,
                "total_path": "pages",   # response field holding the page count
            },
        },
        "resources": [
            {
                "name": "tasks",
                "primary_key": "_id",
                "write_disposition": "append",
                "endpoint": {
                    "path": "export/tasks",
                    "params": {"limit": 500, "state": "live"},
                    "data_selector": "data",   # records live under {"data": [...]}
                },
            },
        ],
    },
    name="mdharura",
)
```

Common paginator types: `page_number` (as above), `json_link` (next-page URL in the response body), `header_link` (RFC 5988 `Link` header), `offset`, `cursor`. See the [rest_api docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api/) for the full matrix.

## Option 2: hand-written resource with `RESTClient`

Drop to this when you need **page-level control**: reading the response envelope (counts, metadata), transforming whole pages, cross-page state, or early stopping. You keep dlt's paginators and retry handling.

```python
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

client = RESTClient(
    base_url="https://api.m-dharura.health.go.ke/v1/",
    paginator=PageNumberPaginator(base_page=1, total_path="pages"),
)

@dlt.source(name="mdharura")
def mdharura_source():

    @dlt.resource(name="tasks", primary_key="_id", write_disposition="append")
    def tasks():
        for page in client.paginate("export/tasks", params={"limit": 500}):
            envelope = page.response.json()      # full page object: total, pages, data
            yield [
                {**record, "total_at_fetch": envelope["total"]}
                for record in envelope["data"]
                if record["state"] == "live"
            ]

    return tasks
```

`client.paginate()` yields one page at a time; `page.response` is the raw `requests.Response`. Yield whatever you want records to be — a list per page, one record at a time, or a summary row.

## Option 3: plain generator

For non-HTTP sources (files, databases, scrapes), any generator works:

```python
@dlt.resource(name="lab_results", primary_key="sample_id", write_disposition="merge")
def lab_results():
    for path in bucket.list("incoming/"):
        yield from parse_result_file(path)
```

## Transforming records before the destination

Transforms attach to resources and run **record-by-record, streaming, during extraction** — nothing is written until they've run.

In declarative configs, use `processing_steps` per resource:

```python
"resources": [
    {
        "name": "tasks",
        "endpoint": {"path": "export/tasks", "data_selector": "data"},
        "processing_steps": [
            {"filter": lambda r: r["status"] == "completed"},
            {"map": redact_phone_numbers},   # def redact_phone_numbers(record) -> record
        ],
    },
],
```

On any resource object, use `add_map` / `add_filter` / `add_yield_map` (one-to-many):

```python
for resource in source.resources.values():
    resource.add_map(redact_pii)
```

For enrichment that needs another API call per record, use a transformer — it becomes its own asset/table:

```python
@dlt.transformer(data_from=tasks, primary_key="_id")
def task_units(task):
    yield from fetch_unit_details(task["units"])
```

**Boundary:** these hooks are for row/page-level reshaping (rename, filter, coerce, redact). Joins, aggregations, and cross-dataset dedup belong in a downstream Dagster asset that reads from the bucket — keep the raw layer replayable.

**Gotcha — maps run before the incremental cursor is read.** If a map renames or drops the cursor field, the run fails with `IncrementalCursorPathMissing`. Point `cursor_path` at the field name **your map emits**, not the API's raw name. In the mdharura source, `map_task` emits `created_at` (from the API's `createdAt`), so the incremental config uses `cursor_path: "created_at"`.

## Incremental loading

Full reloads (`write_disposition="replace"`) are fine for small reference data but wasteful for large or growing endpoints — m-Dharura has 225k+ tasks. The `tasks` resource loads incrementally: dlt remembers the newest `created_at` seen and passes it to the API as `dateStart` on the next run, so each run fetches only new signals:

```python
"endpoint": {
    "path": "export/tasks",
    "params": {"limit": 50, "state": "live"},
    "data_selector": "data",
    "incremental": {
        "start_param": "dateStart",       # query param the API filters on
        "cursor_path": "created_at",      # field dlt tracks — post-map name!
        "initial_value": "2026-07-06T00:00:00.000Z",   # backfill start — set
                                          # earlier to load full history
    },
},
"write_disposition": "append",
```

The cursor is stored in pipeline state between runs. To re-backfill from scratch, change `initial_value` and reset the pipeline state (see [pipelines-and-destinations.md](pipelines-and-destinations.md#pipeline-state-and-troubleshooting)). Full docs: [incremental loading](https://dlthub.com/docs/general-usage/incremental-loading).

Caveat: a `createdAt` cursor only picks up **new** records. m-Dharura tasks are updated after creation (verification/response forms get filled in), so records loaded early may go stale until a periodic full refresh or an `updatedAt`-based strategy is added.

## Schema control

- `max_table_nesting=0` on the source stops dlt from exploding nested objects into child tables — the mdharura source uses this so each task stays one raw record with its EBS forms inline as JSON.
- `columns={...}` on a resource pins types when inference guesses wrong.
- Without a nesting limit, nested lists become child tables named `<resource>__<field>` — you'd see them as extra folders in the bucket; that's expected.
