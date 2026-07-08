# Developer walkthrough: zero to a running resource

This is the end-to-end tutorial: set up your machine, run the existing pipeline, then build a new data source from scratch. For quick reference instead of a tutorial, see [adding-a-source.md](adding-a-source.md).

## 1. Dev setup

Prerequisites:

- [uv](https://docs.astral.sh/uv/getting-started/installation/) (manages Python and the venv — you don't need to install Python separately)
- Docker (to run MinIO locally; skip it to test against local files)

```bash
git clone <this-repo> && cd evd-surveillance-scripts
uv sync
```

`uv sync` creates `.venv/` and installs all runtime and dev dependencies from `uv.lock`. Then either activate the venv or prefix commands with `uv run`:

```bash
source .venv/bin/activate     # then: dg --version
# or, without activating:
uv run dg --version
```

Managing dependencies later: `uv add <pkg>` for runtime deps, `uv add --dev <pkg>` for dev-only tooling. Both update `pyproject.toml` and `uv.lock` — commit both files.

## 2. Set up the destination (MinIO)

Data lands in [MinIO](https://min.io/), an S3-compatible object store — dlt talks to it through the standard S3 API, so bucket URLs use the `s3://` scheme. For local development, run your own instance:

```bash
docker run -d --name minio -p 9000:9000 -p 9001:9001 \
  quay.io/minio/minio server /data --console-address ":9001"
```

Open the console at http://localhost:9001 (default login `minioadmin` / `minioadmin`) and create a bucket, e.g. `evd`.

Configure dlt — `.dlt/config.toml` (committed):

```toml
[destination.filesystem]
bucket_url = "s3://evd/"
```

`.dlt/secrets.toml` (gitignored — copy the committed template and fill it in):

```bash
cp .dlt/secrets.example.toml .dlt/secrets.toml
```

```toml
[destination.filesystem.credentials]
aws_access_key_id = "minioadmin"
aws_secret_access_key = "minioadmin"
endpoint_url = "http://localhost:9000"   # the endpoint goes HERE, never in bucket_url
```

> Two mistakes to avoid: `bucket_url = "s3://localhost:9000/evd"` makes dlt treat `localhost:9000` as the bucket name; and access keys created on a previous MinIO container die with its volume — `InvalidAccessKeyId` means the key doesn't exist on *this* server.

For a shared/production MinIO, the config is identical — point `endpoint_url` at that server and use an access key created in its console (Access Keys → Create).

### Alternative: skip MinIO entirely

Override the bucket with a local path per run — handy for quick tests before anything is set up:

```bash
DESTINATION__FILESYSTEM__BUCKET_URL="file:///tmp/dlt-test" dg launch --assets "mdharura/signals"
```

## 3. Run Dagster

```bash
dg dev
```

Open http://localhost:3000 — you'll see the `mdharura/signals` asset. Click **Materialize** to run it, or do the same headless:

```bash
dg launch --assets "mdharura/signals"
```

Preview what landed (files are gzipped JSONL):

```bash
# local files
gzcat /tmp/dlt-test/mdharura_raw/signals/*.jsonl.gz | head -3

# MinIO
uv run python -c "
import gzip, s3fs
fs = s3fs.S3FileSystem(key='minioadmin', secret='minioadmin', endpoint_url='http://localhost:9000')
path = fs.find('evd/mdharura_raw/signals')[0]
with fs.open(path, 'rb') as f:
    for i, line in enumerate(gzip.open(f)):
        print(line.decode())
        if i >= 2: break
"
```

## 4. Build a new resource, step by step

We'll pretend to add a source called `example`. Substitute your real API.

### 4.1 Explore the API first

Before writing code, answer three questions with `curl`:

```bash
curl -s "https://api.example.org/v1/things?limit=2" | head -c 500
```

1. **Where are the records?** e.g. under `{"data": [...]}` → `data_selector: "data"`
2. **How does pagination work?** a `page` param with a total-pages field → `page_number` paginator; a next-page URL in the body → `json_link`; etc.
3. **Is there a date filter?** e.g. `?dateStart=...` → enables incremental loading

(For m-Dharura we found: records under `data`, `page`/`limit` params with a `pages` total, and a `dateStart` filter — you can see each answer reflected in [`mdharura/loader.py`](../src/datasources/defs/mdharura/loader.py).)

### 4.2 Scaffold

```bash
dg scaffold defs datasources.components.DltLoadSourceCollection example
```

That's our project-local component ([`datasources/components`](../src/datasources/components/__init__.py)); its scaffold creates `src/datasources/defs/example/` with a `loader.py` template and a `defs.yaml` already wired to it. Nothing else to register — Dagster autoloads the folder.

### 4.3 Write the source (`loader.py`)

Start minimal — source + pipeline, no transforms:

```python
import dlt
from dlt.sources.rest_api import rest_api_source

source = rest_api_source(
    {
        "client": {
            "base_url": "https://api.example.org/v1/",
            "paginator": {"type": "page_number", "base_page": 1, "total_path": "pages"},
        },
        "resources": [
            {
                "name": "things",
                "primary_key": "id",
                "write_disposition": "append",
                "endpoint": {
                    "path": "things",
                    "params": {"limit": 50},
                    "data_selector": "data",
                },
            },
        ],
    },
    name="example",
    max_table_nesting=0,   # keep records raw; drop this to explode nested data into child tables
)

pipeline = dlt.pipeline(
    pipeline_name="example",
    destination="filesystem",
    dataset_name="example_raw",     # keep different from pipeline_name
)
```

### 4.4 Check the Dagster wiring (`defs.yaml`)

The scaffold already generated this — verify it matches your object names:

```yaml
type: datasources.components.DltLoadSourceCollection

attributes:
  loads:
    - source: .loader.source
      pipeline: .loader.pipeline
      translation:
        key: "example/{{ resource.name }}"
        group_name: example
```

### 4.5 Validate and test-run

```bash
dg check defs                                    # must pass before anything else
DESTINATION__FILESYSTEM__BUCKET_URL="file:///tmp/dlt-test" \
  dg launch --assets "example/things"            # safe test run to local files
gzcat /tmp/dlt-test/example_raw/things/*.jsonl.gz | head -3
```

### 4.6 Add a mapping (optional)

To reshape records before they land, add a map function and attach it with `processing_steps`. This runs record-by-record, streaming, during extraction:

```python
def map_thing(thing: dict) -> dict:
    return {
        "id": thing.get("_id"),
        "name": thing.get("name"),
        "created_at": thing.get("createdAt"),
    }
```

```python
"resources": [
    {
        "name": "things",
        "primary_key": "id",              # post-map field name
        "processing_steps": [{"map": map_thing}],
        ...
```

### 4.7 Add incremental loading (optional, for growing endpoints)

```python
"endpoint": {
    "path": "things",
    "params": {"limit": 50},
    "data_selector": "data",
    "incremental": {
        "start_param": "dateStart",        # query param the API filters on
        "cursor_path": "created_at",       # ⚠ post-map field name, not the API's raw name
        "initial_value": "2026-07-06T00:00:00.000Z",
    },
},
```

dlt remembers the newest `created_at` between runs and passes it as `?dateStart=` — each run fetches only new records. **The cursor is read after your map runs**: if `cursor_path` references a field your map dropped or renamed, the run fails with `IncrementalCursorPathMissing`. See [resources.md](resources.md#incremental-loading) for details and the update-detection caveat.

### 4.8 Re-run and iterate

```bash
dg check defs && dg launch --assets "example/things"
```

Run it twice — the second run should load few or zero records if incremental is working. When it looks right, run once against the real bucket, then open a PR ([checklist](adding-a-source.md#checklist-before-opening-a-pr)).

## 5. Troubleshooting

| Symptom | Cause / fix |
| --- | --- |
| `command not found: dg` | Venv not activated (or stale shell). `source .venv/bin/activate`, or use `uv run dg ...` |
| `ModuleNotFoundError: datasources` | Run `uv sync`; check `src/datasources/__init__.py` exists |
| `IncrementalCursorPathMissing` | `cursor_path` names a field your map dropped/renamed — use the post-map name |
| `InvalidAccessKeyId` (MinIO) | The key doesn't exist on this MinIO instance — recreate it in the console, or use the root credentials |
| Asset missing from UI | `dg check defs` shows the import error; also check the file is named exactly `defs.yaml` |
| Pipeline behaves oddly after config changes | Reset local state: `rm -rf ~/.dlt/pipelines/<pipeline_name>` and re-run (it re-syncs from the destination) |
| Want a full re-backfill | Change `initial_value`, then reset pipeline state as above |

Also see the [gotchas in dagster.md](dagster.md#gotchas) (the grpcio/protobuf pin, `defs.yaml` naming).
