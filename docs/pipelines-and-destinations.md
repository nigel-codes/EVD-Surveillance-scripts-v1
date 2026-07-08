# Pipelines and destinations

A dlt **pipeline** connects a source to a destination and tracks state (schemas, incremental cursors, load history) between runs. Each source folder defines one:

```python
pipeline = dlt.pipeline(
    pipeline_name="my_source",       # state is tracked under this name — keep it unique
    destination="filesystem",        # MinIO (or any fsspec URL)
    dataset_name="my_source_raw",    # top-level prefix in the bucket
)
```

Keep `pipeline_name` and `dataset_name` **different from each other** and unique across the repo (convention: `<source>` and `<source>_raw`).

## The object-storage destination (MinIO)

We store data in [MinIO](https://min.io/), an S3-compatible object store, via dlt's [`filesystem` destination](https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem) — it speaks the S3 API, so `bucket_url` uses the `s3://` scheme and the MinIO endpoint goes in the credentials. Each resource is written as files under `<bucket_url>/<dataset_name>/<table>/`:

```
s3://evd/
└── mdharura_raw/
    ├── tasks/1783428877.942762.232b0f204d.jsonl.gz
    ├── _dlt_loads/...          # load bookkeeping — leave in place
    └── _dlt_pipeline_state/... # pipeline state — leave in place
```

Default file format is gzipped JSONL. For Parquet (better for Athena/analytics):

```python
# in the pipeline's run config — via defs.yaml this is set on the pipeline:
dlt.pipeline(..., loader_file_format="parquet")   # requires: uv add pyarrow
```

## Configuration & secrets

dlt resolves configuration from, in order of precedence: **env vars → `.dlt/secrets.toml` → `.dlt/config.toml`**.

| What | Where | Committed? |
| --- | --- | --- |
| Bucket URL | [`.dlt/config.toml`](../.dlt/config.toml) | yes |
| MinIO credentials + endpoint | `.dlt/secrets.toml` | **no** (gitignored) |
| API tokens | `.dlt/secrets.toml` under `[datasources.<name>]` | **no** |

`.dlt/config.toml` — bucket name only; the MinIO endpoint **never** goes in the bucket URL (`s3://localhost:9000/evd` would make dlt treat `localhost:9000` as the bucket name):

```toml
[destination.filesystem]
bucket_url = "s3://evd/"
```

`.dlt/secrets.toml` (never committed — start from the committed template: `cp .dlt/secrets.example.toml .dlt/secrets.toml`). The `aws_*` field names are the S3-protocol convention — they hold your MinIO keys:

```toml
[destination.filesystem.credentials]
aws_access_key_id = "minioadmin"          # local default; use a real access key elsewhere
aws_secret_access_key = "minioadmin"
endpoint_url = "http://localhost:9000"    # your MinIO server

[datasources.mdharura]
api_token = "..."
```

In deployment, use env vars — the TOML path maps to `__`-separated uppercase:

```bash
DESTINATION__FILESYSTEM__BUCKET_URL=s3://evd/
DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID=...
DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY=...
DESTINATION__FILESYSTEM__CREDENTIALS__ENDPOINT_URL=https://minio.example.org
DATASOURCES__MDHARURA__API_TOKEN=...
```

(The same config also works against any other S3-compatible store, including AWS S3 — just omit `endpoint_url` there.)

Reading a secret in code: `dlt.secrets["datasources.mdharura.api_token"]`.

## Testing without MinIO

The `filesystem` destination treats local paths identically to object storage — only the URL scheme differs. Override the bucket for one run:

```bash
DESTINATION__FILESYSTEM__BUCKET_URL="file:///tmp/dlt-test" \
  dg launch --assets "mdharura/signals"

find /tmp/dlt-test -name "*.jsonl.gz" | head
gzcat /tmp/dlt-test/mdharura_raw/signals/*.jsonl.gz | head
```

## Pipeline state and troubleshooting

- Local working state lives in `~/.dlt/pipelines/<pipeline_name>/`; authoritative state is also synced to the destination. If a pipeline gets into a confused state during development (e.g. after changing its destination), `rm -rf ~/.dlt/pipelines/<pipeline_name>` and re-run — it re-syncs from the destination.
- Changing `dataset_name` or `pipeline_name` orphans previous state/data; treat renames as migrations.
- Full traces from a failed run: `dlt pipeline <pipeline_name> trace`.
