# Adding a new data source

> Quick reference. First time here? The [developer walkthrough](developer-walkthrough.md) covers this end-to-end, including machine setup and a local S3.

Every data source is one folder under `src/datasources/defs/`. Dagster autoloads the whole `defs/` tree ([`definitions.py`](../src/datasources/definitions.py) uses `load_from_defs_folder`), so a new folder shows up in the UI with **no registration step**.

## 1. Scaffold

```bash
dg scaffold defs datasources.components.DltLoadSourceCollection my_source
```

This uses our project-local component (defined in [`datasources/components`](../src/datasources/components/__init__.py) — it extends the stock `dagster_dlt.DltLoadCollectionComponent` with a scaffold that follows repo conventions) and creates:

```
src/datasources/defs/my_source/
├── defs.yaml    # component config — pre-filled with asset key + group
└── loader.py    # your Python: dlt source + pipeline (template to edit)
```

Naming: use a short `snake_case` name identifying the data source (`mdharura`, `health_facilities`) — it becomes the asset key prefix, the Dagster group, and (by convention) the pipeline name.

## 2. Write the dlt source and pipeline (`loader.py`)

`loader.py` must expose two module-level objects that `defs.yaml` references:

```python
import dlt
from dlt.sources.rest_api import rest_api_source

# 1. A dlt source: where the data comes from
source = rest_api_source(
    {
        "client": {
            "base_url": "https://api.example.org/v1/",
            "paginator": {"type": "json_link", "next_url_path": "next"},
        },
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "replace",
            "endpoint": {"data_selector": "results"},
        },
        # each resource = one Dagster asset = one table/prefix in the bucket
        "resources": [
            {"name": "records", "endpoint": {"path": "records/"}},
        ],
    },
    name="my_source",
)

# 2. A dlt pipeline: where the data goes
pipeline = dlt.pipeline(
    pipeline_name="my_source",
    destination="filesystem",          # MinIO — see docs/pipelines-and-destinations.md
    dataset_name="my_source_raw",      # top-level prefix in the bucket
)
```

The declarative `rest_api_source` covers most REST APIs. For page-level control, non-REST sources, or record transformations, see [resources.md](resources.md).

The filename and object names are referenced from `defs.yaml` by module path, so keep them in sync if you deviate from the scaffold.

## 3. Wire it into Dagster (`defs.yaml`)

The scaffold pre-fills this — adjust only if you rename things. The filename **must** be `defs.yaml`; that's how Dagster recognizes the folder as a component.

```yaml
type: datasources.components.DltLoadSourceCollection

attributes:
  loads:
    - source: .loader.source                   # .<module>.<object> relative to this folder
      pipeline: .loader.pipeline
      translation:
        key: "my_source/{{ resource.name }}"   # asset key in the Dagster UI
        group_name: my_source                  # groups the assets together
```

Without the `translation` block, asset keys default to `<dataset_name>/<resource>` (e.g. `my_source_raw/records`). Setting it keeps keys stable even if the dataset name changes.

## 4. Verify

```bash
dg check defs                              # everything imports and validates
dg list defs                               # your assets appear with kinds dlt + filesystem
dg launch --assets "my_source/records"     # full test run
```

To test without writing to the real bucket, point the destination at a local folder for one run:

```bash
DESTINATION__FILESYSTEM__BUCKET_URL="file:///tmp/dlt-test" \
  dg launch --assets "my_source/records"
```

## Checklist before opening a PR

- [ ] `dg check defs` passes
- [ ] A test materialization succeeded (local `file://` bucket is fine)
- [ ] Secrets (API tokens, credentials) are read from `dlt.secrets` / env vars — never hardcoded ([details](pipelines-and-destinations.md#configuration--secrets))
- [ ] `translation.key` and `group_name` are set in `defs.yaml`
- [ ] Large endpoints use incremental loading rather than `replace` ([details](resources.md#incremental-loading))
