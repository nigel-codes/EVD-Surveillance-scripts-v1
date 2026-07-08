# How Dagster ties it together

## Autoloading

[`definitions.py`](../src/datasources/definitions.py) calls `load_from_defs_folder`, which walks `src/datasources/defs/` recursively and loads everything it finds. Two kinds of content are picked up:

1. **Component folders** — any folder containing a file named exactly `defs.yaml` (the name is how Dagster recognizes a component; nothing else works). Our dlt sources use `type: datasources.components.DltLoadSourceCollection` (our project-local component — see [`datasources/components`](../src/datasources/components/__init__.py)).
2. **Plain Python modules** — any `.py` file defining Dagster objects (`@dg.asset`, `ScheduleDefinition`, sensors, asset checks) in folders **without** a `defs.yaml`.

Important: these are mutually exclusive per folder. Once a folder contains `defs.yaml`, the whole folder belongs to that component — sibling `.py` files there (other than the ones `defs.yaml` references, like `loader.py`) are **not** autoloaded. Schedules, downstream assets, and sensors must live in plain modules outside component folders.

Consequence: **never import or register anything centrally.** Drop a folder in `defs/`, and it's live.

## Asset keys, groups, and dependencies

Each dlt resource becomes an asset. The `translation` block in `defs.yaml` controls naming:

```yaml
loads:
  - source: .loader.source
    pipeline: .loader.pipeline
    translation:
      key: "mdharura/{{ resource.name }}"
      group_name: mdharura
```

Each asset produced by our component also self-documents, dbt-style. Its **description** is a plain-text summary line followed by the full `loader.py` source rendered as a Python code block — the summary shows in asset lists, the code on the asset page. The summary is `translation.description` from `defs.yaml` when set, otherwise the first line of the `loader.py` module docstring — so write a real docstring either way.

Convention: key prefix and group = the source folder name. Dagster also creates an upstream *external* asset per resource (e.g. `mdharura_signals`) representing the raw API — it has no materialization function; it's lineage metadata.

To build a downstream asset (e.g. a cleaned table computed from the raw bucket data), reference the dlt asset's key as a dependency from any Python file under `defs/`:

```python
import dagster as dg

@dg.asset(deps=[dg.AssetKey(["mdharura", "signals"])], group_name="mdharura")
def weekly_signal_summary():
    ...  # read s3://…/mdharura_raw/signals/, write derived output
```

## Schedules

Schedules live in [`defs/schedules.py`](../src/datasources/defs/schedules.py) — a plain module at the `defs/` root, **not** inside a source folder (component folders don't autoload sibling `.py` files; see [Autoloading](#autoloading)). Target a source's assets by group, name the schedule `sync_<source>_<resource>_<cadence>`, and give it a description (shown in the UI's Schedules tab):

```python
import dagster as dg

sync_mdharura_signals_daily = dg.ScheduleDefinition(
    name="sync_mdharura_signals_daily",
    target=dg.AssetSelection.groups("mdharura"),
    cron_schedule="0 6 * * *",
    description="Syncs signals from m-Dharura every day at 06:00 UTC",
)
```

Schedules only fire when the daemon is running (`dg dev` runs one locally; production deployments run `dagster-daemon`).

## Useful commands

| Command | What it does |
| --- | --- |
| `dg dev` | Web UI + daemon at http://localhost:3000 |
| `dg check defs` | Validate that all definitions load — run before every PR |
| `dg list defs` | List all assets/schedules/etc. in the terminal |
| `dg launch --assets "<key>"` | Materialize assets headless |
| `dg list components` | Available component types for scaffolding |
| `dg scaffold defs <component> <name>` | Scaffold a new source folder |

Run them as `uv run dg ...` if the venv isn't activated.

## Deployment

`docker compose up -d --build` runs the production layout: one image, two containers (webserver + daemon), SQLite state in a shared volume, runs executing inside the daemon container. Full guide — architecture, configuration, operations, troubleshooting, scaling path: [deployment.md](deployment.md).

## Gotchas

- **Asset not showing up?** Run `dg check defs` — import errors in any file under `defs/` are reported there. A folder with a misnamed `defs.yml`/`component.yaml` loads as a plain module and its YAML is silently ignored.
- **grpcio/protobuf pin:** `dagster` requires `protobuf<7`, so `grpcio` is pinned `<1.80` in [`pyproject.toml`](../pyproject.toml) (newer grpcio ships protobuf-7 generated code). Don't bump it until Dagster lifts the cap.
- **Dependencies:** add runtime deps with `uv add <pkg>`, dev-only tooling with `uv add --dev <pkg>`.
