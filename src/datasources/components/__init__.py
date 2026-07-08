"""Project component types, registered via [tool.dg] registry_modules in pyproject.toml."""

import ast
import textwrap
from pathlib import Path

import dagster as dg
from dagster.components.core.context import ComponentLoadContext
from dagster_dlt import DltLoadCollectionComponent as _DltLoadCollectionComponent
from dagster.components.scaffold.scaffold import ScaffoldRequest, scaffold_with

LOADER_TEMPLATE = '''\
"""{name}: load data from <API> into MinIO.

See docs/developer-walkthrough.md for the step-by-step guide.
"""

import dlt
from dlt.sources.rest_api import rest_api_source

source = rest_api_source(
    {{
        "client": {{
            "base_url": "https://api.example.org/v1/",
            "paginator": {{
                "type": "page_number",
                "base_page": 1,
                "total_path": "pages",
            }},
        }},
        "resources": [
            {{
                "name": "records",
                "primary_key": "id",
                "write_disposition": "append",
                "endpoint": {{
                    "path": "records",
                    "params": {{"limit": 50}},
                    "data_selector": "data",
                }},
            }},
        ],
    }},
    name="{name}",
    max_table_nesting=0,
)

pipeline = dlt.pipeline(
    pipeline_name="{name}",
    destination="filesystem",
    dataset_name="{name}_raw",
)
'''


class DltLoaderScaffolder(dg.Scaffolder):
    """Scaffolds a data-source folder following this repo's conventions:

    - the Python module is named loader.py (upstream default is loads.py)
    - it exposes module-level `source` and `pipeline` objects
    - defs.yaml is pre-filled with the asset-key/group translation derived
      from the folder name
    """

    def scaffold(self, request: ScaffoldRequest) -> None:
        target = Path(request.target_path)
        name = target.name
        target.mkdir(parents=True, exist_ok=True)
        (target / "loader.py").write_text(
            textwrap.dedent(LOADER_TEMPLATE).format(name=name), encoding="utf-8"
        )
        dg.scaffold_component(
            request=request,
            yaml_attributes={
                "loads": [
                    {
                        "source": ".loader.source",
                        "pipeline": ".loader.pipeline",
                        "translation": {
                            "key": name + "/{{ resource.name }}",
                            "group_name": name,
                        },
                    }
                ]
            },
        )


@scaffold_with(DltLoaderScaffolder)
class DltLoadSourceCollection(_DltLoadCollectionComponent):
    """dlt load collection whose scaffold follows this repo's conventions (loader.py).

    Like the dbt integration surfaces each model's SQL, every asset's
    description ends with the full loader.py source, rendered as a Python code
    block in the UI. The leading summary line is the translation.description
    from defs.yaml when set, otherwise the loader.py docstring's first line.
    """

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        defs = super().build_defs(context)
        loader_path = Path(context.path) / "loader.py"
        if not loader_path.exists():
            return defs

        code = loader_path.read_text(encoding="utf-8")
        docstring = ast.get_docstring(ast.parse(code)) or ""
        fallback = docstring.splitlines()[0] if docstring else f"Defined in {loader_path.name}."

        def _attach(spec: dg.AssetSpec) -> dg.AssetSpec:
            # summary first so list views show readable text; the full source
            # renders as a code block on the asset page
            summary = spec.description or fallback
            return spec.replace_attributes(
                description=f"{summary}\n\n```python\n{code}\n```"
            )

        return defs.map_asset_specs(func=_attach)


# keep the imported base class out of the component registry
del _DltLoadCollectionComponent
