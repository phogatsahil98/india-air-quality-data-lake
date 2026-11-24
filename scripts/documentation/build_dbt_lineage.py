
import json

from collections import defaultdict
from pathlib import Path


# ==================================================
# 1. PROJECT CONFIGURATION
# ==================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DBT_DIR = PROJECT_ROOT / "dbt"

MANIFEST_PATH = DBT_DIR / "target" / "manifest.json"

DOCS_DIR = DBT_DIR / "docs"

DOCS_DIR.mkdir(
    parents=True,
    exist_ok=True
)


# ==================================================
# 2. LOAD MANIFEST
# ==================================================

def load_manifest():

    if not MANIFEST_PATH.exists():

        raise FileNotFoundError(
            "manifest.json not found. Run dbt parse first."
        )

    with MANIFEST_PATH.open(
        encoding="utf-8"
    ) as file:

        manifest = json.load(file)

    print("Manifest loaded successfully.")

    return manifest


# ==================================================
# 3. EXTRACT PROJECT MODELS AND SOURCES
# ==================================================

def extract_resources(manifest):

    project_name = manifest["metadata"]["project_name"]

    models = {}

    for unique_id, node in manifest.get("nodes", {}).items():

        if (
            node.get("resource_type") == "model"
            and node.get("package_name") == project_name
        ):

            models[unique_id] = node

    sources = manifest.get("sources", {})

    print("Models discovered:", len(models))

    print(
        "Sources discovered:",
        sum(
            source.get("package_name") == project_name
            for source in sources.values()
        )
    )

    return models, sources, project_name


# ==================================================
# 4. DETERMINE MODEL LAYER
# ==================================================

def get_layer(node):

    path = node.get("original_file_path", "").replace("\\", "/")

    if "/staging/" in path:
        return "Staging"

    if "/intermediate/" in path:
        return "Intermediate"

    if "/marts/dimensions/" in path:
        return "Gold - Dimensions"

    if "/marts/facts/" in path:
        return "Gold - Facts"

    if "/marts/aggregates/" in path:
        return "Gold - Aggregates"

    return "Other"


# ==================================================
# 5. CREATE MERMAID NODE IDENTIFIERS
# ==================================================

def build_identifier_map(resources, prefix):

    return {
        unique_id: f"{prefix}{index}"
        for index, unique_id in enumerate(
            sorted(resources)
        )
    }


# ==================================================
# 6. BUILD LINEAGE DOCUMENT
# ==================================================

def generate_lineage(models, sources):

    model_ids = build_identifier_map(
        models,
        "M"
    )

    project_sources = {
        uid: source
        for uid, source in sources.items()
        if source.get("package_name") == "india_aqi_analytics"
    }

    source_ids = build_identifier_map(
        project_sources,
        "S"
    )

    lines = [
        "# India AQI — dbt Model Lineage",
        "",
        "Generated automatically from target/manifest.json.",
        "",
        "```mermaid",
        "flowchart TD",
        "",
        "    subgraph SRC[\"Source Tables\"]",
    ]

    # Source nodes

    for unique_id, source in sorted(project_sources.items()):

        node_id = source_ids[unique_id]

        label = (
            source.get("source_name", "")
            + "."
            + source.get("name", "")
        )

        lines.append(
            f'        {node_id}["{label}"]'
        )

    lines.append("    end")
    lines.append("")

    # Model groups

    layers = defaultdict(list)

    for unique_id, model in models.items():

        layers[get_layer(model)].append(
            (unique_id, model)
        )

    ordered_layers = [
        "Staging",
        "Intermediate",
        "Gold - Dimensions",
        "Gold - Facts",
        "Gold - Aggregates",
        "Other"
    ]

    for layer in ordered_layers:

        layer_models = sorted(
            layers.get(layer, []),
            key=lambda item: item[1].get("name", "")
        )

        if not layer_models:
            continue

        group_id = (
            layer.upper()
            .replace(" ", "_")
            .replace("-", "")
        )

        lines.append(
            f'    subgraph {group_id}["{layer}"]'
        )

        for unique_id, model in layer_models:

            node_id = model_ids[unique_id]

            name = model.get("name", "")

            materialization = (
                model.get("config", {}).get(
                    "materialized",
                    "unknown"
                )
            )

            label = f"{name}<br/>{materialization}"

            lines.append(
                f'        {node_id}["{label}"]'
            )

        lines.append("    end")
        lines.append("")

    # Dependency edges

    lines.append("    %% Model dependencies")

    for unique_id, model in sorted(models.items()):

        target_id = model_ids[unique_id]

        dependencies = (
            model.get("depends_on", {}).get("nodes", [])
        )

        for dependency in dependencies:

            if dependency in model_ids:

                source_id = model_ids[dependency]

                lines.append(
                    f"    {source_id} --> {target_id}"
                )

            elif dependency in source_ids:

                source_id = source_ids[dependency]

                lines.append(
                    f"    {source_id} --> {target_id}"
                )

    lines.append("")
    lines.append("```")

    output_path = DOCS_DIR / "lineage.md"

    output_path.write_text(
        "\n".join(lines),
        encoding="utf-8"
    )

    print("\nLineage generated:", output_path)


# ==================================================
# 7. GENERATE MODEL INVENTORY
# ==================================================

def generate_inventory(models):

    layers = defaultdict(list)

    for unique_id, model in models.items():

        layers[get_layer(model)].append(
            (unique_id, model)
        )

    lines = [
        "# India AQI — dbt Model Inventory",
        "",
        "Generated from the dbt manifest.",
        "",
        f"Total project models: {len(models)}",
        ""
    ]

    for layer in [
        "Staging",
        "Intermediate",
        "Gold - Dimensions",
        "Gold - Facts",
        "Gold - Aggregates",
        "Other"
    ]:

        layer_models = layers.get(layer, [])

        if not layer_models:
            continue

        lines.extend([
            f"## {layer}",
            ""
        ])

        for unique_id, model in sorted(
            layer_models,
            key=lambda item: item[1].get("name", "")
        ):

            config = model.get("config", {})

            dependencies = (
                model.get("depends_on", {}).get("nodes", [])
            )

            description = (
                model.get("description")
                or "Description not provided."
            )

            lines.extend([
                f"### {model.get('name')}",
                "",
                f"- Materialization: `{config.get('materialized', 'unknown')}`",
                f"- File: `{model.get('original_file_path')}`",
                f"- Database: `{model.get('database')}`",
                f"- Schema: `{model.get('schema')}`",
                f"- Upstream resource count: {len(dependencies)}",
                f"- Description: {description}",
                ""
            ])

    output_path = DOCS_DIR / "model_inventory.md"

    output_path.write_text(
        "\n".join(lines),
        encoding="utf-8"
    )

    print("Model inventory generated:", output_path)


# ==================================================
# 8. EXECUTION
# ==================================================

if __name__ == "__main__":

    manifest = load_manifest()

    models, sources, project_name = (
        extract_resources(manifest)
    )

    if not models:
        raise RuntimeError(
            "No project models were found."
        )

    generate_lineage(
        models,
        sources
    )

    generate_inventory(models)

    print("\n" + "=" * 55)
    print("DBT DOCUMENTATION BUILD COMPLETED")
    print("=" * 55)

    print("Project:", project_name)

    print("Models:", len(models))

    print("Files generated: 2")
