
import json

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

MANIFEST = ROOT / "dbt" / "target" / "manifest.json"


with MANIFEST.open(encoding="utf-8") as file:

    manifest = json.load(file)


models = [
    node
    for node in manifest.get("nodes", {}).values()
    if node.get("resource_type") == "model"
    and node.get("package_name") == "india_aqi_analytics"
]


print("\nDBT DOCUMENTATION QUALITY AUDIT")

missing_model_descriptions = []

missing_column_descriptions = []

for model in models:

    name = model["name"]

    description = (
        model.get("description") or ""
    ).strip()

    if not description:

        missing_model_descriptions.append(name)

    for column_name, column in (
        model.get("columns", {}).items()
    ):

        column_description = (
            column.get("description") or ""
        ).strip()

        if not column_description:

            missing_column_descriptions.append(
                f"{name}.{column_name}"
            )


print("\nTotal models:", len(models))

print(
    "Models missing descriptions:",
    len(missing_model_descriptions)
)

print(
    "Columns missing descriptions:",
    len(missing_column_descriptions)
)

if missing_model_descriptions:

    print("\nMODELS WITHOUT DESCRIPTIONS:")

    for name in missing_model_descriptions:
        print("-", name)

if missing_column_descriptions:

    print("\nCOLUMNS WITHOUT DESCRIPTIONS:")

    for column in missing_column_descriptions:
        print("-", column)

print("\nDOCUMENTATION AUDIT COMPLETED.")
