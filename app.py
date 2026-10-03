"""
app.py

A lightweight Flask web application that provides a drag-and-drop UI for
designing Spark DataFrame pipelines. Users can:

    - Drag Ingestion, Transformation, and Sink nodes onto a canvas.
    - Configure each node's parameters in a sidebar form.
    - Save/load pipeline specifications to disk.
    - Export the current pipeline as YAML.
    - Run the pipeline using pipeline_builder.PipelineBuilder and view results.

Run with:
    python app.py

Then open http://127.0.0.1:5000 in a browser.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import yaml
from flask import Flask, jsonify, request, send_from_directory
from pyspark.sql import SparkSession

from pipeline_builder import PipelineBuilder

# ---------------------------------------------------------------------------
# App configuration
# ---------------------------------------------------------------------------
APP_ROOT = Path(__file__).resolve().parent
SPECS_DIR = APP_ROOT / "saved_specs"
SPECS_DIR.mkdir(exist_ok=True)

app = Flask(__name__, static_folder="static", template_folder="templates")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Lazily initialized SparkSession (created on first pipeline run).
_spark_session: SparkSession | None = None


def _get_spark_session() -> SparkSession:
    """Return a singleton SparkSession, creating it if necessary."""
    global _spark_session
    if _spark_session is None or _spark_session._jsc is None:
        _spark_session = SparkSession.builder.appName("PipelineBuilderUI").getOrCreate()
    return _spark_session


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _sanitize_filename(name: str) -> str:
    """Convert a pipeline name into a filesystem-safe filename."""
    safe = "".join(c for c in name if c.isalnum() or c in (" ", "-", "_")).rstrip()
    return safe.replace(" ", "_") or "pipeline"


def _build_pipeline_spec(data: Dict[str, Any]) -> Dict[str, Any]:
    """Convert the frontend JSON payload into a pipeline_builder YAML spec."""
    version = data.get("version", "1.0")
    pipelines = data.get("pipelines", [])
    return {"version": version, "pipelines": pipelines}


# ---------------------------------------------------------------------------
# HTML routes
# ---------------------------------------------------------------------------
@app.route("/")
def index() -> Any:
    """Serve the main designer page."""
    return send_from_directory(app.template_folder, "index.html")


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------
@app.route("/api/save", methods=["POST"])
def save_spec() -> Any:
    """Save the current pipeline specification to disk as JSON."""
    data = request.get_json(force=True)
    pipeline_name = data.get("name", "untitled_pipeline")
    filename = _sanitize_filename(pipeline_name) + ".json"
    filepath = SPECS_DIR / filename

    payload = {
        "name": pipeline_name,
        "saved_at": datetime.utcnow().isoformat() + "Z",
        "spec": _build_pipeline_spec(data),
    }

    with filepath.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2)

    logger.info("Saved pipeline spec: %s", filepath)
    return jsonify({"status": "saved", "filename": filename})


@app.route("/api/list", methods=["GET"])
def list_specs() -> Any:
    """Return a list of saved pipeline specifications."""
    specs = []
    for filepath in sorted(SPECS_DIR.glob("*.json")):
        try:
            with filepath.open("r", encoding="utf-8") as file:
                payload = json.load(file)
            specs.append(
                {
                    "filename": filepath.name,
                    "name": payload.get("name", filepath.stem),
                    "saved_at": payload.get("saved_at"),
                }
            )
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Skipping unreadable spec %s: %s", filepath, exc)

    return jsonify({"specs": specs})


@app.route("/api/load/<filename>", methods=["GET"])
def load_spec(filename: str) -> Any:
    """Load a saved pipeline specification from disk."""
    filepath = SPECS_DIR / filename
    if not filepath.exists():
        return jsonify({"error": "Spec not found"}), 404

    with filepath.open("r", encoding="utf-8") as file:
        payload = json.load(file)

    return jsonify(payload)


@app.route("/api/export", methods=["POST"])
def export_yaml() -> Any:
    """Export the current pipeline as YAML."""
    data = request.get_json(force=True)
    spec = _build_pipeline_spec(data)
    yaml_text = yaml.safe_dump(spec, sort_keys=False, default_flow_style=False)
    return jsonify({"yaml": yaml_text})


@app.route("/api/run", methods=["POST"])
def run_pipeline() -> Any:
    """
    Run the current pipeline using pipeline_builder.PipelineBuilder.

    The frontend sends the pipeline JSON, which we convert to YAML, write to a
    temporary file, and pass to PipelineBuilder. The result of each pipeline is
    returned as metadata (row count and columns) since full DataFrames cannot be
    serialized directly.
    """
    data = request.get_json(force=True)
    spec = _build_pipeline_spec(data)

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False, encoding="utf-8"
    ) as tmp:
        yaml.safe_dump(spec, tmp, sort_keys=False, default_flow_style=False)
        tmp_path = tmp.name

    try:
        spark = _get_spark_session()
        builder = PipelineBuilder(spark)
        results = builder.build(tmp_path)

        output = {}
        for pipeline_name, df in results.items():
            output[pipeline_name] = {
                "row_count": df.count(),
                "columns": df.columns,
            }

        return jsonify({"status": "success", "results": output})
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("Pipeline run failed")
        return jsonify({"status": "error", "message": str(exc)}), 500
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


@app.route("/api/schema", methods=["GET"])
def get_schema() -> Any:
    """Return the supported node schema for the frontend to render forms."""
    return jsonify(
        {
            "node_types": ["Ingestion", "Transformation", "Sink"],
            "connectors": ["PostgreSQL", "S3"],
            "transformations": ["Clean_Data", "Aggregate_Data"],
            "sinks": ["Snowflake", "BigQuery"],
            "fields": {
                "Ingestion": {
                    "label": "Data Source",
                    "required": ["name", "connector"],
                    "connector_fields": {
                        "PostgreSQL": ["host", "port", "database", "table"],
                        "S3": ["bucket", "prefix"],
                    },
                },
                "Transformation": {
                    "label": "Transform",
                    "required": ["name", "transformation", "input_df"],
                },
                "Sink": {
                    "label": "Destination",
                    "required": ["sink_type"],
                    "sink_fields": {
                        "Snowflake": [
                            "account",
                            "database",
                            "schema",
                            "table",
                        ],
                        "BigQuery": ["project", "dataset", "table"],
                    },
                },
            },
        }
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
