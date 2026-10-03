"""
pipeline_builder.py

A utility module for constructing Spark DataFrame-based data pipelines from YAML
specifications. The module parses pipeline definitions, creates DataFrames for
each node, and chains Ingestion -> Transformation -> Sink operations together.

Assumptions:
    - A SparkSession is available and passed to the builder.
    - Connector/sink implementations are provided as pluggable callables.
    - YAML files follow the supported schema (see README / examples).

Example usage:
    >>> from pyspark.sql import SparkSession
    >>> spark = SparkSession.builder.appName("Pipeline").getOrCreate()
    >>> builder = PipelineBuilder(spark)
    >>> dfs = builder.build("pipeline.yaml")
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import yaml
from pyspark.sql import DataFrame, SparkSession


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------
class PipelineBuilderError(Exception):
    """Base exception for pipeline builder errors."""


class UnknownNodeTypeError(PipelineBuilderError):
    """Raised when a node type is not one of the supported types."""


class UnknownConnectorError(PipelineBuilderError):
    """Raised when an ingestion connector is not registered."""


class UnknownSinkError(PipelineBuilderError):
    """Raised when a sink type is not registered."""


class UnknownTransformationError(PipelineBuilderError):
    """Raised when a transformation type is not registered."""


class MissingInputDataFrameError(PipelineBuilderError):
    """Raised when a transformation references a missing input DataFrame."""


# ---------------------------------------------------------------------------
# Default node implementations (placeholders)
# ---------------------------------------------------------------------------
def _default_postgres_connector(spark: SparkSession, config: Dict[str, Any]) -> DataFrame:
    """
    Placeholder connector for PostgreSQL ingestion.

    In a production implementation this would use ``spark.read.format("jdbc")``
    with the provided host, port, database and table. Here we return an empty
    DataFrame so the pipeline skeleton can be exercised without a real DB.
    """
    logger.info(
        "Connecting to PostgreSQL at %(host)s:%(port)s/%(database)s table=%(table)s",
        config,
    )
    return spark.createDataFrame([], schema="id LONG, value STRING")


def _default_s3_connector(spark: SparkSession, config: Dict[str, Any]) -> DataFrame:
    """
    Placeholder connector for S3 ingestion.

    In a production implementation this would use ``spark.read.format("parquet")``
    or ``spark.read.text`` against the given bucket and prefix. Here we return an
    empty DataFrame so the pipeline skeleton can be exercised without real S3 data.
    """
    logger.info("Reading from S3 bucket=%(bucket)s prefix=%(prefix)s", config)
    return spark.createDataFrame([], schema="key STRING, value STRING")


def _default_clean_data(df: DataFrame, **kwargs: Any) -> DataFrame:
    """Placeholder transformation that returns the input DataFrame unchanged."""
    logger.info("Applying Clean_Data transformation")
    return df


def _default_aggregate_data(df: DataFrame, **kwargs: Any) -> DataFrame:
    """Placeholder transformation that returns the input DataFrame unchanged."""
    logger.info("Applying Aggregate_Data transformation")
    return df


def _default_snowflake_sink(df: DataFrame, config: Dict[str, Any]) -> DataFrame:
    """
    Placeholder sink for Snowflake.

    A real implementation would call ``df.write.format("snowflake").options(...).save()``.
    We return the DataFrame so the pipeline chain can continue to be inspected.
    """
    logger.info("Writing to Snowflake account=%(account)s table=%(table)s", config)
    return df


def _default_bigquery_sink(df: DataFrame, config: Dict[str, Any]) -> DataFrame:
    """
    Placeholder sink for BigQuery.

    A real implementation would call ``df.write.format("bigquery").save()``.
    We return the DataFrame so the pipeline chain can continue to be inspected.
    """
    logger.info("Writing to BigQuery project=%(project)s table=%(table)s", config)
    return df


# ---------------------------------------------------------------------------
# PipelineBuilder
# ---------------------------------------------------------------------------
class PipelineBuilder:
    """
    Build Spark DataFrame pipelines from YAML specifications.

    Parameters
    ----------
    spark : SparkSession
        An initialized SparkSession used to create DataFrames.
    connectors : Optional[Dict[str, Callable[[SparkSession, Dict[str, Any]], DataFrame]]]
        Mapping from connector name to a callable that returns a DataFrame.
    transformations : Optional[Dict[str, Callable[[DataFrame], DataFrame]]]
        Mapping from transformation name to a callable that transforms a DataFrame.
    sinks : Optional[Dict[str, Callable[[DataFrame, Dict[str, Any]], DataFrame]]]
        Mapping from sink name to a callable that writes a DataFrame.

    The default registries contain placeholder implementations for the connectors,
    transformations and sinks shown in the few-shot examples. Replace them with
    real implementations for production use.
    """

    def __init__(
        self,
        spark: SparkSession,
        connectors: Optional[Dict[str, Callable[[SparkSession, Dict[str, Any]], DataFrame]]] = None,
        transformations: Optional[Dict[str, Callable[[DataFrame], DataFrame]]] = None,
        sinks: Optional[Dict[str, Callable[[DataFrame, Dict[str, Any]], DataFrame]]] = None,
    ) -> None:
        self.spark = spark

        # Register default connectors. Users can override by passing their own dict.
        self.connectors = connectors or {
            "PostgreSQL": _default_postgres_connector,
            "S3": _default_s3_connector,
        }

        # Register default transformations. Users can override by passing their own dict.
        self.transformations = transformations or {
            "Clean_Data": _default_clean_data,
            "Aggregate_Data": _default_aggregate_data,
        }

        # Register default sinks. Users can override by passing their own dict.
        self.sinks = sinks or {
            "Snowflake": _default_snowflake_sink,
            "BigQuery": _default_bigquery_sink,
        }

    # -----------------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------------
    def build(self, yaml_path: str) -> Dict[str, DataFrame]:
        """
        Parse a YAML pipeline specification and build DataFrames for every node.

        Parameters
        ----------
        yaml_path : str
            Path to the YAML file containing the pipeline specification.

        Returns
        -------
        Dict[str, DataFrame]
            Mapping from pipeline name to the final DataFrame produced by that
            pipeline's sink node.

        Chain of Thought:
            1. Read and parse the YAML file to obtain the pipeline definitions.
            2. Validate the top-level schema (version and pipelines list).
            3. For each pipeline, iterate over its nodes in declaration order.
            4. For each node, dispatch to the appropriate handler based on ``type``.
            5. Track produced DataFrames so downstream nodes can reference them.
            6. Return the final DataFrame for each pipeline.
        """
        spec = self._load_spec(yaml_path)

        # The top-level structure must contain a list of pipelines.
        pipelines = spec.get("pipelines", [])
        if not isinstance(pipelines, list):
            raise PipelineBuilderError("'pipelines' must be a list")

        results: Dict[str, DataFrame] = {}
        for pipeline in pipelines:
            pipeline_name = pipeline["name"]
            logger.info("Building pipeline: %s", pipeline_name)
            results[pipeline_name] = self._build_pipeline(pipeline)

        return results

    # -----------------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------------
    def _load_spec(self, yaml_path: str) -> Dict[str, Any]:
        """Load and parse the YAML file from disk."""
        path = Path(yaml_path)
        if not path.exists():
            raise PipelineBuilderError(f"YAML specification not found: {yaml_path}")

        with path.open("r", encoding="utf-8") as file:
            spec = yaml.safe_load(file)

        if not isinstance(spec, dict):
            raise PipelineBuilderError("YAML file must contain a top-level mapping")

        return spec

    def _build_pipeline(self, pipeline: Dict[str, Any]) -> DataFrame:
        """
        Build a single pipeline by processing each node sequentially.

        Nodes are processed in the order they appear in the YAML file. Each node
        produces a DataFrame that can be referenced by name by later nodes using
        the ``input_df`` field.
        """
        nodes = pipeline.get("nodes", [])
        if not isinstance(nodes, list):
            raise PipelineBuilderError("Each pipeline's 'nodes' must be a list")

        # Keep a registry of DataFrames produced by node names so that
        # transformation nodes can resolve their inputs.
        dataframes: Dict[str, DataFrame] = {}

        final_df: Optional[DataFrame] = None
        for node in nodes:
            node_type = node.get("type")
            if node_type is None:
                raise PipelineBuilderError("Every node must have a 'type' field")

            # Dispatch to the correct node handler.
            if node_type == "Ingestion":
                df = self._build_ingestion_node(node)
            elif node_type == "Transformation":
                df = self._build_transformation_node(node, dataframes)
            elif node_type == "Sink":
                df = self._build_sink_node(node, dataframes)
            else:
                raise UnknownNodeTypeError(f"Unsupported node type: {node_type}")

            # Name the resulting DataFrame. The name defaults to the pipeline name
            # plus "_df" when the node does not explicitly provide one.
            node_name = node.get("name", f"{pipeline['name']}_df")
            dataframes[node_name] = df
            final_df = df

        if final_df is None:
            raise PipelineBuilderError(f"Pipeline '{pipeline['name']}' contains no nodes")

        return final_df

    def _build_ingestion_node(self, node: Dict[str, Any]) -> DataFrame:
        """
        Build an Ingestion node.

        The node must specify a ``connector`` and a ``config`` dictionary. We look
        up the connector in the registered connector map and invoke it with the
        SparkSession and configuration.
        """
        connector_name = node.get("connector")
        if connector_name is None:
            raise PipelineBuilderError("Ingestion node missing 'connector' field")

        config = node.get("config", {})
        if not isinstance(config, dict):
            raise PipelineBuilderError("Ingestion node 'config' must be a dictionary")

        connector = self.connectors.get(connector_name)
        if connector is None:
            raise UnknownConnectorError(
                f"Unknown connector '{connector_name}'. "
                f"Registered connectors: {list(self.connectors.keys())}"
            )

        return connector(self.spark, config)

    def _build_transformation_node(
        self, node: Dict[str, Any], dataframes: Dict[str, DataFrame]
    ) -> DataFrame:
        """
        Build a Transformation node.

        The node must specify a ``transformation`` type and an ``input_df`` that
        references a DataFrame already produced by an earlier node. We resolve
        the input DataFrame and apply the registered transformation.
        """
        transformation_name = node.get("transformation")
        if transformation_name is None:
            raise PipelineBuilderError("Transformation node missing 'transformation' field")

        input_df_name = node.get("input_df")
        if input_df_name is None:
            raise PipelineBuilderError("Transformation node missing 'input_df' field")

        input_df = dataframes.get(input_df_name)
        if input_df is None:
            raise MissingInputDataFrameError(
                f"Transformation '{transformation_name}' references unknown "
                f"input DataFrame '{input_df_name}'"
            )

        transformation = self.transformations.get(transformation_name)
        if transformation is None:
            raise UnknownTransformationError(
                f"Unknown transformation '{transformation_name}'. "
                f"Registered transformations: {list(self.transformations.keys())}"
            )

        return transformation(input_df)

    def _build_sink_node(
        self, node: Dict[str, Any], dataframes: Dict[str, DataFrame]
    ) -> DataFrame:
        """
        Build a Sink node.

        The node must specify a ``sink_type`` and a ``config`` dictionary. By
        convention a sink writes the most recently produced DataFrame. If the
        node provides ``input_df`` we use that explicitly, otherwise we fall back
        to the last DataFrame in the registry.
        """
        sink_type = node.get("sink_type")
        if sink_type is None:
            raise PipelineBuilderError("Sink node missing 'sink_type' field")

        config = node.get("config", {})
        if not isinstance(config, dict):
            raise PipelineBuilderError("Sink node 'config' must be a dictionary")

        input_df_name = node.get("input_df")
        if input_df_name is not None:
            input_df = dataframes.get(input_df_name)
            if input_df is None:
                raise MissingInputDataFrameError(
                    f"Sink '{sink_type}' references unknown input DataFrame '{input_df_name}'"
                )
        elif not dataframes:
            raise PipelineBuilderError("Sink node has no available input DataFrame")
        else:
            # Fall back to the most recently produced DataFrame.
            input_df = list(dataframes.values())[-1]

        sink = self.sinks.get(sink_type)
        if sink is None:
            raise UnknownSinkError(
                f"Unknown sink '{sink_type}'. "
                f"Registered sinks: {list(self.sinks.keys())}"
            )

        return sink(input_df, config)


# ---------------------------------------------------------------------------
# CLI entry point (optional)
# ---------------------------------------------------------------------------
def main() -> None:
    """Simple CLI wrapper for building pipelines from a YAML file."""
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <path-to-pipeline-spec.yaml>", file=sys.stderr)
        raise SystemExit(1)

    yaml_path = sys.argv[1]

    # Chain of Thought for the CLI:
    # 1. Initialize a SparkSession (the user may also inject an existing session).
    # 2. Instantiate the PipelineBuilder with the SparkSession.
    # 3. Call build() with the provided YAML path.
    # 4. Print the names of the DataFrames produced for each pipeline.
    spark = SparkSession.builder.appName("PipelineBuilder").getOrCreate()
    builder = PipelineBuilder(spark)
    results = builder.build(yaml_path)

    print("\nPipeline build complete. Produced DataFrames:")
    for pipeline_name, df in results.items():
        print(f"  - {pipeline_name}: {type(df).__name__} with {df.count()} rows")


if __name__ == "__main__":
    main()
