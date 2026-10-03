# Pipeline Builder

A lightweight, modular utility that builds Spark DataFrame-based data pipelines from YAML specifications. It is intended as a core component of a Data Platform Framework.

## Features

- **YAML-driven pipelines**: Define one or more pipelines and their nodes in a single YAML file.
- **Node types**: `Ingestion`, `Transformation`, and `Sink`.
- **Pluggable implementations**: Register custom connectors, transformations, and sinks via constructor arguments.
- **DataFrame chaining**: Each node produces a named Spark DataFrame that downstream nodes can reference by `input_df`.
- **Production-ready skeleton**: Includes structured error handling, logging, and placeholder implementations that can be replaced with real data-store integrations.

## Project Structure

```text
data_pipeline_generator/
├── app.py                       # Flask web designer backend
├── pipeline_builder.py          # Core builder implementation
├── pipeline_spec_example.yaml   # Example pipeline specification
├── requirements.txt             # Python dependencies
├── static/                      # Frontend assets
│   ├── app.js
│   └── style.css
├── templates/
│   └── index.html               # Main designer page
├── saved_specs/                 # Persisted pipeline specs
└── README.md                    # This file
```

## Installation

Install the required dependencies:

```bash
pip install -r requirements.txt
```

## Web UI

A drag-and-drop web designer is included. Run it with:

```bash
python app.py
```

Then open http://127.0.0.1:5000 in a browser.

Features:

- Drag Ingestion, Transformation, and Sink nodes onto a canvas.
- Configure each node via the sidebar inspector.
- Save/load pipeline specifications to `saved_specs/`.
- Export the current pipeline as YAML.
- Run the pipeline and view row counts and column names for each output.

## YAML Specification Format

Each YAML file contains a `version` and a list of `pipelines`. Every pipeline has a `name` and a list of `nodes`.

### Supported Node Types

#### Ingestion

Reads data from an external source.

```yaml
- type: "Ingestion"
  name: "customer_data_df"
  connector: "PostgreSQL"
  config:
    host: "db.example.com"
    port: 5432
    database: "customer_data"
    table: "customers"
```

#### Transformation

Applies a named transformation to an existing DataFrame.

```yaml
- type: "Transformation"
  name: "customer_data_cleaned_df"
  transformation: "Clean_Data"
  input_df: "customer_data_df"
```

#### Sink

Writes a DataFrame to a target destination.

```yaml
- type: "Sink"
  sink_type: "Snowflake"
  input_df: "customer_data_cleaned_df"
  config:
    account: "my_snowflake_account"
    database: "my_snowflake_database"
    schema: "my_snowflake_schema"
    table: "customer_data_final"
```

## Usage

### As a Python Module

```python
from pyspark.sql import SparkSession
from pipeline_builder import PipelineBuilder

spark = SparkSession.builder.appName("PipelineBuilder").getOrCreate()
builder = PipelineBuilder(spark)
results = builder.build("pipeline_spec_example.yaml")

for pipeline_name, df in results.items():
    print(f"{pipeline_name}: {df.count()} rows")
```

### From the Command Line

```bash
python pipeline_builder.py pipeline_spec_example.yaml
```

### Custom Connectors, Transformations, and Sinks

Pass custom callables to override the default placeholder implementations:

```python
def my_custom_connector(spark, config):
    return spark.read.format("jdbc").options(**config).load()

def my_custom_transformation(df):
    return df.dropna()

def my_custom_sink(df, config):
    df.write.format("parquet").save(config["path"])
    return df

builder = PipelineBuilder(
    spark,
    connectors={"PostgreSQL": my_custom_connector},
    transformations={"Clean_Data": my_custom_transformation},
    sinks={"Snowflake": my_custom_sink},
)
```

## Defaults

The builder ships with the following placeholder defaults:

- **Connectors**: `PostgreSQL`, `S3`
- **Transformations**: `Clean_Data`, `Aggregate_Data`
- **Sinks**: `Snowflake`, `BigQuery`

The default implementations log the requested operation and return an empty Spark DataFrame so that the pipeline structure can be exercised without live data sources.

## Error Handling

Custom exceptions are raised for common problems:

- `UnknownNodeTypeError` — node `type` is not supported
- `UnknownConnectorError` — connector name is not registered
- `UnknownTransformationError` — transformation name is not registered
- `UnknownSinkError` — sink type is not registered
- `MissingInputDataFrameError` — a node references a DataFrame that does not exist
- `PipelineBuilderError` — general schema or configuration errors

## Assumptions

- A SparkSession is already initialized and passed to the builder.
- YAML files follow the supported schema shown in [pipeline_spec_example.yaml](pipeline_spec_example.yaml).
- Real connector/sink implementations must be provided for production workloads.

## License

This is an internal utility for the Data Platform Framework.
