# **DBT Model Analyzer**

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue)](https://www.python.org/downloads/release/python-380/)

## **Overview**

The **DBT Model Analyzer** is a tool designed to analyze the lineage of DBT models and compare report models with materialized models. It helps users trace the source of each column in a report, match report columns with materialized models, and identify potential performance improvements by leveraging existing materialized tables.

This tool works by parsing DBT's `manifest.json` file and optionally integrating with YAML model definitions. It outputs lineage details, highlights overlapping columns between report models and materialized models, and provides actionable insights to optimize query performance.

## **Key Features**

- **Lineage Tracing**: Identify the source tables and columns for any given DBT model.
- **Materialized Model Matching**: Compare report models with materialized models and score them based on column overlaps.
- **YAML Integration**: Incorporate YAML model definitions into the analysis for enhanced metadata processing.
- **Performance Insights**: Find existing materialized models that can be reused to improve performance for SQL reports.

## **Table of Contents**

- [**DBT Model Analyzer**](#dbt-model-analyzer)
  - [**Overview**](#overview)
  - [**Key Features**](#key-features)
  - [**Table of Contents**](#table-of-contents)
  - [**Installation**](#installation)
    - [**Requirements**](#requirements)
  - [**Usage**](#usage)
    - [**Command-Line Arguments**](#command-line-arguments)
    - [**Example Command**](#example-command)
  - [**Example Output**](#example-output)
  - [**Contributing**](#contributing)

## **Installation**

To get started, clone the repository and install the required dependencies.

```bash
git clone https://github.com/garrywilliams/dbt-sources-analyser.git
cd dbt-sources-analyser
pip install -r requirements.txt
```

### **Requirements**

- **Python 3.8+**
- **PyYAML** for YAML processing.
- **sqlglot** for SQL parsing.

Install required Python packages using pip:

```bash
pip install -r requirements.txt
```

## **Usage**

Run the tool from the command line to analyze a report model and compare it against materialized models in DBT.

```bash
python analyze_models.py --manifest /path/to/manifest.json --report_model model.report_model_1 --yaml_path /path/to/yaml
```

### **Command-Line Arguments**

| Argument         | Required | Description                                                             |
| ---------------- | -------- | ----------------------------------------------------------------------- |
| `--manifest`     | Yes      | Path to the `manifest.json` file from DBT.                              |
| `--report_model` | Yes      | The name of the report model to analyze (e.g., `model.report_model_1`). |
| `--yaml_path`    | No       | Optional path to YAML model definitions for more detailed analysis.     |

### **Example Command**

```bash
python analyze_models.py --manifest path/to/manifest.json --report_model model.report_1 --yaml_path path/to/yaml
```

This command will:

1. Load the DBT `manifest.json` file.
2. Analyze the lineage of `model.report_1`.
3. Dump the materialized models and their column sources for manual inspection.
4. Compare the report model against materialized models, providing a match score based on overlapping columns.

## **Example Output**

```text
Source columns for report model 'model.report_model_1':
  Column: a, Sources: {('source_model_1', 'a')}
  Column: b, Sources: {('source_model_1', 'b')}
  Column: c, Sources: {('source_model_2', 'c')}

Materialized models and their source columns:
Materialized Model: model.materialized_model_1
  Column: a, Sources: {('source_model_1', 'a')}
  Column: b, Sources: {('source_model_1', 'b')}
  Column: d, Sources: {('source_model_2', 'd')}

Materialized models matching 'model.report_model_1':
  Materialized Model: model.materialized_model_1, Match Score: 2 out of 3
  Matching Columns: [('a', 'a'), ('b', 'b')]
```

## **Contributing**

Contributions are welcome! Please follow these steps to contribute:

1. Fork the repository.
2. Create a new branch (`git checkout -b feature-branch`).
3. Commit your changes (`git commit -am 'Add a new feature'`).
4. Push to the branch (`git push origin feature-branch`).
5. Create a new Pull Request.
