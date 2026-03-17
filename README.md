# Alteryx Workflow Catalog

A command-line tool that scans Alteryx workflow files (`.yxmd`, `.yxwz`, `.yxmc`), parses the XML, and builds a catalog with metadata, data lineage, and an auto-generated plain-English description of what each workflow does — without opening Alteryx Designer.

## Why

Cataloging Alteryx workflows manually means opening each one in Designer, tracing the connections, and writing down what it does. For teams with dozens or hundreds of workflows, this doesn't scale. This tool reads the XML directly and builds the catalog in seconds.

## What It Extracts

| Field | Source |
|---|---|
| Workflow name | Metadata or filename |
| Author | Metadata |
| Department | Workflow-level annotation |
| Description | Metadata (if present) |
| **Auto Description** | **Generated from tool chain — always available** |
| Created / Last Saved | Metadata timestamps |
| Inputs | Input tool connections (ODBC, file paths, Excel, CSV) |
| Outputs | Output tool destinations (databases, files, email) |
| Filters | Filter expressions (e.g. `[status] = "Active"`) |
| Joins | Join fields (e.g. `product_id`) |
| Formulas | Computed fields and expressions |
| Aggregations | Summarize actions (Sum, Count, etc.) |
| Tool inventory | All tools used and total count |

## Auto Description

Even when workflows have no metadata or annotations, the tool reads the XML structure and generates a description:

> "Pulls data from 2 sources: Workday Export, Department Mapping. Filters rows where [status] = "Active". Joins data on [dept_code]. Writes results to HR Database (database)."

It narrates the full data flow: what comes in, how it's transformed, and where it goes.

## Setup

```bash
git clone https://github.com/marcolopez335/alteryx-catalog.git
cd alteryx-catalog
pip install -r requirements.txt
```

## Usage

```bash
# Scan a folder of workflows
python catalog.py /path/to/workflows/

# Scan recursively (subfolders)
python catalog.py /path/to/workflows/ -r

# Export to CSV
python catalog.py /path/to/workflows/ -r --csv catalog.csv

# Scan a network share
python catalog.py "\\fileserver\alteryx\workflows" -r --csv catalog.csv
```

## Example Output

```
┌──────────────────────────────────────────────────────────┐
│  Weekly Sales Report  by Jane Smith                      │
└──────────────────────────────────────────────────────────┘
  Description:  Pulls weekly sales data from SQL Server, joins with
                product catalog, computes KPIs, and publishes to shared drive.
  Auto Summary: Pulls data from 2 sources: Sales Orders (last 7 days),
                Product Catalog. Joins data on [product_id]. Computes new
                fields: avg_price, revenue_tier. Aggregates: Sum(revenue),
                Sum(quantity), Count(order_id). Writes results to 2
                destinations: weekly_sales.csv (CSV file),
                dbo.weekly_sales_summary (database).
  Department:   Weekly Sales Report - Finance Team
  File:         weekly_sales_report.yxmd
  Created:      2025-06-15
  Last Saved:   2026-02-28
  Tools:        7 total (Input, Join, Formula, Output, Summarize)

  Inputs
  #   Label                            Source
  1   Sales Orders (last 7 days)       odbc:DSN=SalesDB | SELECT ... FROM dbo.orders
  2   Product Catalog                  \\fileserver\data\reference\product_catalog.xlsx

  Outputs
  #   Label                   Destination                              Type
  1   Weekly Sales CSV        \\sharepoint\finance\reports\weekly.csv  CSV file
  2   Reporting DB            odbc:DSN=ReportingDB | dbo.weekly_sales  database

  Joins
  Label               Join Fields
  Join on product_id   product_id

  Formulas
  Field            Expression
  avg_price        [revenue] / [quantity]
  revenue_tier     IF [revenue] > 1000 THEN 'High' ELSE 'Low' ENDIF

  Aggregations
  Field       Action   Output Name
  revenue     Sum      total_revenue
  quantity    Sum      total_quantity
  order_id    Count    order_count
```

## CSV Export

The `--csv` flag outputs one row per workflow with all fields — easy to share, filter in Excel, or drop into SharePoint/Confluence.

| Workflow Name | Author | Department | Auto Description | Inputs | Outputs | Filters | Joins | Formulas | Aggregations |
|---|---|---|---|---|---|---|---|---|---|
| Weekly Sales Report | Jane Smith | Finance Team | Pulls data from 2 sources... | SalesDB, product_catalog.xlsx | weekly_sales.csv, ReportingDB | | product_id | avg_price, revenue_tier | Sum(revenue), Count(order_id) |
| HR Headcount Refresh | Mike Johnson | Human Resources | Pulls data from 2 sources... | workday_headcount.csv, dept_mapping.xlsx | HRDB | [status] = "Active" | dept_code | | |

## Dependencies

- Python 3.10+
- `rich` (terminal formatting)

All XML parsing uses Python's built-in `xml.etree.ElementTree` — no extra libraries needed.
