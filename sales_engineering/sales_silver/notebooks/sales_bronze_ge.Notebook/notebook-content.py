# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4ebd26eb-1475-418b-99a7-cb40fa7db777",
# META       "default_lakehouse_name": "sales_lakehouse",
# META       "default_lakehouse_workspace_id": "f11271a2-a297-421d-8f17-0036535e8757",
# META       "known_lakehouses": [
# META         {
# META           "id": "4ebd26eb-1475-418b-99a7-cb40fa7db777"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "8aae97dc-dcfa-a4ea-4432-806ea2317726",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Bronze Data Quality Checks (Great Expectations) — Microsoft Fabric
# 
# This notebook is a **Bronze-layer** adaptation of the Microsoft Tech Community pattern:
# - original article - https://devblogs.microsoft.com/ise/data-validations-with-great-expectations-in-ms-fabric/
# - Read from Lakehouse (parameterized)
# - Create/register expectation suites
# - Add expectations via reusable functions
# - Run validation + persist results + optionally fail the pipeline
# 
# Update the parameters in **Step 0** to match your workspace.


# MARKDOWN ********************

# > **Fabric Environment note:** This notebook assumes `great_expectations` is installed via the attached Fabric **Environment**.
# > If you still have `%pip install` cells, remove them to avoid session-level dependency churn.


# CELL ********************

import datetime
from typing import Dict, List, Optional, Any
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
import great_expectations as gx

RUN_ID = datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')
print(f'RUN_ID={RUN_ID}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 0 — Parameters
# 
# Set your Lakehouse and schema.


# CELL ********************

# ---- EDIT THESE ----
# variable library
varLibrary = notebookutils.variableLibrary.getLibrary("sales_variable_library")
LAKEHOUSE_NAME = varLibrary.lakehouse
SCHEMA = 'sales_bronze'

# Persist DQ results here
DQ_RESULTS_TABLE = f"{LAKEHOUSE_NAME}.{SCHEMA}.dq_results"

# If True: validate each table by ingest_timestamp slices (last N distinct dates)
TIME_SLICE_VALIDATE = False
TIME_SLICE_COL = 'ingest_timestamp'
TIME_SLICE_LAST_N = 3

# Row drift: if you have a baseline table of expected rowcounts, set it here.
# Baseline schema (recommended): table_name STRING, expected_row_count BIGINT, updated_ts TIMESTAMP
DQ_BASELINE_TABLE = None  # e.g. f"{LAKEHOUSE_NAME}.{SCHEMA}.dq_baseline"
MAX_ROW_DRIFT_PCT = 0.20


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1 — Table config (PKs, required fields, schema)
# 
# Update expectations to match your business rules.


# CELL ********************

TABLES: Dict[str, Dict[str, Any]] = {
  'customers': {
    'fqn': f"{LAKEHOUSE_NAME}.{SCHEMA}.customers",
    'primary_key_columns': ['customer_id'],
    'required_columns': ['customer_id', 'customer_name', 'ingest_timestamp'],
    'expected_schema': ['customer_id','customer_name','email','phone','country','state','city','postcode','segment','join_date','ingest_timestamp']
  },
  'regions': {
    'fqn': f"{LAKEHOUSE_NAME}.{SCHEMA}.regions",
    'primary_key_columns': ['state'],
    'required_columns': ['state', 'state_name', 'country', 'gst_rate', 'ingest_timestamp'],
    'expected_schema': ['state','state_name','country','gst_rate','ingest_timestamp']
  },
  'sales_reps': {
    'fqn': f"{LAKEHOUSE_NAME}.{SCHEMA}.sales_reps",
    'primary_key_columns': ['rep_id'],
    'required_columns': ['rep_id', 'rep_name', 'ingest_timestamp'],
    'expected_schema': ['rep_id','rep_name','region','email','ingest_timestamp']
  },
  'products': {
    'fqn': f"{LAKEHOUSE_NAME}.{SCHEMA}.products",
    'primary_key_columns': ['product_id'],
    'required_columns': ['product_id','product_name','category','unit_price_aud', 'active_flag','ingest_timestamp'],
    'expected_schema': ['product_id','product_name','category','unit_price_aud', 'active_flag','ingest_timestamp']
  },
  'order_status': {
    'fqn': f"{LAKEHOUSE_NAME}.{SCHEMA}.order_status",
    'primary_key_columns': ['order_id'],
    'required_columns': ['order_id', 'order_date', 'customer_id', 'sales_rep_id', 'status', 'ingest_timestamp'],
    'expected_schema': ['order_id','order_date','customer_id','sales_rep_id','status','ingest_timestamp']
  },
  'ordered_items': {
    'fqn': f"{LAKEHOUSE_NAME}.{SCHEMA}.ordered_items",
    'primary_key_columns': [],
    'required_columns': ['order_id', 'product_id', 'quantity', 'unit_price_aud', 'discount_pct', 'ingest_timestamp'],
    'expected_schema':  ['order_id', 'product_id', 'quantity', 'unit_price_aud', 'discount_pct', 'ingest_timestamp']
  },
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Great Expectations context + suite helpers


# CELL ********************

context = gx.get_context(mode='ephemeral')

def get_expected_row_count(table_name: str) -> Optional[int]:
    if not DQ_BASELINE_TABLE:
        return None
    try:
        b = (
            spark.sql(f"SELECT table_name, expected_row_count FROM {DQ_BASELINE_TABLE} WHERE table_name = '{table_name}' ORDER BY updated_ts DESC LIMIT 1")
            .collect()
        )
        if not b:
            return None
        return int(b[0]['expected_row_count'])
    except Exception as e:
        print(f"Baseline lookup failed for {table_name}: {e}")
        return None

def create_suite(table_name: str):
    suite_name = f"silver_suite__{table_name}"
    try:
        # Add new or replace existing in ephemeral context
        suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
    except Exception:
        # If it already exists
        suite = context.suites.get(suite_name)
    return suite


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3 — Expectation functions
# 
# The Microsoft article pattern is:
# - row-count drift
# - schema match
# - required columns not-null
# - PK uniqueness
# 
# We keep that pattern and add a few **Silver business rule** checks.


# CELL ********************

def add_core_expectations(
    suite: gx.ExpectationSuite,
    primary_key_columns: List[str],
    required_columns: List[str],
    expected_schema: List[str],
    expected_row_count: Optional[int] = None,
    max_row_drift_pct: float = 0.2,
) -> gx.ExpectationSuite:

    # 1) Row count drift
    if expected_row_count is not None:
        min_rows = int(expected_row_count * (1 - max_row_drift_pct))
        max_rows = int(expected_row_count * (1 + max_row_drift_pct))
        suite.add_expectation(
            gx.expectations.ExpectTableRowCountToBeBetween(min_value=min_rows, max_value=max_rows)
        )

    # 2) Schema compliance
    suite.add_expectation(
        gx.expectations.ExpectTableColumnsToMatchSet(column_set=expected_schema, exact_match=True)
    )

    # 3) Required fields not null
    for c in required_columns:
        suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column=c))

    # 4) Primary key uniqueness
    if primary_key_columns:
        if len(primary_key_columns) == 1:
            suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column=primary_key_columns[0]))
        else:
            suite.add_expectation(gx.expectations.ExpectCompoundColumnsToBeUnique(column_list=primary_key_columns))

    return suite

def add_silver_business_rules(suite: gx.ExpectationSuite, table_name: str, extra: Dict[str, Any]) -> gx.ExpectationSuite:
    
    # Table-specific rule examples based on your ERD
    if table_name in ['customers','sales_reps'] and extra.get('email_regex'):
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToMatchRegex(column='email', regex=extra['email_regex'], mostly=0.95)
        )

    if table_name == 'customers' and extra.get('postcode_regex'):
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToMatchRegex(column='postcode', regex=extra['postcode_regex'], mostly=0.95)
        )

    if table_name == 'regions':
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(column='gst_rate', min_value=extra.get('gst_min',0.0), max_value=extra.get('gst_max',1.0), mostly=0.99)
        )

    if table_name == 'products':
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(column='unit_price_aud', min_value=0.0, max_value=None, mostly=0.999)
        )
        if extra.get('discount_regex'):
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToMatchRegex(column='discount_pct', regex=extra['discount_regex'], mostly=0.95)
            )

    if table_name == 'order_status':
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(column='order_date', min_value=extra.get('min_order_date','1900-01-01'), max_value=str(datetime.datetime.utcnow().date()), mostly=0.999)
        )

    if table_name == 'ordered_items':
        suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column='quantity', min_value=1, max_value=None, mostly=0.999))
        suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column='unit_price_aud', min_value=0.0, max_value=None, mostly=0.999))
        # Note: discount_pct allows NULLs. We'll validate it with a Spark-only check later (non-null rows must be in range).

    return suite


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4 — Datasource + dataframe asset + batch definition
# 
# This matches the Microsoft article approach but uses one datasource per run.


# CELL ********************

# Create Spark datasource once
data_source = context.data_sources.add_spark(name=f"silver_spark_ds__{RUN_ID}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 5 — Run validation and persist results


# CELL ********************

def run_validation_for_df(table_name: str, df: DataFrame, suite: gx.ExpectationSuite) -> Any:
    # Attach data asset + whole dataframe batch definition
    asset = data_source.add_dataframe_asset(name=f"asset__{table_name}__{RUN_ID}")
    batch_def = asset.add_batch_definition_whole_dataframe(f"batch__{table_name}__full")

    # ValidationDefinition pattern from the Microsoft article
    try:
        vdef = gx.ValidationDefinition(data=batch_def, suite=suite, name=f"Silver_DQ__{table_name}__{RUN_ID}")
        return vdef.run(batch_parameters={"dataframe": df})
    except Exception as e:
        # Fallback to validator API if needed
        print(f"ValidationDefinition not available or failed for {table_name}. Falling back. Error: {e}")
        batch = batch_def.get_batch(batch_parameters={"dataframe": df})
        validator = context.get_validator(batch=batch, expectation_suite=suite)
        return validator.validate(result_format='SUMMARY')

def _get_any(obj, *names, default=None):
    for n in names:
        if isinstance(obj, dict) and n in obj:
            return obj[n]
        if hasattr(obj, n):
            return getattr(obj, n)
    return default

def _to_dict(obj):
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "to_json_dict"):
        return obj.to_json_dict()
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if hasattr(obj, "dict"):
        return obj.dict()
    try:
        return dict(obj)
    except Exception:
        return {"_repr": str(obj)}

def gx_results_to_rows(table_name: str, gx_result):
    """Convert GX validation results to a flat list of rows (COLUMN NAMES ONLY)."""
    out = []

    gx_result_dict = _to_dict(gx_result) or {}
    results_list = gx_result_dict.get("results")
    if results_list is None:
        results_list = _get_any(gx_result, "results", default=[])

    # Only keep schema/column-name expectations
    COLUMN_NAME_EXPECTATIONS = {
        "expect_table_columns_to_match_ordered_list",
        "expect_table_columns_to_match_unordered_list",
        "expect_table_columns_to_match_set",  # older naming / some setups
    }

    for r in results_list:
        r_dict = _to_dict(r) or {}
        success = r_dict.get("success", _get_any(r, "success", default=False))

        cfg = r_dict.get("expectation_config", _get_any(r, "expectation_config", default=None))
        cfg_dict = _to_dict(cfg) or {}

        # GE 0.18 uses expectation_type; GE 1.x uses type
        etype = (
            cfg_dict.get("expectation_type")
            or cfg_dict.get("type")
            or _get_any(cfg, "expectation_type", "type", default="unknown")
        )

        # Filter: only column-name expectations
        if str(etype) not in COLUMN_NAME_EXPECTATIONS:
            continue

        kwargs = cfg_dict.get("kwargs") or _get_any(cfg, "kwargs", default={}) or {}
        kwargs = _to_dict(kwargs) or {}

        res = r_dict.get("result", _get_any(r, "result", default={}))
        res_dict = _to_dict(res) or {}

        # For schema expectations, unexpected_count is often absent; keep as 0.
        unexpected_count = res_dict.get("unexpected_count")
        if unexpected_count is None:
            unexpected_count = res_dict.get("partial_unexpected_count", 0)

        key_parts = []
        for k in ["column_list", "column_set", "mostly"]:
            if k in kwargs:
                key_parts.append(f"{k}={kwargs.get(k)}")

        check_name = str(etype) + (" | " + ",".join(key_parts) if key_parts else "")

        # Schema mismatch should be critical
        severity = "CRITICAL"

        out.append({
            "run_id": RUN_ID,
            "table_name": table_name,
            "check_name": check_name,
            "success": bool(success),
            "unexpected_count": int(unexpected_count) if unexpected_count is not None else 0,
            "details": str(res_dict)[:2000],
            "check_type": "GX",
            "severity": severity,
            "run_ts_utc": datetime.datetime.utcnow().isoformat(timespec="seconds"),
        })

    return out


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

all_rows: List[Dict[str, Any]] = []

def load_table(fqn: str) -> DataFrame:
    return spark.sql(f"SELECT * FROM {fqn}")

for table_name, cfg in TABLES.items():
    print(f"--- Validating {table_name} (column names only) ---")
    df = load_table(cfg["fqn"])

    asset_name = table_name
    suite = create_suite(asset_name)

    # validate column names (schema)
    add_core_expectations(
        suite=suite,
        primary_key_columns=[],          # disable
        required_columns=[],             # disable
        expected_schema=cfg.get("expected_schema", []),
        expected_row_count=None,         # disable
        max_row_drift_pct=None,          # disable
    )

    gx_result = run_validation_for_df(asset_name, df, suite)
    all_rows.extend(gx_results_to_rows(table_name, gx_result))

    print(f"Completed GX column-name validation for {table_name} \n")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 6 - Check for validation issues and fail notebook if issues are found

# CELL ********************

df = spark.createDataFrame(all_rows)

failed = df.filter("success = false")

if failed.count() > 0:
    print("❌ Great Expectations schema check failed (column names mismatch) for:")
    display(failed.distinct())
    raise Exception("Great Expectations schema check failed (column names mismatch)")
else:
    print("✅ Great Expectations schema check passed for all tables (column names).")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Keep the spark session alive for the next notebook to use
# - https://www.linkedin.com/pulse/fabric-notebook-performance-hack-reuse-spark-sessions-tiago-balabuch-0cubf/

# CELL ********************

notebookutils.session.stop(detach=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
