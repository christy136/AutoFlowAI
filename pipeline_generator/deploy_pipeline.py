import os
import json

from azure.identity import DefaultAzureCredential, InteractiveBrowserCredential, ChainedTokenCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from utils.settings import (
    BLOB_LS_DEFAULT, SNOWFLAKE_LS_DEFAULT, SRC_DS_DEFAULT, SNK_DS_DEFAULT
)
from datetime import datetime, timezone

# ---------- Helpers ----------

def _ls_exists(adf_client, rg, factory, name: str) -> bool:
    try:
        adf_client.linked_services.get(rg, factory, name)
        return True
    except Exception:
        return False

def _dataset_exists(adf_client, rg, factory, name: str) -> bool:
    try:
        adf_client.datasets.get(rg, factory, name)
        return True
    except Exception:
        return False
# ---- Scheduling helpers ------------------------------------------------------
def _parse_schedule_string(s: str) -> dict | None:
    """
    Accepts: "once", "manual", "", "daily", "daily@HH:MM"
    Returns a dict like {"kind":"daily","hour":H,"minute":M} or None (no trigger).
    """
    if not s:
        return None
    s = str(s).strip().lower()
    if s in {"once", "manual", "off", "disabled", "none"}:
        return None
    if s == "daily":
        return {"kind": "daily", "hour": 0, "minute": 0}
    if s.startswith("daily@"):
        try:
            hhmm = s.split("@", 1)[1]
            hh, mm = hhmm.split(":", 1)
            h = max(0, min(23, int(hh)))
            m = max(0, min(59, int(mm)))
            return {"kind": "daily", "hour": h, "minute": m}
        except Exception:
            # fall back to a daily midnight trigger if parse fails
            return {"kind": "daily", "hour": 0, "minute": 0}
    # If you want to expand later (hourly/cron), do it here.
    return None


def ensure_schedule_trigger(adf_client,
                            resource_group: str,
                            factory_name: str,
                            pipeline_name: str,
                            schedule_string: str) -> dict | None:
    """
    Creates/updates a ScheduleTrigger that runs the given pipeline.
    Starts (enables) the trigger after creation.
    Returns a small result dict or None if no trigger is needed.
    """
    parsed = _parse_schedule_string(schedule_string)
    if not parsed:
        return None  # no trigger requested

    trigger_name = f"{pipeline_name}_schedule"
    now_utc = datetime.now(timezone.utc)

    if parsed["kind"] == "daily":
        hour = int(parsed["hour"])
        minute = int(parsed["minute"])

        props = {
            "type": "ScheduleTrigger",
            "pipelines": [
                {
                    "pipelineReference": {
                        "referenceName": pipeline_name,
                        "type": "PipelineReference"
                    },
                    "parameters": {}
                }
            ],
            "typeProperties": {
                "recurrence": {
                    "frequency": "Day",
                    "interval": 1,
                    "startTime": now_utc.isoformat(),  # required; immediate window OK
                    "timeZone": "UTC",
                    "schedule": {"hours": [hour], "minutes": [minute]}
                }
            }
        }

        adf_client.triggers.create_or_update(
            resource_group_name=resource_group,
            factory_name=factory_name,
            trigger_name=trigger_name,
            trigger={"properties": props}
        )
        # Start (enable) the trigger
        adf_client.triggers.start(
            resource_group_name=resource_group,
            factory_name=factory_name,
            trigger_name=trigger_name
        )
        return {"trigger": trigger_name, "status": "started", "schedule": schedule_string}

    # Future kinds could be handled here
    return None

# ---------- Ensure Linked Services ----------

def ensure_blob_linked_service(adf_client, rg, factory, ls_name: str,
                               storage_account_name: str, storage_key: str):
    """
    Create Azure Blob Storage linked service if missing.
    """
    if _ls_exists(adf_client, rg, factory, ls_name):
        return True, None

    if not storage_account_name or not storage_key:
        return False, {
            "type": "missing_blob_credentials",
            "needed": ["storage_account_name", "storage_key"],
            "message": "Blob Linked Service is missing and needs Storage Account credentials."
        }

    conn_str = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={storage_account_name};"
        f"AccountKey={storage_key};"
        f"EndpointSuffix=core.windows.net"
    )
    props = {
        "type": "AzureBlobStorage",
        "typeProperties": {
            "connectionString": {"type": "SecureString", "value": conn_str}
        }
    }
    adf_client.linked_services.create_or_update(rg, factory, ls_name, {"properties": props})
    return True, None


def ensure_snowflake_linked_service(adf_client, rg, factory, ls_name: str,
                                    connection_string: str):
    """
    Create Snowflake linked service if missing.
    """
    if _ls_exists(adf_client, rg, factory, ls_name):
        return True, None

    if not connection_string:
        return False, {
            "type": "missing_snowflake_connection_string",
            "needed": ["snowflake_connection_string"],
            "message": "Snowflake Linked Service is missing and needs a connection string."
        }

    props = {
    "type": "SnowflakeV2",
    "typeProperties": {
        "connectionString": {"type": "SecureString", "value": connection_string}
    }
    }
    adf_client.linked_services.create_or_update(rg, factory, ls_name, {"properties": props})
    return True, None


def ensure_linked_services_from_config(adf_client, rg, factory, config: dict, ctx: dict):
    """
    Ensures required Linked Services from LLM config exist (blob & snowflake).
    Uses values from:
      - config["source"]["linked_service"], config["sink"]["linked_service"]
      - ctx/environment for credentials
    Returns (ok: bool, issues: list[dict])
    """
    issues = []

    # Source (Blob)
    src = (config or {}).get("source", {}) or {}
    sink = (config or {}).get("sink", {})  or {}
    # Blob LS
    blob_ls = src.get("linked_service") or os.getenv("ADF_BLOB_LINKED_SERVICE") or BLOB_LS_DEFAULT
    # Snowflake LS
    snowflake_ls = sink.get("linked_service") or os.getenv("ADF_SNOWFLAKE_LINKED_SERVICE") or SNOWFLAKE_LS_DEFAULT
    storage_account_name = ctx.get("storage_account_name") or os.getenv("STORAGE_ACCOUNT_NAME")
    storage_key = ctx.get("storage_account_key") or os.getenv("STORAGE_ACCOUNT_KEY")

    ok, issue = ensure_blob_linked_service(
        adf_client, rg, factory, blob_ls, storage_account_name, storage_key
    )
    if not ok and issue:
        issue["linked_service_name"] = blob_ls
        issues.append(issue)

    # Sink (Snowflake)
    sink = (config or {}).get("sink", {}) or {}
    snowflake_ls = sink.get("linked_service") or os.getenv("ADF_SNOWFLAKE_LINKED_SERVICE") or "Snowflake_LS"
    snowflake_conn = ctx.get("snowflake_connection_string") or os.getenv("SNOWFLAKE_CONNECTION_STRING")

    # Fail fast with a clear message so the UI can direct the user to /ui/secrets
    if not snowflake_conn:
        raise RuntimeError(
            "Snowflake connection string not provided. "
            "Set SNOWFLAKE_CONNECTION_STRING in /ui/secrets or pass 'snowflake_connection_string' in context."
        )
    ok, issue = ensure_snowflake_linked_service(
        adf_client, rg, factory, snowflake_ls, snowflake_conn
    )

    if not ok and issue:
        issue["linked_service_name"] = snowflake_ls
        issues.append(issue)

    return len(issues) == 0, issues


# ---------- Ensure Datasets ----------

def ensure_blob_csv_dataset(adf_client, rg, factory, dataset_name: str, blob_ls_name: str,
                            container: str, file_name: str, folder_path: str = ""):
    if _dataset_exists(adf_client, rg, factory, dataset_name):
        return
    props = {
        "linkedServiceName": {
            "referenceName": blob_ls_name,
            "type": "LinkedServiceReference"
        },
        "type": "DelimitedText",
        "typeProperties": {
            "location": {
                "type": "AzureBlobStorageLocation",
                "container": container,
                "folderPath": folder_path or "",
                "fileName": file_name
            },
            "columnDelimiter": ",",
            "firstRowAsHeader": True
        },
        "schema": []
    }
    adf_client.datasets.create_or_update(rg, factory, dataset_name, {"properties": props})


def ensure_snowflake_table_dataset(adf_client, rg, factory, dataset_name: str, snowflake_ls_name: str,
                                   table: str):
    if _dataset_exists(adf_client, rg, factory, dataset_name):
        return
    schema = None
    table_name = table
    if "." in table:
        schema, table_name = table.split(".", 1)

    props = {
        "linkedServiceName": {
            "referenceName": snowflake_ls_name,
            "type": "LinkedServiceReference"
        },
        "type": "SnowflakeTable",
        "typeProperties": {
            "tableName": table_name
        }
    }
    if schema:
        props["typeProperties"]["schema"] = schema

    adf_client.datasets.create_or_update(rg, factory, dataset_name, {"properties": props})


def ensure_datasets_from_config(adf_client, rg, factory, config: dict, ctx: dict):
    """
    Create datasets using names and locations from ctx first, then config.
    We do NOT invent defaults (like 'mycontainer/sales.csv'); if required
    parts are missing, we simply return and let /precheck surface missing inputs.
    """
    # Resolve LS names
    src = (config or {}).get("source", {}) or {}
    sink = (config or {}).get("sink", {}) or {}

    blob_ls = src.get("linked_service") or os.getenv("ADF_BLOB_LINKED_SERVICE") or BLOB_LS_DEFAULT
    snowflake_ls = sink.get("linked_service") or os.getenv("ADF_SNOWFLAKE_LINKED_SERVICE") or SNOWFLAKE_LS_DEFAULT

    # Dataset names (LLM may set them; else from ctx; else canonical)
    source_ds = src.get("dataset_name") or ctx.get("source_dataset_name") or SRC_DS_DEFAULT
    sink_ds   = sink.get("dataset_name") or ctx.get("sink_dataset_name") or SNK_DS_DEFAULT

    # Blob location: prefer explicit ctx
    container   = ctx.get("container")  or os.getenv("BLOB_CONTAINER")
    blob_name   = ctx.get("blob_name")  or os.getenv("BLOB_NAME")
    folder_path = ""

    # If still missing, try parsing config source.path (e.g., "cont/folder/file.csv")
    if (not container or not blob_name) and src.get("path"):
        parts = [p for p in src["path"].split("/") if p]
        if parts:
            container = container or parts[0]
            if len(parts) >= 2:
                blob_name = blob_name or parts[-1]
            if len(parts) > 2:
                folder_path = "/".join(parts[1:-1])

    # If we still don't have a usable blob location, do nothing here.
    # /precheck and auto-fix will request/repair inputs instead of guessing.
    if container and blob_name:
        ensure_blob_csv_dataset(
            adf_client, rg, factory,
            dataset_name=source_ds,
            blob_ls_name=blob_ls,
            container=container,
            file_name=blob_name,
            folder_path=folder_path
        )

    # Snowflake table: prefer config; else build from ctx schema/table; else skip
    table_qualified = sink.get("table")
    if not table_qualified:
        schema = ctx.get("snowflake_schema")
        table  = ctx.get("snowflake_table")
        if schema and table:
            table_qualified = f"{schema}.{table}"

    if table_qualified:
        ensure_snowflake_table_dataset(
            adf_client, rg, factory,
            dataset_name=sink_ds,
            snowflake_ls_name=snowflake_ls,
            table=table_qualified
        )



# ---------- Public: Deploy Pipeline ----------

def deploy_to_adf(pipeline_json_path: str, config: dict, ctx: dict):
    """
    Loads pipeline JSON, ensures Linked Services + Datasets exist, then deploys.
    ctx can carry:
      - storage_account_name
      - storage_account_key
      - snowflake_connection_string
    """
    if not os.path.exists(pipeline_json_path):
        raise FileNotFoundError(f"Pipeline file not found: {pipeline_json_path}")

    with open(pipeline_json_path, "r") as f:
        pipeline_data = json.load(f)

    pipeline_name = pipeline_data.get("name", "GeneratedPipeline")

    subscription_id = ctx.get("subscription_id") or os.getenv("AZURE_SUBSCRIPTION_ID")
    resource_group  = ctx.get("resource_group")  or os.getenv("AZURE_RESOURCE_GROUP")
    factory_name    = ctx.get("factory_name")    or os.getenv("AZURE_FACTORY_NAME")

    if not all([subscription_id, resource_group, factory_name]):
        raise RuntimeError("Azure env vars missing: AZURE_SUBSCRIPTION_ID, AZURE_RESOURCE_GROUP, AZURE_FACTORY_NAME")

    credential = ChainedTokenCredential(
        DefaultAzureCredential(exclude_cli_credential=False),
        InteractiveBrowserCredential()
    )
    adf_client = DataFactoryManagementClient(credential, subscription_id)

    # Ensure Linked Services
    ok, issues = ensure_linked_services_from_config(adf_client, resource_group, factory_name, config, ctx)
    if not ok:
        return {
            "status": "blocked",
            "reason": "missing_credentials_or_names",
            "issues": issues
        }

    # Ensure Datasets (now requires ctx)
    ensure_datasets_from_config(adf_client, resource_group, factory_name, config, ctx)

    # Deploy Pipeline
    result = adf_client.pipelines.create_or_update(
        resource_group_name=resource_group,
        factory_name=factory_name,
        pipeline_name=pipeline_name,
        pipeline={"properties": pipeline_data["properties"]}
    )

    # 🔔 Ensure schedule trigger if requested
    trigger_result = None
    schedule_str = (config or {}).get("schedule", "once")
    try:
        trigger_result = ensure_schedule_trigger(
            adf_client, resource_group, factory_name, pipeline_name, schedule_str
        )
    except Exception as e:
        # Don't fail the deployment just because scheduling failed
        trigger_result = {"error": f"trigger_setup_failed: {e}", "schedule": schedule_str}

    return {
        "status": "deployed",
        "pipeline_name": pipeline_name,
        "result": str(result),
        "trigger_result": trigger_result
    }
