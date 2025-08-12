import os
import json

from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient


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

    props = {
        "type": "AzureBlobStorage",
        "typeProperties": {
            "connectionString": (
                f"DefaultEndpointsProtocol=https;"
                f"AccountName={storage_account_name};"
                f"AccountKey={storage_key};"
                f"EndpointSuffix=core.windows.net"
            )
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
        "type": "Snowflake",
        "typeProperties": {
            "connectionString": connection_string
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
    blob_ls = src.get("linked_service") or os.getenv("ADF_BLOB_LINKED_SERVICE") or "AzureBlobStorage_LS"
    storage_account_name = ctx.get("storage_account_name") or os.getenv("STORAGE_ACCOUNT_NAME")
    storage_key = ctx.get("storage_account_key") or os.getenv("STORAGE_ACCOUNT_KEY")

    ok, issue = ensure_blob_linked_service(
        adf_client, rg, factory, blob_ls, storage_account_name, storage_key
    )
    if not ok and issue:
        # also include which linked service name we were trying to create
        issue["linked_service_name"] = blob_ls
        issues.append(issue)

    # Sink (Snowflake)
    sink = (config or {}).get("sink", {}) or {}
    snowflake_ls = sink.get("linked_service") or os.getenv("ADF_SNOWFLAKE_LINKED_SERVICE") or "Snowflake_LS"
    snowflake_conn = ctx.get("snowflake_connection_string") or os.getenv("SNOWFLAKE_CONNECTION_STRING")

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
    # Split schema.table if given
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


def ensure_datasets_from_config(adf_client, rg, factory, config: dict):
    """
    Create default datasets 'SourceDataset' and 'SinkDataset' from config if missing.
    Infers blob container/path/file from source.path (e.g., 'mycontainer/sales.csv' or 'mycontainer/folder/file.csv')
    """
    src = (config or {}).get("source", {}) or {}
    sink = (config or {}).get("sink", {}) or {}
    blob_ls = src.get("linked_service") or os.getenv("ADF_BLOB_LINKED_SERVICE") or "AzureBlobStorage_LS"
    snowflake_ls = sink.get("linked_service") or os.getenv("ADF_SNOWFLAKE_LINKED_SERVICE") or "Snowflake_LS"

    # Parse source path -> container, (optional) folder, file
    src_path = src.get("path") or ""
    parts = [p for p in src_path.split("/") if p]
    container = parts[0] if len(parts) >= 1 else "mycontainer"
    file_name = parts[-1] if len(parts) >= 2 else "sales.csv"
    folder_path = ""
    if len(parts) > 2:
        # everything between container and file
        folder_path = "/".join(parts[1:-1])

    ensure_blob_csv_dataset(
        adf_client, rg, factory,
        dataset_name="SourceDataset",
        blob_ls_name=blob_ls,
        container=container,
        file_name=file_name,
        folder_path=folder_path
    )

    ensure_snowflake_table_dataset(
        adf_client, rg, factory,
        dataset_name="SinkDataset",
        snowflake_ls_name=snowflake_ls,
        table=sink.get("table") or "finance.daily_sales"
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

    subscription_id = os.getenv("AZURE_SUBSCRIPTION_ID")
    resource_group = os.getenv("AZURE_RESOURCE_GROUP")
    factory_name = os.getenv("AZURE_FACTORY_NAME")

    if not all([subscription_id, resource_group, factory_name]):
        raise RuntimeError("Azure env vars missing: AZURE_SUBSCRIPTION_ID, AZURE_RESOURCE_GROUP, AZURE_FACTORY_NAME")

    credential = DefaultAzureCredential()
    adf_client = DataFactoryManagementClient(credential, subscription_id)

    # Ensure Linked Services
    ok, issues = ensure_linked_services_from_config(adf_client, resource_group, factory_name, config, ctx)
    if not ok:
        return {
            "status": "blocked",
            "reason": "missing_credentials_or_names",
            "issues": issues
        }

    # Ensure Datasets
    ensure_datasets_from_config(adf_client, resource_group, factory_name, config)

    # Deploy Pipeline
    result = adf_client.pipelines.create_or_update(
        resource_group_name=resource_group,
        factory_name=factory_name,
        pipeline_name=pipeline_name,
        pipeline={"properties": pipeline_data["properties"]}
    )
    return {
        "status": "deployed",
        "pipeline_name": pipeline_name,
        "result": str(result)
    }
