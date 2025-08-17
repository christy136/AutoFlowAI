"""
Self-sufficient prerequisites checker + auto-fixer for ADF deployments.
"""
from typing import Dict, Any, List, Tuple
import os

from azure.identity import DefaultAzureCredential, InteractiveBrowserCredential, ChainedTokenCredential
from azure.mgmt.subscription import SubscriptionClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.storage.blob import BlobServiceClient
from utils.settings import (
    BLOB_LS_DEFAULT, SNOWFLAKE_LS_DEFAULT, SRC_DS_DEFAULT, SNK_DS_DEFAULT,
    CSV_DELIMITER_DEFAULT, CSV_HEADER_DEFAULT, REGION_FALLBACK
)

# ----------------------------- helpers -----------------------------
def _ok(item: str, details: Dict[str, Any] = None):
    return {"item": item, "status": "present", "details": details or {}}

def _missing(item: str, how_to_fix: str, details: Dict[str, Any] = None):
    return {"item": item, "status": "missing", "how_to_fix": how_to_fix, "details": details or {}}

def _error(item: str, error: str, details: Dict[str, Any] = None):
    return {"item": item, "status": "error", "error": error, "details": details or {}}

def _finish(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "summary": {
            "present": [i["item"] for i in items if i["status"] == "present"],
            "missing": [i for i in items if i["status"] == "missing"],
            "errors":  [i for i in items if i["status"] == "error"],
        },
        "items": items
    }

# ----------------------------- check -----------------------------

def check_prerequisites(context: Dict[str, Any]) -> Dict[str, Any]:
    report: List[Dict[str, Any]] = []

    # Resolve config from context or env
    subscription_id = context.get("subscription_id") or os.getenv("AZURE_SUBSCRIPTION_ID")
    resource_group  = context.get("resource_group")  or os.getenv("AZURE_RESOURCE_GROUP")
    factory_name    = context.get("factory_name")    or os.getenv("AZURE_FACTORY_NAME")

    storage_account_name = context.get("storage_account_name") or os.getenv("STORAGE_ACCOUNT_NAME")
    storage_account_key  = context.get("storage_account_key")  or os.getenv("STORAGE_ACCOUNT_KEY")
    container_name = context.get("container") or os.getenv("BLOB_CONTAINER")
    blob_name      = context.get("blob_name") or os.getenv("BLOB_NAME")

    blob_ls_name        = context.get("blob_ls_name") or os.getenv("ADF_BLOB_LINKED_SERVICE") or BLOB_LS_DEFAULT
    snowflake_ls_name   = context.get("snowflake_ls_name") or os.getenv("ADF_SNOWFLAKE_LINKED_SERVICE") or SNOWFLAKE_LS_DEFAULT
    source_dataset_name = context.get("source_dataset_name") or SRC_DS_DEFAULT
    sink_dataset_name   = context.get("sink_dataset_name") or SNK_DS_DEFAULT

    # --------- sanity (collect all hard-missing and return early) ----------
    hard_missing = []
    if not subscription_id:
        hard_missing.append(_missing("subscription_id", "Set AZURE_SUBSCRIPTION_ID or include 'subscription_id' in context"))
    if not resource_group:
        hard_missing.append(_missing("resource_group", "Set AZURE_RESOURCE_GROUP or include 'resource_group' in context"))
    if not factory_name:
        hard_missing.append(_missing("factory_name", "Set AZURE_FACTORY_NAME or include 'factory_name' in context"))

    if hard_missing:
        report.extend(hard_missing)
        return _finish(report)

    # --------- clients (with browser fallback) ----------
    try:
        credential = ChainedTokenCredential(
            DefaultAzureCredential(exclude_cli_credential=False),
            InteractiveBrowserCredential()
        )
        sub_client  = SubscriptionClient(credential)
        res_client  = ResourceManagementClient(credential, subscription_id)
        adf_client  = DataFactoryManagementClient(credential, subscription_id)
        stor_client = StorageManagementClient(credential, subscription_id)
        report.append(_ok("credentials", {"type": "ChainedTokenCredential"}))
    except Exception as e:
        report.append(_error("credentials", f"Auth failed: {e}"))
        return _finish(report)

    # --------- subscription (robust) ----------
    try:
        subs = list(sub_client.subscriptions.list())
        if any(getattr(s, "subscription_id", "") == subscription_id for s in subs):
            report.append(_ok("subscription", {"subscription_id": subscription_id}))
        else:
            report.append(_missing("subscription",
                                   "az account set --subscription <id>",
                                   {"subscription_id": subscription_id}))
            return _finish(report)
    except Exception as e:
        # Non-fatal if enumeration fails, but still record the id
        report.append(_ok("subscription", {"subscription_id": subscription_id, "note": f"Could not enumerate ({e})"}))

    # --------- providers (coarse) ----------
    try:
        list(adf_client.factories.list())
        list(stor_client.storage_accounts.list())
        report.append(_ok("providers", {"Microsoft.DataFactory": "ok", "Microsoft.Storage": "ok"}))
    except Exception as e:
        report.append(_missing(
            "providers",
            "az provider register --namespace Microsoft.DataFactory && az provider register --namespace Microsoft.Storage",
            {"error": str(e)}
        ))

    # --------- resource group ----------
    try:
        rg = res_client.resource_groups.get(resource_group)
        report.append(_ok("resource_group", {"name": rg.name, "location": rg.location}))
    except Exception as e:
        report.append(_missing(
            "resource_group",
            f"az group create -n {resource_group} -l <your-region>",
            {"error": str(e)}
        ))
        return _finish(report)

    # --------- data factory ----------
    try:
        factory = adf_client.factories.get(resource_group, factory_name)
        report.append(_ok("data_factory", {"name": factory.name, "location": factory.location}))
    except Exception as e:
        report.append(_missing(
            "data_factory",
            f"az datafactory create -g {resource_group} -n {factory_name} -l <your-region>",
            {"error": str(e)}
        ))
        return _finish(report)

    # --------- storage account existence (ARM) ----------
    if storage_account_name:
        try:
            acct = stor_client.storage_accounts.get_properties(resource_group, storage_account_name)
            report.append(_ok("storage_account", {"name": acct.name, "location": acct.location}))
        except Exception as e:
            report.append(_missing(
                "storage_account",
                f"az storage account create -g {resource_group} -n {storage_account_name} -l <your-region> --sku Standard_LRS --kind StorageV2",
                {"error": str(e)}
            ))
        # Data plane check if key present
        if storage_account_key:
            try:
                endpoint = f"https://{storage_account_name}.blob.core.windows.net"
                bsc = BlobServiceClient(account_url=endpoint, credential=storage_account_key)
                containers = [c['name'] if isinstance(c, dict) else c.name for c in bsc.list_containers()]
                if container_name:
                    if container_name in containers:
                        report.append(_ok("blob_container", {"name": container_name}))
                        if blob_name:
                            blob_names = [b.name for b in bsc.get_container_client(container_name).list_blobs()]
                            if blob_name in blob_names:
                                report.append(_ok("blob_exists", {"name": blob_name}))
                            else:
                                report.append(_missing(
                                    "blob_exists",
                                    f"az storage blob upload --account-name {storage_account_name} --account-key <KEY> "
                                    f"--container-name {container_name} --name {blob_name} --file ./sales.csv"
                                ))
                        else:
                            report.append(_missing("blob_name", "Provide blob file name in context or env (BLOB_NAME)."))
                    else:
                        report.append(_missing(
                            "blob_container",
                            f"az storage container create --account-name {storage_account_name} --account-key <KEY> --name {container_name}"
                        ))
                else:
                    report.append(_missing("container", "Provide blob container name in context or env (BLOB_CONTAINER)."))
            except Exception as e:
                report.append(_error("blob_data_plane", f"Blob data-plane access failed: {e}"))
        else:
            report.append(_missing("storage_account_key", "Provide STORAGE_ACCOUNT_KEY or use Managed Identity for LS."))
    else:
        report.append(_missing("storage_account_name", "Provide 'storage_account_name' in context or env."))

    # --------- linked services ----------
    try:
        ls_names = {ls.name for ls in adf_client.linked_services.list_by_factory(resource_group, factory_name)}
        if blob_ls_name in ls_names:
            report.append(_ok("blob_linked_service", {"name": blob_ls_name}))
        else:
            report.append(_missing(
                "blob_linked_service",
                f"Create Blob LS '{blob_ls_name}' (connection string SecureString or Managed Identity)."
            ))
        if snowflake_ls_name in ls_names:
            report.append(_ok("snowflake_linked_service", {"name": snowflake_ls_name}))
        else:
            report.append(_missing(
                "snowflake_linked_service",
                f"Create Snowflake LS '{snowflake_ls_name}' with a valid JDBC-like connection string."
            ))
    except Exception as e:
        report.append(_error("linked_services", f"List failed: {e}"))

    # --------- datasets ----------
    try:
        ds_names = {ds.name for ds in adf_client.datasets.list_by_factory(resource_group, factory_name)}
        if source_dataset_name in ds_names:
            report.append(_ok("source_dataset", {"name": source_dataset_name}))
        else:
            report.append(_missing(
                "source_dataset",
                f"Create dataset '{source_dataset_name}' (Blob CSV) referencing '{blob_ls_name}'."
            ))
        if sink_dataset_name in ds_names:
            report.append(_ok("sink_dataset", {"name": sink_dataset_name}))
        else:
            report.append(_missing(
                "sink_dataset",
                f"Create dataset '{sink_dataset_name}' (Snowflake table) referencing '{snowflake_ls_name}'."
            ))
    except Exception as e:
        report.append(_error("datasets", f"List failed: {e}"))

    return _finish(report)

# ----------------------------- auto-fix -----------------------------

def auto_fix_prereqs(context: Dict[str, Any], initial_report: Dict[str, Any]) -> Tuple[bool, List[Dict[str, Any]]]:
    """
    Tries to auto-create missing Linked Services and Datasets if enough context is provided.
    Returns (fixed_any, actions[]) where each action is {item, action, status, details|error}.
    Never guesses object names; skips and reports missing input instead.
    """
    subscription_id = context.get("subscription_id") or os.getenv("AZURE_SUBSCRIPTION_ID")
    resource_group  = context.get("resource_group")  or os.getenv("AZURE_RESOURCE_GROUP")
    factory_name    = context.get("factory_name")    or os.getenv("AZURE_FACTORY_NAME")

    storage_account_name = context.get("storage_account_name") or os.getenv("STORAGE_ACCOUNT_NAME")
    storage_account_key  = context.get("storage_account_key")  or os.getenv("STORAGE_ACCOUNT_KEY")
    container_name       = context.get("container") or os.getenv("BLOB_CONTAINER")
    blob_name            = context.get("blob_name") or os.getenv("BLOB_NAME")

    blob_ls_name        = context.get("blob_ls_name") or os.getenv("ADF_BLOB_LINKED_SERVICE") or BLOB_LS_DEFAULT
    snowflake_ls_name   = context.get("snowflake_ls_name") or os.getenv("ADF_SNOWFLAKE_LINKED_SERVICE") or SNOWFLAKE_LS_DEFAULT
    source_dataset_name = context.get("source_dataset_name") or SRC_DS_DEFAULT
    sink_dataset_name   = context.get("sink_dataset_name") or SNK_DS_DEFAULT

    snowflake_conn_str  = context.get("snowflake_connection_string") or os.getenv("SNOWFLAKE_CONNECTION_STRING")
    snowflake_schema    = context.get("snowflake_schema") or os.getenv("SNOWFLAKE_SCHEMA")
    snowflake_table     = context.get("snowflake_table")  or os.getenv("SNOWFLAKE_TABLE")

    actions: List[Dict[str, Any]] = []
    fixed_any = False

    # --- Hard prerequisite sanity: without these we cannot call ADF at all ---
    missing_bootstrap = [k for k, v in [
        ("subscription_id", subscription_id),
        ("resource_group",  resource_group),
        ("factory_name",    factory_name),
    ] if not v]
    if missing_bootstrap:
        actions.append({
            "item": "bootstrap",
            "action": "verify_context",
            "status": "skipped",
            "error": f"Missing: {', '.join(missing_bootstrap)}"
        })
        return False, actions

    # Build client
    try:
        credential = ChainedTokenCredential(
            DefaultAzureCredential(exclude_cli_credential=False),
            InteractiveBrowserCredential()
        )
        adf_client = DataFactoryManagementClient(credential, subscription_id)
    except Exception as e:
        actions.append({"item": "credentials", "action": "init_clients", "status": "error", "error": str(e)})
        return False, actions

    # Helper: what was missing according to initial_report
    missing_items = {i["item"] for i in initial_report.get("summary", {}).get("missing", [])}

    # 1) Blob Linked Service
    if "blob_linked_service" in missing_items:
        if storage_account_name and storage_account_key:
            conn_str = (
                f"DefaultEndpointsProtocol=https;"
                f"AccountName={storage_account_name};"
                f"AccountKey={storage_account_key};"
                f"EndpointSuffix=core.windows.net"
            )
            try:
                adf_client.linked_services.create_or_update(
                    resource_group,
                    factory_name,
                    blob_ls_name,
                    {"properties": {
                        "type": "AzureBlobStorage",
                        "typeProperties": {
                            "connectionString": {"type": "SecureString", "value": conn_str}
                        }
                    }}
                )
                actions.append({"item": "blob_linked_service", "action": "create_or_update", "status": "created",
                                "details": {"name": blob_ls_name}})
                fixed_any = True
            except Exception as e:
                actions.append({"item": "blob_linked_service", "action": "create_or_update", "status": "error", "error": str(e)})
        else:
            miss = []
            if not storage_account_name: miss.append("storage_account_name")
            if not storage_account_key:  miss.append("storage_account_key")
            actions.append({"item": "blob_linked_service", "action": "skipped", "status": "missing_input",
                            "error": f"Missing required inputs: {', '.join(miss) or 'unknown'}"})

    # 2) Snowflake Linked Service (only if connection string provided)
    if "snowflake_linked_service" in missing_items:
        if snowflake_conn_str:
            try:
                adf_client.linked_services.create_or_update(
                    resource_group,
                    factory_name,
                    snowflake_ls_name,
                    {"properties": {
                        "type": "SnowflakeV2",
                        "typeProperties": {
                            "connectionString": {"type": "SecureString", "value": snowflake_conn_str}
                        }
                    }}
                )
                actions.append({"item": "snowflake_linked_service", "action": "create_or_update", "status": "created",
                                "details": {"name": snowflake_ls_name}})
                fixed_any = True
            except Exception as e:
                actions.append({"item": "snowflake_linked_service", "action": "create_or_update", "status": "error", "error": str(e)})
        else:
            actions.append({"item": "snowflake_linked_service", "action": "skipped", "status": "missing_input",
                            "error": "snowflake_connection_string not provided"})

    # 3) Source Dataset (Blob CSV) — no guessing
    if "source_dataset" in missing_items:
        if blob_ls_name and container_name and blob_name:
            try:
                adf_client.datasets.create_or_update(
                    resource_group,
                    factory_name,
                    source_dataset_name,
                    {"properties": {
                        "linkedServiceName": {"referenceName": blob_ls_name, "type": "LinkedServiceReference"},
                        "type": "DelimitedText",
                        "typeProperties": {
                            "location": {
                                "type": "AzureBlobStorageLocation",
                                "container": container_name,
                                "fileName": blob_name
                            },
                            "columnDelimiter": ",",
                            "firstRowAsHeader": True
                        },
                        "schema": []
                    }}
                )
                actions.append({"item": "source_dataset", "action": "create_or_update", "status": "created",
                                "details": {"name": source_dataset_name}})
                fixed_any = True
            except Exception as e:
                actions.append({"item": "source_dataset", "action": "create_or_update", "status": "error", "error": str(e)})
        else:
            missing = []
            if not container_name: missing.append("container")
            if not blob_name:      missing.append("blob_name")
            actions.append({
                "item": "source_dataset",
                "action": "skipped",
                "status": "missing_input",
                "error": f"Missing required inputs: {', '.join(missing) or 'unknown'}"
            })

    # 4) Sink Dataset (Snowflake table) — no guessing
    if "sink_dataset" in missing_items:
        if snowflake_ls_name and snowflake_schema and snowflake_table:
            try:
                adf_client.datasets.create_or_update(
                    resource_group,
                    factory_name,
                    sink_dataset_name,
                    {"properties": {
                        "linkedServiceName": {"referenceName": snowflake_ls_name, "type": "LinkedServiceReference"},
                        "type": "SnowflakeTable",
                        "typeProperties": {
                            "schema": snowflake_schema,
                            "tableName": snowflake_table
                        }
                    }}
                )
                actions.append({"item": "sink_dataset", "action": "create_or_update", "status": "created",
                                "details": {"name": sink_dataset_name}})
                fixed_any = True
            except Exception as e:
                actions.append({"item": "sink_dataset", "action": "create_or_update", "status": "error", "error": str(e)})
        else:
            missing = []
            if not snowflake_ls_name: missing.append("snowflake_ls_name")
            if not snowflake_schema:  missing.append("snowflake_schema")
            if not snowflake_table:   missing.append("snowflake_table")
            actions.append({
                "item": "sink_dataset",
                "action": "skipped",
                "status": "missing_input",
                "error": f"Missing required inputs: {', '.join(missing) or 'unknown'}"
            })

    return fixed_any, actions
