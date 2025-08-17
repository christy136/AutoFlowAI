#!/usr/bin/env bash
set -euo pipefail

API_URL_DEFAULT="http://localhost:5000/precheck"
LOCATION_DEFAULT="westeurope"

# ---------- Helpers ----------
need() { command -v "$1" >/dev/null 2>&1 || { echo "❌ Missing dependency: $1"; exit 1; }; }
say()  { echo -e "$*"; }
ask()  { local p="$1"; local d="${2-}"; read -r -p "$p${d:+ [$d]}: " r || true; echo "${r:-$d}"; }

json_val() { # jq wrapper to pull a field from JSON; args: json key
  echo "$1" | jq -r "$2" 2>/dev/null
}

ensure_rg() {
  local rg="$1" loc="$2"
  if az group show -n "$rg" >/dev/null 2>&1; then
    say "✅ Resource group exists: $rg"
  else
    say "➕ Creating resource group: $rg ($loc)"
    az group create -n "$rg" -l "$loc" >/dev/null
  fi
}

ensure_adf() {
  local rg="$1" adf="$2" loc="$3"
  if az datafactory show -g "$rg" -n "$adf" >/dev/null 2>&1; then
    say "✅ Data Factory exists: $adf"
  else
    say "➕ Creating Data Factory: $adf ($loc)"
    az datafactory create -g "$rg" -n "$adf" -l "$loc" >/dev/null
  fi
}

ensure_storage() {
  local rg="$1" sa="$2" loc="$3"
  if az storage account show -g "$rg" -n "$sa" >/dev/null 2>&1; then
    say "✅ Storage account exists: $sa"
  else
    say "➕ Creating storage account: $sa ($loc)"
    az storage account create -g "$rg" -n "$sa" -l "$loc" --sku Standard_LRS --kind StorageV2 >/dev/null
  fi
}

ensure_container() {
  local sa="$1" key="$2" container="$3"
  if az storage container show --account-name "$sa" --account-key "$key" --name "$container" >/dev/null 2>&1; then
    say "✅ Container exists: $container"
  else
    say "➕ Creating container: $container"
    az storage container create --account-name "$sa" --account-key "$key" --name "$container" >/dev/null
  fi
}

ensure_blob() {
  local sa="$1" key="$2" container="$3" name="$4"
  if az storage blob show --account-name "$sa" --account-key "$key" --container-name "$container" --name "$name" >/dev/null 2>&1; then
    say "✅ Blob exists: $name"
  else
    say "⚠️  Blob '$name' not found in '$container'."
    local path; path=$(ask "Provide local path to upload as '$name' (or press Enter to skip)" "")
    if [[ -n "$path" && -f "$path" ]]; then
      say "⬆️  Uploading $path -> $container/$name"
      az storage blob upload --account-name "$sa" --account-key "$key" --container-name "$container" --file "$path" --name "$name" >/dev/null
      say "✅ Uploaded."
    else
      say "ℹ️  Skipping upload."
    fi
  fi
}

# ---------- Linked Service / Dataset Creation ----------
ensure_blob_linked_service() {
  local rg="$1" adf="$2" ls_name="$3" conn_str="$4"
  # Check existence
  if az datafactory linked-service show -g "$rg" --factory-name "$adf" --linked-service-name "$ls_name" >/dev/null 2>&1; then
    say "✅ Blob Linked Service exists: $ls_name"
    return
  fi
  say "➕ Creating Blob Linked Service: $ls_name"
  az datafactory linked-service create \
    -g "$rg" --factory-name "$adf" --linked-service-name "$ls_name" \
    --properties "{
      \"type\": \"AzureBlobStorage\",
      \"typeProperties\": {
        \"connectionString\": { \"type\": \"SecureString\", \"value\": \"$conn_str\" }
      }
    }" >/dev/null
  say "✅ Created Blob LS: $ls_name"
}

ensure_snowflake_linked_service() {
  local rg="$1" adf="$2" ls_name="$3" snowflake_cs="$4"
  if az datafactory linked-service show -g "$rg" --factory-name "$adf" --linked-service-name "$ls_name" >/dev/null 2>&1; then
    say "✅ Snowflake Linked Service exists: $ls_name"
    return
  fi
  if [[ -z "$snowflake_cs" ]]; then
    say "⚠️  Snowflake connection string not provided."
    say "    Expected JDBC-like key=value; pairs, e.g.:"
    say "    account=...;user=...;password=...;warehouse=...;db=...;schema=...;role=..."
    snowflake_cs=$(ask "Enter Snowflake connection string (leave empty to skip)")
    if [[ -z "$snowflake_cs" ]]; then
      say "⏭️  Skipping Snowflake LS creation."
      return
    fi
  fi
  say "➕ Creating Snowflake Linked Service: $ls_name"
  az datafactory linked-service create \
    -g "$rg" --factory-name "$adf" --linked-service-name "$ls_name" \
    --properties "{
      \"type\": \"SnowflakeV2\",
      \"typeProperties\": {
        \"connectionString\": { \"type\": \"SecureString\", \"value\": \"$snowflake_cs\" }
      }
    }" >/dev/null
  say "✅ Created Snowflake LS: $ls_name"
}

ensure_source_dataset() {
  local rg="$1" adf="$2" ds_name="$3" blob_ls="$4" container="$5" file_name="$6"
  if az datafactory dataset show -g "$rg" --factory-name "$adf" --name "$ds_name" >/dev/null 2>&1; then
    say "✅ Source dataset exists: $ds_name"
    return
  fi
  say "➕ Creating Source dataset (DelimitedText): $ds_name"
  az datafactory dataset create \
    --resource-group "$rg" --factory-name "$adf" --name "$ds_name" \
    --properties "{
      \"linkedServiceName\": { \"referenceName\": \"$blob_ls\", \"type\": \"LinkedServiceReference\" },
      \"type\": \"DelimitedText\",
      \"typeProperties\": {
        \"location\": {
          \"type\": \"AzureBlobStorageLocation\",
          \"container\": \"$container\",
          \"fileName\": \"$file_name\"
        },
        \"columnDelimiter\": \",\",
        \"firstRowAsHeader\": true
      },
      \"schema\": []
    }" >/dev/null
  say "✅ Created Source dataset: $ds_name"
}

ensure_sink_dataset() {
  local rg="$1" adf="$2" ds_name="$3" snowflake_ls="$4" schema_name="$5" table_name="$6"
  if az datafactory dataset show -g "$rg" --factory-name "$adf" --name "$ds_name" >/dev/null 2>&1; then
    say "✅ Sink dataset exists: $ds_name"
    return
  fi
  say "➕ Creating Sink dataset (SnowflakeTable): $ds_name"
  az datafactory dataset create \
    --resource-group "$rg" --factory-name "$adf" --name "$ds_name" \
    --properties "{
      \"linkedServiceName\": { \"referenceName\": \"$snowflake_ls\", \"type\": \"LinkedServiceReference\" },
      \"type\": \"SnowflakeTable\",
      \"typeProperties\": {
        \"schema\": \"$schema_name\",
        \"tableName\": \"$table_name\"
      }
    }" >/dev/null
  say "✅ Created Sink dataset: $ds_name"
}

# ---------- Main ----------
need az
if ! command -v jq >/dev/null 2>&1; then
  say "ℹ️  jq not found; responses will not be pretty-printed."
fi

if ! az account show >/dev/null 2>&1; then
  say "🔐 Logging into Azure (device code)…"
  az login --use-device-code >/dev/null
fi

# Providers (best-effort)
az provider register --namespace Microsoft.Storage >/dev/null 2>&1 || true
az provider register --namespace Microsoft.DataFactory >/dev/null 2>&1 || true

SUBSCRIPTION_ID=$(az account show --query id -o tsv)

API_URL=$(ask "API URL for /precheck" "$API_URL_DEFAULT")
RG_NAME=$(ask "Resource Group" "AutoFlowRG")
LOCATION=$(ask "Azure location for RG/ADF (if created)" "$LOCATION_DEFAULT")
ensure_rg "$RG_NAME" "$LOCATION"

# ADF
ADF_NAME=$(ask "Data Factory name" "AutoFlowADF")
ensure_adf "$RG_NAME" "$ADF_NAME" "$LOCATION"

# Storage
SA_NAME=$(ask "Storage account name" "autoflowstorage9876")
ensure_storage "$RG_NAME" "$SA_NAME" "$LOCATION"
SA_KEY=$(az storage account keys list -g "$RG_NAME" -n "$SA_NAME" --query "[0].value" -o tsv)

CONTAINER=$(ask "Blob container name" "adf-container")
ensure_container "$SA_NAME" "$SA_KEY" "$CONTAINER"

BLOB_NAME=$(ask "Blob name" "sales.csv")
ensure_blob "$SA_NAME" "$SA_KEY" "$CONTAINER" "$BLOB_NAME"

# ADF resource names
BLOB_LS_NAME=$(ask "Blob Linked Service name" "AzureBlobStorageLinkedService")
SNOWFLAKE_LS_NAME=$(ask "Snowflake Linked Service name" "Snowflake_LS")
SRC_DS_NAME=$(ask "Source dataset name" "SourceDataset")
SNK_DS_NAME=$(ask "Sink dataset name" "SinkDataset")
SNOWFLAKE_SCHEMA=$(ask "Snowflake schema for sink dataset" "finance")
SNOWFLAKE_TABLE=$(ask "Snowflake table for sink dataset" "daily_sales")

# Blob LS connection string (build from account/key)
BLOB_CONN_STR="DefaultEndpointsProtocol=https;AccountName=${SA_NAME};AccountKey=${SA_KEY};EndpointSuffix=core.windows.net"

# Optional Snowflake connection string from env or prompt
SNOWFLAKE_CS="${SNOWFLAKE_CS-}"

# 1) Run precheck first
PAYLOAD_FILE="precheck_payload.json"
cat > "$PAYLOAD_FILE" <<JSON
{
  "context": {
    "subscription_id": "$SUBSCRIPTION_ID",
    "resource_group": "$RG_NAME",
    "factory_name": "$ADF_NAME",
    "storage_account_name": "$SA_NAME",
    "storage_account_key": "$SA_KEY",
    "container": "$CONTAINER",
    "blob_name": "$BLOB_NAME",
    "blob_ls_name": "$BLOB_LS_NAME",
    "snowflake_ls_name": "$SNOWFLAKE_LS_NAME",
    "source_dataset_name": "$SRC_DS_NAME",
    "sink_dataset_name": "$SNK_DS_NAME"
  }
}
JSON

say "🌐 Calling /precheck (phase 1)…"
PRE1=$(curl -s -X POST "$API_URL" -H "Content-Type: application/json" --data @"$PAYLOAD_FILE")
[[ -n "${PRE1:-}" ]] || { say "❌ /precheck returned empty response"; exit 1; }
command -v jq >/dev/null 2>&1 && echo "$PRE1" | jq || echo "$PRE1"

# 2) Auto-create missing items (Blob LS, Snowflake LS, Source/Sink datasets)
# Determine which are missing
MISSING_ITEMS=$(json_val "$PRE1" '.summary.missing[]?.item' || true)

for item in $MISSING_ITEMS; do
  case "$item" in
    blob_linked_service)
      ensure_blob_linked_service "$RG_NAME" "$ADF_NAME" "$BLOB_LS_NAME" "$BLOB_CONN_STR"
      ;;
    snowflake_linked_service)
      ensure_snowflake_linked_service "$RG_NAME" "$ADF_NAME" "$SNOWFLAKE_LS_NAME" "$SNOWFLAKE_CS"
      ;;
    source_dataset)
      ensure_source_dataset "$RG_NAME" "$ADF_NAME" "$SRC_DS_NAME" "$BLOB_LS_NAME" "$CONTAINER" "$BLOB_NAME"
      ;;
    sink_dataset)
      ensure_sink_dataset "$RG_NAME" "$ADF_NAME" "$SNK_DS_NAME" "$SNOWFLAKE_LS_NAME" "$SNOWFLAKE_SCHEMA" "$SNOWFLAKE_TABLE"
      ;;
    *)
      say "ℹ️  Skipping unsupported missing item: $item"
      ;;
  esac
done

# 3) Re-run precheck to confirm
say "🌐 Calling /precheck (phase 2, after auto-fix)…"
PRE2=$(curl -s -X POST "$API_URL" -H "Content-Type: application/json" --data @"$PAYLOAD_FILE")
command -v jq >/dev/null 2>&1 && echo "$PRE2" | jq || echo "$PRE2"

say "✅ Done."
