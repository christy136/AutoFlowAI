# -*- coding: utf-8 -*-
"""
ADF pipeline JSON generator.

Inputs:
  - config: dict produced by the LLM (schema-validated before calling this)
    {
      "pipeline_type": "adf",
      "name": "CopyBlobToSnowflake",         # optional
      "source": {
        "type": "blob",                      # e.g., "blob", "json", "adls"
        "path": "adf-container/sales.csv",   # informational (datasets use ctx)
        "linked_service": "AzureBlobStorageLinkedService",
        "format": "csv"                      # optional, not enforced here
      },
      "transformation": [ ... ],             # optional, ignored by this stub
      "sink": {
        "type": "snowflake",                 # e.g., "snowflake", "sql", "blob"
        "table": "finance.daily_sales",
        "linked_service": "Snowflake_LS"
      },
      "schedule": "once"                     # e.g., "once", "daily@01:00"
    }

Output:
  - Minimal ADF pipeline body ready for create_or_update in ADF SDK.
  - Uses dataset reference names "SourceDataset" & "SinkDataset" by default,
    so keep these consistent across deploy_* helpers.
"""

from __future__ import annotations
import json
import os
import re
from datetime import datetime
from typing import Dict, Any

# Keep these aligned with dataset creation in deploy_pipeline.py
DEFAULT_SOURCE_DATASET = "SourceDataset"
DEFAULT_SINK_DATASET = "SinkDataset"


def _sanitize_name(name: str, fallback: str = "CopyPipeline") -> str:
    """
    ADF names must be alphanumeric, '_' or '-' (no spaces, no leading digits in some cases).
    This keeps it simple and safe.
    """
    if not name:
        return fallback
    # Replace spaces with '_' and drop illegal chars
    name = re.sub(r"\s+", "_", name.strip())
    name = re.sub(r"[^A-Za-z0-9_\-]", "", name)
    return name or fallback


def _map_source_block_type(source_type: str) -> str:
    """
    Copy activity 'source' type mapping for ADF.
    This is NOT the dataset type; it is the activity's typeProperties.source.type.
    """
    s = (source_type or "").lower()
    if "blob" in s or "azureblob" in s:
        return "BlobSource"
    if "adls" in s or "datalake" in s:
        return "AzureDataLakeStoreSource"  # coarse fallback
    if "json" in s:
        return "JsonSource"
    # Fallback to BlobSource; safer for file-based sources
    return "BlobSource"


def _map_sink_block_type(sink_type: str) -> str:
    """
    Copy activity 'sink' type mapping for ADF.
    IMPORTANT: Snowflake must be 'SnowflakeSink', not 'SqlSink'.
    """
    s = (sink_type or "").lower()
    if "snowflake" in s or "sf" in s:
        return "SnowflakeSink"
    if "sql" in s or "synapse" in s or "azuresql" in s:
        return "SqlSink"
    if "blob" in s or "azureblob" in s:
        return "BlobSink"
    # Reasonable default
    return "BlobSink"


def create_copy_activity_pipeline(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Builds a minimal, valid ADF pipeline JSON structure for a single Copy activity.
    Datasets are referenced by name; ensure they exist before deployment.
    """
    source_cfg = (config or {}).get("source", {}) or {}
    sink_cfg = (config or {}).get("sink", {}) or {}
    schedule = (config or {}).get("schedule", "once") or "once"

    pipeline_name = _sanitize_name(config.get("name", "CopyPipeline"))
    source_type = source_cfg.get("type", "")
    sink_type = sink_cfg.get("type", "")

    source_ds = source_cfg.get("dataset_name", DEFAULT_SOURCE_DATASET)
    sink_ds = sink_cfg.get("dataset_name", DEFAULT_SINK_DATASET)

    source_block_type = _map_source_block_type(source_type)
    sink_block_type = _map_sink_block_type(sink_type)

    pipeline = {
        "name": pipeline_name,
        "properties": {
            "activities": [
                {
                    "name": "CopyActivity",
                    "type": "Copy",
                    "policy": {
                        # conservative defaults; tune as needed
                        "timeout": "7.00:00:00",
                        "retry": 2,
                        "retryIntervalInSeconds": 30,
                        "secureOutput": False,
                        "secureInput": False
                    },
                    "inputs": [
                        {"referenceName": source_ds, "type": "DatasetReference"}
                    ],
                    "outputs": [
                        {"referenceName": sink_ds, "type": "DatasetReference"}
                    ],
                    "typeProperties": {
                        "source": {
                            "type": source_block_type
                        },
                        "sink": {
                            "type": sink_block_type
                        }
                    }
                }
            ],
            # Non-breaking place to stash run-intent metadata for your framework
            "annotations": [
                {"autoflow:schedule": schedule},
                {"autoflow:generated_at": datetime.utcnow().isoformat() + "Z"}
            ]
            # NOTE: We intentionally do NOT add non-standard top-level properties
            # that ADF might reject (e.g., custom 'runtimeConfiguration').
        }
    }

    return pipeline


def save_pipeline_to_file(pipeline_json: Dict[str, Any],
                          path: str = "output",
                          pipeline_name: str | None = None) -> str:
    os.makedirs(path, exist_ok=True)
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    name = pipeline_name or pipeline_json.get("name", "GeneratedPipeline")
    safe_name = _sanitize_name(name, "GeneratedPipeline")
    file_path = os.path.join(path, f"{safe_name}_{stamp}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(pipeline_json, f, indent=4)
    return file_path
