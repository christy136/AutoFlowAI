# utils/env_generator.py
# -*- coding: utf-8 -*-
import json, os, tempfile
from pathlib import Path
from typing import Dict, Tuple

TEMPLATE_KEYS = [
    ("FLASK_DEBUG", "false"),
    ("PORT", "5000"),
    ("AZURE_SUBSCRIPTION_ID", ""),
    ("AZURE_RESOURCE_GROUP", ""),
    ("AZURE_FACTORY_NAME", ""),
    ("STORAGE_ACCOUNT_NAME", ""),
    ("STORAGE_ACCOUNT_KEY", ""),  # leave blank if using MI
    ("BLOB_CONTAINER", ""),
    ("BLOB_NAME", ""),
    ("ADF_BLOB_LINKED_SERVICE", "AzureBlobStorageLinkedService"),
    ("ADF_SNOWFLAKE_LINKED_SERVICE", "Snowflake_LS"),
    ("SNOWFLAKE_CONNECTION_STRING", ""),
    ("SNOWFLAKE_SCHEMA", ""),
    ("SNOWFLAKE_TABLE", ""),
    ("OPENROUTER_API_KEY", ""),
    ("YOUR_SITE_URL", ""),
    ("YOUR_SITE_NAME", "")
]

SENSITIVE_KEYS = {"STORAGE_ACCOUNT_KEY", "OPENROUTER_API_KEY", "SNOWFLAKE_CONNECTION_STRING"}

def _load_json(path: Path) -> Dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))

def _coalesce(*vals):
    for v in vals:
        if v is not None and str(v) != "":
            return v
    return ""

def _atomic_write(path: Path, data: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=path.name, dir=str(path.parent))
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(data)
    os.replace(tmp, path)

def redact_env_map(env_map: Dict[str, str]) -> Dict[str, str]:
    redacted = {}
    for k, v in env_map.items():
        if k in SENSITIVE_KEYS and v:
            redacted[k] = v[:4] + "…" + v[-4:] if len(v) > 8 else "•••"
        else:
            redacted[k] = v
    return redacted

def generate_env_from_profiles(account_path: str,
                               usecase_path: str,
                               out_path: str = ".env") -> Tuple[str, Dict[str, str]]:
    """
    Merge account + usecase profiles with process env and write .env atomically.
    Returns (output_path, written_map).
    """
    acct = _load_json(Path(account_path))
    uc   = _load_json(Path(usecase_path))
    env  = os.environ  # CI/local env secrets win

    values = {
        "FLASK_DEBUG": _coalesce(env.get("FLASK_DEBUG"), "false"),
        "PORT": _coalesce(env.get("PORT"), "5000"),

        "AZURE_SUBSCRIPTION_ID": _coalesce(env.get("AZURE_SUBSCRIPTION_ID"), acct.get("subscription_id")),
        "AZURE_RESOURCE_GROUP":  _coalesce(env.get("AZURE_RESOURCE_GROUP"),  acct.get("resource_group")),
        "AZURE_FACTORY_NAME":    _coalesce(env.get("AZURE_FACTORY_NAME"),    acct.get("factory_name")),

        "STORAGE_ACCOUNT_NAME":  _coalesce(env.get("STORAGE_ACCOUNT_NAME"),  acct.get("storage_account_name")),
        "STORAGE_ACCOUNT_KEY":   _coalesce(env.get("STORAGE_ACCOUNT_KEY")),  # never store in JSON

        "BLOB_CONTAINER": _coalesce(env.get("BLOB_CONTAINER"), uc.get("blob_container")),
        "BLOB_NAME":      _coalesce(env.get("BLOB_NAME"),      uc.get("blob_name")),

        "ADF_BLOB_LINKED_SERVICE":     _coalesce(env.get("ADF_BLOB_LINKED_SERVICE"), "AzureBlobStorageLinkedService"),
        "ADF_SNOWFLAKE_LINKED_SERVICE": _coalesce(env.get("ADF_SNOWFLAKE_LINKED_SERVICE"), "Snowflake_LS"),

        "SNOWFLAKE_CONNECTION_STRING": _coalesce(env.get("SNOWFLAKE_CONNECTION_STRING")),  # prefer KV in prod
        "SNOWFLAKE_SCHEMA":            _coalesce(env.get("SNOWFLAKE_SCHEMA"), uc.get("snowflake_schema")),
        "SNOWFLAKE_TABLE":             _coalesce(env.get("SNOWFLAKE_TABLE"),  uc.get("snowflake_table")),

        "OPENROUTER_API_KEY": _coalesce(env.get("OPENROUTER_API_KEY")),
        "YOUR_SITE_URL":      _coalesce(env.get("YOUR_SITE_URL")),
        "YOUR_SITE_NAME":     _coalesce(env.get("YOUR_SITE_NAME")),
    }

    # render .env
    lines = [f"{k}={values.get(k, d)}" for k, d in TEMPLATE_KEYS]
    content = "\n".join(lines) + "\n"
    _atomic_write(Path(out_path), content)
    return out_path, values
