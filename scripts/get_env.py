# -*- coding: utf-8 -*-
import argparse, json, os, sys
from pathlib import Path

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

def load_json(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        return {}
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

def coalesce(*vals):
    for v in vals:
        if v is not None and str(v) != "":
            return v
    return ""

def main():
    ap = argparse.ArgumentParser(description="Generate .env from profiles.")
    ap.add_argument("--account", required=True, help="profiles/account-*.json")
    ap.add_argument("--usecase", required=True, help="profiles/usecase-*.json")
    ap.add_argument("--out", default=".env", help="output .env path")
    args = ap.parse_args()

    acct = load_json(args.account)
    uc = load_json(args.usecase)

    # env fallbacks (so CI/CD can override)
    env = os.environ

    values = {
        "FLASK_DEBUG": coalesce(env.get("FLASK_DEBUG"), "false"),
        "PORT": coalesce(env.get("PORT"), "5000"),

        "AZURE_SUBSCRIPTION_ID": coalesce(env.get("AZURE_SUBSCRIPTION_ID"), acct.get("subscription_id")),
        "AZURE_RESOURCE_GROUP":  coalesce(env.get("AZURE_RESOURCE_GROUP"), acct.get("resource_group")),
        "AZURE_FACTORY_NAME":    coalesce(env.get("AZURE_FACTORY_NAME"), acct.get("factory_name")),

        "STORAGE_ACCOUNT_NAME":  coalesce(env.get("STORAGE_ACCOUNT_NAME"), acct.get("storage_account_name")),
        "STORAGE_ACCOUNT_KEY":   coalesce(env.get("STORAGE_ACCOUNT_KEY")),  # never put in JSON profiles
        "BLOB_CONTAINER":        coalesce(env.get("BLOB_CONTAINER"), uc.get("blob_container")),
        "BLOB_NAME":             coalesce(env.get("BLOB_NAME"), uc.get("blob_name")),

        "ADF_BLOB_LINKED_SERVICE":    coalesce(env.get("ADF_BLOB_LINKED_SERVICE"), "AzureBlobStorageLinkedService"),
        "ADF_SNOWFLAKE_LINKED_SERVICE": coalesce(env.get("ADF_SNOWFLAKE_LINKED_SERVICE"), "Snowflake_LS"),

        "SNOWFLAKE_CONNECTION_STRING": coalesce(env.get("SNOWFLAKE_CONNECTION_STRING")),
        "SNOWFLAKE_SCHEMA":            coalesce(env.get("SNOWFLAKE_SCHEMA"), uc.get("snowflake_schema")),
        "SNOWFLAKE_TABLE":             coalesce(env.get("SNOWFLAKE_TABLE"), uc.get("snowflake_table")),

        "OPENROUTER_API_KEY": coalesce(env.get("OPENROUTER_API_KEY")),
        "YOUR_SITE_URL":      coalesce(env.get("YOUR_SITE_URL")),
        "YOUR_SITE_NAME":     coalesce(env.get("YOUR_SITE_NAME"))
    }

    # Write .env
    out = Path(args.out)
    with out.open("w", encoding="utf-8") as f:
        for key, default in TEMPLATE_KEYS:
            f.write(f"{key}={values.get(key, default)}\n")

    print(f"✅ Wrote {out.resolve()}")

if __name__ == "__main__":
    sys.exit(main())
