#!/usr/bin/env python3
import json, os, shutil, subprocess, sys
from pathlib import Path

PROFILES_DIR = Path("profiles")
PROFILES_DIR.mkdir(exist_ok=True)

def has_az():
    try:
        subprocess.run(["az","account","show","-o","none"], check=False,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return True
    except Exception:
        return False

def prompt(msg, default=""):
    v = input(f"{msg} [{default}]: ").strip()
    return v or default

def discover_subscription():
    try:
        out = subprocess.check_output(["az","account","show","-o","tsv","--query","id"]).decode().strip()
        return out
    except Exception:
        return ""
def discover_rg():
    try:
        out = subprocess.check_output(["az","group","list","-o","tsv","--query","[0].name"]).decode().strip()
        return out
    except Exception:
        return ""
def discover_storage():
    try:
        out = subprocess.check_output(["az","storage","account","list","-o","tsv","--query","[0].name"]).decode().strip()
        return out
    except Exception:
        return ""
def discover_adf(rg):
    try:
        out = subprocess.check_output([
            "az","datafactory","factory","list","-g", rg, "-o","tsv","--query","[0].name"
        ]).decode().strip()
        return out
    except Exception:
        return ""

def write_json(path: Path, data: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(f"✅ wrote {path}")

def main():
    print("=== AutoFlowAI Profile Wizard ===")
    preset = prompt("Account preset name (e.g., dev/prod)", "dev")
    usecase = prompt("Use-case name (e.g., blob2sf-sales)", "blob2sf-sales")

    use_az = has_az()
    if use_az:
        print("🔎 Azure CLI detected — attempting discovery…")

    sub_id   = discover_subscription() if use_az else ""
    rg       = discover_rg() if use_az else ""
    storage  = discover_storage() if use_az else ""
    factory  = discover_adf(rg) if (use_az and rg) else ""

    # Confirm/override interactively
    sub_id  = prompt("Azure Subscription ID", sub_id or "YOUR-SUB-ID")
    rg      = prompt("Resource Group", rg or "AutoFlowRG")
    factory = prompt("Data Factory name", factory or "AutoFlowADF")
    storage = prompt("Storage account name", storage or "autoflowstorage9876")

    container = prompt("Blob container (use-case)", "adf-container")
    blob      = prompt("Blob name (use-case)", "sales.csv")
    schema    = prompt("Snowflake schema (use-case)", "finance")
    table     = prompt("Snowflake table (use-case)", "daily_sales")

    acct_json = {
        "subscription_id": sub_id,
        "resource_group": rg,
        "factory_name": factory,
        "storage_account_name": storage
    }
    uc_json = {
        "blob_container": container,
        "blob_name": blob,
        "snowflake_schema": schema,
        "snowflake_table": table
    }

    acct_path = PROFILES_DIR / f"account-{preset}.json"
    uc_path   = PROFILES_DIR / f"usecase-{usecase}.json"

    write_json(acct_path, acct_json)
    write_json(uc_path, uc_json)

    print("\nNext:")
    print(f"  python scripts/get_env.py --account {acct_path} --usecase {uc_path} --out .env")

    # Optional: run it now
    run_now = prompt("Generate .env now from these profiles? (y/N)", "N")
    if run_now.lower().startswith("y"):
        try:
            cmd = [sys.executable, "scripts/get_env.py", "--account", str(acct_path), "--usecase", str(uc_path), "--out", ".env"]
            subprocess.run(cmd, check=True)
            print("✅ .env generated.")
        except Exception as e:
            print(f"⚠️ Failed to generate .env: {e}")

if __name__ == "__main__":
    sys.exit(main())
