# -*- coding: utf-8 -*-
"""
AutoFlowAI Flask application
- LLM -> Config (schema-validated) -> Precheck (auto-fix) -> Generate ADF JSON -> Validate -> Save -> Deploy
- UI-driven profiles persist (account + use-case), automatic .env generation & hot-reload
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Dict, Any, Tuple

from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from dotenv import load_dotenv
from jsonschema import validate as js_validate, ValidationError
from dotenv import set_key, dotenv_values, load_dotenv

# ---- LLM & Pipeline Modules ----
from llm_clients.openrouter_client import generate_with_openrouter
from pipeline_generator.adf_generator import (
    create_copy_activity_pipeline,
    save_pipeline_to_file,
)
from pipeline_generator.deploy_simulator import validate_pipeline_hooks
from pipeline_generator.deploy_pipeline import deploy_to_adf
from pipeline_generator.prereq_checker import (
    check_prerequisites,
    auto_fix_prereqs,
)

# ---- Utilities ----
from utils.auto_corrector import auto_correct_json
from utils.error_classifier import classify_error
from utils.logger import log_error
from utils.env_generator import generate_env_from_profiles, redact_env_map

# -------------------- App & Config --------------------
ROOT = Path(__file__).resolve().parent
SCHEMA_PATH = ROOT / "schemas" / "adf_pipeline_schema.json"
PROFILES_DIR = ROOT / "profiles"
ACTIVE_PATH = PROFILES_DIR / "active.json"

app = Flask(__name__)
CORS(app)  # enable cross-origin for dev UI
load_dotenv()  # initial load; we hot-reload after profile saves

# Load LLM config schema once
with SCHEMA_PATH.open("r", encoding="utf-8") as f:
    ADF_CONFIG_SCHEMA: Dict[str, Any] = json.load(f)

DEFAULTS = {
    "blob_ls": os.getenv("ADF_BLOB_LINKED_SERVICE", "AzureBlobStorageLinkedService"),
    "snowflake_ls": os.getenv("ADF_SNOWFLAKE_LINKED_SERVICE", "Snowflake_LS"),
    "source_dataset": "SourceDataset",
    "sink_dataset": "SinkDataset",
}

# -------------------- Helpers --------------------
def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")

def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))

def build_dynamic_prompt(user_input: str, context: dict) -> str:
    """
    Keep prompt aligned with schema: schedule is a STRING (e.g., "once" or "daily@01:00")
    """
    schedule = context.get("schedule", "once")
    return f"""
Return only a valid JSON object (no markdown, no comments).
Required fields:
- pipeline_type (must be "adf")
- source: type, path, linked_service
- sink:   type, table, linked_service
- schedule: string (e.g., "once", "daily@01:00")
Optional:
- name
- transformation: array of objects

User Request: "{user_input}"

Context:
- Source Path: {context.get("source_path", "unknown")}
- Target Table: {context.get("snowflake_table", "unknown")}
- Schedule: {schedule}
""".strip()

def normalize_schedule_in_config(cfg: dict) -> dict:
    """
    If LLM returns schedule as an object, flatten to a string to match the schema,
    e.g., {"frequency":"daily","time":"01:00"} -> "daily@01:00"
    """
    val = cfg.get("schedule")
    if isinstance(val, dict):
        freq = (val.get("frequency") or "once").strip()
        tm = (val.get("time") or "").strip()
        cfg["schedule"] = f"{freq}@{tm}" if tm else freq
    return cfg

def call_llm_and_parse(requirement: str, ctx: dict) -> dict:
    """
    1) Build prompt
    2) Call LLM (OpenRouter)
    3) Auto-correct JSON fences/trailing commas
    4) Parse & schema-validate
    5) Default linked service names if missing
    """
    prompt = build_dynamic_prompt(requirement, ctx)
    raw = generate_with_openrouter(prompt)
    if not raw:
        raise RuntimeError("Empty response from LLM")

    corrected = auto_correct_json(raw)  # strips ```json fences, trailing commas, newlines
    try:
        cfg = json.loads(corrected)
    except json.JSONDecodeError as e:
        raise ValueError(f"LLM JSON parse failed: {e}") from e

    cfg = normalize_schedule_in_config(cfg)

    try:
        js_validate(instance=cfg, schema=ADF_CONFIG_SCHEMA)
    except ValidationError as ve:
        raise ValueError(f"Config schema invalid: {ve.message}") from ve

    # Ensure LS names present (keep consistent with dataset creation)
    cfg.setdefault("source", {}).setdefault("linked_service", DEFAULTS["blob_ls"])
    cfg.setdefault("sink", {}).setdefault("linked_service", DEFAULTS["snowflake_ls"])
    return cfg

def merge_context(payload_ctx: dict) -> dict:
    """
    Build final context with 3-level precedence:
      1) Request payload 'context' (highest)
      2) Live environment (.env) loaded via load_dotenv()
      3) Active profiles (profiles/account-*.json + profiles/usecase-*.json)
    Secrets are NEVER sourced from profiles.
    """
    ctx = dict(payload_ctx or {})

    # Load active profile names
    active = _read_json(ACTIVE_PATH)
    acct_path = PROFILES_DIR / f"account-{active.get('account','')}.json"
    uc_path = PROFILES_DIR / f"usecase-{active.get('usecase','')}.json"
    acct = _read_json(acct_path) if acct_path.exists() else {}
    uc = _read_json(uc_path) if uc_path.exists() else {}

    # Coalesce: if not in ctx, take env, else profiles
    ctx.setdefault("subscription_id", os.getenv("AZURE_SUBSCRIPTION_ID") or acct.get("subscription_id"))
    ctx.setdefault("resource_group",  os.getenv("AZURE_RESOURCE_GROUP")  or acct.get("resource_group"))
    ctx.setdefault("factory_name",    os.getenv("AZURE_FACTORY_NAME")    or acct.get("factory_name"))

    ctx.setdefault("storage_account_name", os.getenv("STORAGE_ACCOUNT_NAME") or acct.get("storage_account_name"))
    ctx.setdefault("storage_account_key",  os.getenv("STORAGE_ACCOUNT_KEY"))  # NEVER from profiles

    ctx.setdefault("container", os.getenv("BLOB_CONTAINER") or uc.get("blob_container"))
    ctx.setdefault("blob_name", os.getenv("BLOB_NAME") or uc.get("blob_name"))

    ctx.setdefault("snowflake_schema", os.getenv("SNOWFLAKE_SCHEMA") or uc.get("snowflake_schema"))
    ctx.setdefault("snowflake_table",  os.getenv("SNOWFLAKE_TABLE")  or uc.get("snowflake_table"))

    ctx.setdefault("blob_ls_name",     os.getenv("ADF_BLOB_LINKED_SERVICE") or DEFAULTS["blob_ls"])
    ctx.setdefault("snowflake_ls_name", os.getenv("ADF_SNOWFLAKE_LINKED_SERVICE") or DEFAULTS["snowflake_ls"])

    # Optional: prefer Key Vault in prod; here we allow a connection string from env only
    ctx.setdefault("snowflake_connection_string", os.getenv("SNOWFLAKE_CONNECTION_STRING"))

    return ctx

def error_response(http_code: int, reason: str, extra: dict | None = None):
    payload = {"error": reason}
    if extra:
        payload.update(extra)
    return jsonify(payload), http_code

# -------------------- API: Generate --------------------
@app.route("/generate", methods=["POST"])
def generate():
    data = request.get_json(force=True) or {}
    requirement = data.get("requirement")
    if not requirement:
        return error_response(400, "Missing 'requirement'")

    ctx = merge_context(data.get("context", {}))
    simulate_only = bool(data.get("simulate", False))

    # 1) LLM -> config (schema validated)
    try:
        cfg = call_llm_and_parse(requirement, ctx)
    except Exception as e:
        etype = classify_error(str(e))
        log_error(reason=str(e), error_type=etype, data={"stage": "llm_parse"})
        return error_response(400, "Invalid structured output from LLM", {"reason": str(e)})

    # 2) Precheck + determine inputs needed
    initial = check_prerequisites(ctx)
    missing_items = {i["item"] for i in initial.get("summary", {}).get("missing", [])}
    missing_inputs = {}

    if "snowflake_linked_service" in missing_items and not ctx.get("snowflake_connection_string"):
        missing_inputs["snowflake_connection_string"] = "Provide Snowflake JDBC connection string"
    if "blob_linked_service" in missing_items and not ctx.get("storage_account_key"):
        missing_inputs["storage_account_key"] = f"Provide storage key for {ctx.get('storage_account_name')}"
    if "source_dataset" in missing_items and (not ctx.get("container") or not ctx.get("blob_name")):
        if not ctx.get("container"): missing_inputs["container"] = "Blob container name required"
        if not ctx.get("blob_name"): missing_inputs["blob_name"] = "Blob file name required"
    if "sink_dataset" in missing_items and (not ctx.get("snowflake_schema") or not ctx.get("snowflake_table")):
        if not ctx.get("snowflake_schema"): missing_inputs["snowflake_schema"] = "Snowflake schema required"
        if not ctx.get("snowflake_table"):  missing_inputs["snowflake_table"]  = "Snowflake table required"

    if missing_inputs:
        # Inform UI; do not hard-fail
        return jsonify({
            "status": "blocked",
            "stage": "precheck",
            "initial": initial,
            "missing_inputs": missing_inputs
        }), 200

    # 3) Auto-fix
    fixed_actions: list[dict] = []
    try:
        fixed_any, fixed_actions = auto_fix_prereqs(ctx, initial)
        final = check_prerequisites(ctx) if fixed_any else initial
    except Exception as e:
        log_error(reason=str(e), error_type="precheck_autofix_error", data={"stage": "precheck"})
        return error_response(500, "Precheck auto-fix failed", {"reason": str(e), "initial": initial})

    # 4) Build ADF JSON + validate hooks
    try:
        adf_json = create_copy_activity_pipeline(cfg)
        ok, msg = validate_pipeline_hooks(adf_json)
        if not ok:
            raise ValueError(msg)
    except Exception as e:
        log_error(reason=str(e), error_type="validation_error", data={"stage": "adf_json"})
        return error_response(400, "ADF JSON validation failed", {"reason": str(e)})

    # 5) Save + (optionally) deploy
    try:
        file_path = save_pipeline_to_file(adf_json)
        if simulate_only:
            return jsonify({
                "status": "validated",
                "pipeline": adf_json,
                "saved_to": file_path,
                "autofix_actions": fixed_actions,
                "message": "Validated & saved. Skipped deployment (simulate=True)."
            }), 200

        deploy_result = deploy_to_adf(file_path, cfg, ctx)
        return jsonify({
            "status": "deployed",
            "pipeline": adf_json,
            "saved_to": file_path,
            "autofix_actions": fixed_actions,
            "deploy_result": deploy_result
        }), 200
    except Exception as e:
        log_error(reason=str(e), error_type="deploy_error", data={"stage": "deploy"})
        return error_response(500, "Deployment failed", {"reason": str(e)})

# -------------------- API: Precheck --------------------
@app.route("/precheck", methods=["POST"])
def precheck():
    data = request.get_json(force=True) or {}
    ctx = merge_context(data.get("context", {}))
    do_autofix = bool(data.get("auto_fix", True))

    initial_report = check_prerequisites(ctx)
    missing_items = {i["item"] for i in initial_report.get("summary", {}).get("missing", [])}
    missing_inputs = {}

    if "snowflake_linked_service" in missing_items and not ctx.get("snowflake_connection_string"):
        missing_inputs["snowflake_connection_string"] = "Enter Snowflake JDBC connection string"
    if "blob_linked_service" in missing_items and not ctx.get("storage_account_key"):
        missing_inputs["storage_account_key"] = f"Enter storage account key for {ctx.get('storage_account_name')}"
    if "source_dataset" in missing_items:
        if not ctx.get("container"): missing_inputs["container"] = "Enter blob container name"
        if not ctx.get("blob_name"): missing_inputs["blob_name"] = "Enter blob file name"
    if "sink_dataset" in missing_items:
        if not ctx.get("snowflake_schema"): missing_inputs["snowflake_schema"] = "Enter Snowflake schema"
        if not ctx.get("snowflake_table"):  missing_inputs["snowflake_table"]  = "Enter Snowflake table"

    if missing_inputs and do_autofix:
        return jsonify({
            "status": "missing_inputs",
            "initial": initial_report,
            "missing_inputs": missing_inputs,
            "message": "Provide these inputs and call /precheck again with auto_fix=true"
        }), 200

    actions = []
    final_report = initial_report
    if do_autofix and not missing_inputs:
        fixed_any, actions = auto_fix_prereqs(ctx, initial_report)
        if fixed_any:
            final_report = check_prerequisites(ctx)

    return jsonify({
        "status": "ok",
        "initial": initial_report,
        "autofix_actions": actions,
        "final": final_report
    }), 200

# -------------------- API: Secrets (.env) --------------------
@app.route("/secrets/status", methods=["GET"])
def secrets_status():
    """
    Return a redacted view of secrets stored in .env so the UI can confirm state.
    """
    env_path = ROOT / ".env"
    vals = dotenv_values(str(env_path)) if env_path.exists() else {}
    subset = {
        "OPENROUTER_API_KEY": vals.get("OPENROUTER_API_KEY", ""),
        "STORAGE_ACCOUNT_KEY": vals.get("STORAGE_ACCOUNT_KEY", ""),
        "SNOWFLAKE_CONNECTION_STRING": vals.get("SNOWFLAKE_CONNECTION_STRING", ""),
    }
    return jsonify({"secrets": redact_env_map(subset)}), 200


@app.route("/secrets/save", methods=["POST"])
def secrets_save():
    """
    Persist secrets into .env, then hot-reload process env for immediate use.
    NEVER echoes raw values; returns a redacted preview instead.
    """
    data = request.get_json(force=True) or {}
    env_path = str(ROOT / ".env")
    updated = []

    for key in ("OPENROUTER_API_KEY", "STORAGE_ACCOUNT_KEY", "SNOWFLAKE_CONNECTION_STRING"):
        if data.get(key):
            set_key(env_path, key, data[key])
            updated.append(key)

    # hot-reload .env into the running process
    load_dotenv(override=True)

    # redacted preview
    preview = {k: data.get(k, "") for k in updated}
    return jsonify({"status": "saved", "updated": updated, "preview": redact_env_map(preview)}), 200


@app.route("/ui/secrets", methods=["GET"])
def ui_secrets():
    """
    Render the Secrets UI (templates/secrets.html).
    """
    return render_template("secrets.html")


# -------------------- API: Profiles + .env automation --------------------
@app.route("/profiles/save", methods=["POST"])
def profiles_save():
    """
    Persist account/usecase profiles from the UI (NO SECRETS),
    mark active, generate .env, and hot-reload for the current process.
    """
    data = request.get_json(force=True) or {}
    account = data.get("account", {}) or {}
    usecase = data.get("usecase", {}) or {}
    gen_env = bool(data.get("generate_env", True))

    acct_name = account.get("name") or "dev"
    uc_name = usecase.get("name") or "blob2sf-default"

    acct_path = PROFILES_DIR / f"account-{acct_name}.json"
    uc_path = PROFILES_DIR / f"usecase-{uc_name}.json"

    acct_doc = {
        "subscription_id": account.get("subscription_id", ""),
        "resource_group": account.get("resource_group", ""),
        "factory_name": account.get("factory_name", ""),
        "storage_account_name": account.get("storage_account_name", "")
    }
    uc_doc = {
        "blob_container": usecase.get("blob_container", ""),
        "blob_name": usecase.get("blob_name", ""),
        "snowflake_schema": usecase.get("snowflake_schema", ""),
        "snowflake_table": usecase.get("snowflake_table", "")
    }

    _write_json(acct_path, acct_doc)
    _write_json(uc_path, uc_doc)
    _write_json(ACTIVE_PATH, {"account": acct_name, "usecase": uc_name})

    result = {
        "status": "saved",
        "account_profile": str(acct_path),
        "usecase_profile": str(uc_path),
        "active": {"account": acct_name, "usecase": uc_name},
    }

    if gen_env:
        out_path, env_map = generate_env_from_profiles(str(acct_path), str(uc_path), out_path=str(ROOT / ".env"))
        # Hot-reload .env for current process (dev convenience)
        load_dotenv(override=True)
        result.update({
            "env_written_to": out_path,
            "env_preview": redact_env_map(env_map)  # NEVER return raw secrets
        })

    return jsonify(result), 200

@app.route("/profiles/activate", methods=["POST"])
def profiles_activate():
    """
    Switch to existing profiles (no editing), regenerate .env, hot-reload.
    """
    data = request.get_json(force=True) or {}
    acct_name = data.get("account_name")
    uc_name = data.get("usecase_name")
    if not acct_name or not uc_name:
        return jsonify({"error": "account_name and usecase_name required"}), 400

    acct_path = PROFILES_DIR / f"account-{acct_name}.json"
    uc_path = PROFILES_DIR / f"usecase-{uc_name}.json"
    if not acct_path.exists() or not uc_path.exists():
        return jsonify({"error": "profile files not found"}), 404

    _write_json(ACTIVE_PATH, {"account": acct_name, "usecase": uc_name})
    out_path, env_map = generate_env_from_profiles(str(acct_path), str(uc_path), out_path=str(ROOT / ".env"))
    load_dotenv(override=True)

    return jsonify({
        "status": "activated",
        "active": {"account": acct_name, "usecase": uc_name},
        "env_written_to": out_path,
        "env_preview": redact_env_map(env_map)
    }), 200

@app.route("/profiles/list", methods=["GET"])
def profiles_list():
    """
    List available profiles for UI selectors and show which are active.
    """
    active = _read_json(ACTIVE_PATH)
    accounts = [p.stem.replace("account-", "") for p in PROFILES_DIR.glob("account-*.json")]
    usecases = [p.stem.replace("usecase-", "") for p in PROFILES_DIR.glob("usecase-*.json")]
    return jsonify({"active": active, "accounts": accounts, "usecases": usecases}), 200

# -------------------- Misc --------------------
@app.route("/healthz", methods=["GET"])
def healthz():
    return jsonify({"status": "ok"}), 200

# Optional UI stubs — add templates/templates/precheck.html & generate.html if you use these
@app.route("/ui/precheck")
def ui_precheck():
    return render_template("precheck.html")

@app.route("/ui/generate")
def ui_generate():
    return render_template("generate.html")

# -------------------- Main --------------------
if __name__ == "__main__":
    debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=debug)
