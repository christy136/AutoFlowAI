# -*- coding: utf-8 -*-
"""AutoFlowAI Flask app"""
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from dotenv import load_dotenv
import os, json, re

from jsonschema import validate as js_validate, ValidationError

from llm_clients.openrouter_client import generate_with_openrouter
from pipeline_generator.adf_generator import create_copy_activity_pipeline, save_pipeline_to_file
from pipeline_generator.deploy_simulator import validate_pipeline_hooks
from pipeline_generator.deploy_pipeline import deploy_to_adf
from pipeline_generator.prereq_checker import check_prerequisites, auto_fix_prereqs

from utils.auto_corrector import auto_correct_json
from utils.error_classifier import classify_error
from utils.logger import log_error

# -------------------- App --------------------
app = Flask(__name__)
CORS(app)  # allow UI to call APIs across origins in dev
load_dotenv()

# -------------------- Constants & Schema --------------------
ROOT = os.path.dirname(os.path.abspath(__file__))
SCHEMA_PATH = os.path.join(ROOT, "schemas", "adf_pipeline_schema.json")
with open(SCHEMA_PATH, "r") as f:
    ADF_CONFIG_SCHEMA = json.load(f)

DEFAULTS = {
    "blob_ls": os.getenv("ADF_BLOB_LINKED_SERVICE", "AzureBlobStorageLinkedService"),
    "snowflake_ls": os.getenv("ADF_SNOWFLAKE_LINKED_SERVICE", "Snowflake_LS"),
    "source_dataset": "SourceDataset",
    "sink_dataset": "SinkDataset",
}

# -------------------- Helpers --------------------
def build_dynamic_prompt(user_input: str, context: dict) -> str:
    # NOTE: schema expects schedule as a STRING; keep prompt aligned to avoid mismatch
    schedule = context.get("schedule", "once")  # e.g., "daily@01:00" or "once"
    return f"""
Return only a valid JSON object (no markdown). 
Schema fields (required):
- pipeline_type (must be "adf")
- source: type, path, linked_service
- sink:   type, table, linked_service
- schedule: string (e.g., "once", "daily@01:00")
Optional:
- transformation: array of steps (objects)

Now generate config for this request:
User Request: "{user_input}"

Context:
- Source Path: {context.get("source_path", "unknown")}
- Target Table: {context.get("snowflake_table", "unknown")}
- Schedule: {schedule}
"""

def merge_context(payload_ctx: dict) -> dict:
    """
    Merge incoming context with env fallbacks.
    NEVER read secrets from files here; keep secrets in env or Key Vault.
    """
    ctx = dict(payload_ctx or {})
    # Azure resource identity
    ctx.setdefault("subscription_id", os.getenv("AZURE_SUBSCRIPTION_ID"))
    ctx.setdefault("resource_group", os.getenv("AZURE_RESOURCE_GROUP"))
    ctx.setdefault("factory_name", os.getenv("AZURE_FACTORY_NAME"))
    # Storage
    ctx.setdefault("storage_account_name", os.getenv("STORAGE_ACCOUNT_NAME"))
    ctx.setdefault("storage_account_key", os.getenv("STORAGE_ACCOUNT_KEY"))
    ctx.setdefault("container", os.getenv("BLOB_CONTAINER"))
    ctx.setdefault("blob_name", os.getenv("BLOB_NAME"))
    # Linked service names (keep consistent across the stack)
    ctx.setdefault("blob_ls_name", DEFAULTS["blob_ls"])
    ctx.setdefault("snowflake_ls_name", DEFAULTS["snowflake_ls"])
    # Snowflake (structured; prefer KV in production)
    ctx.setdefault("snowflake_schema", os.getenv("SNOWFLAKE_SCHEMA"))
    ctx.setdefault("snowflake_table", os.getenv("SNOWFLAKE_TABLE"))
    ctx.setdefault("snowflake_connection_string", os.getenv("SNOWFLAKE_CONNECTION_STRING"))
    # UX sugar
    if "source_path" in ctx and not ctx.get("container"):
        # allow callers to pass container/file in one string like "container/folder/file.csv"
        pass
    return ctx

def normalize_schedule_in_config(cfg: dict) -> dict:
    """
    If LLM returns schedule as an object, flatten to string to match schema,
    e.g., {"frequency": "daily", "time": "01:00"} -> "daily@01:00"
    """
    val = cfg.get("schedule")
    if isinstance(val, dict):
        freq = val.get("frequency", "once")
        tm = val.get("time")
        cfg["schedule"] = f"{freq}@{tm}" if tm else freq
    return cfg

def error_response(http_code: int, reason: str, extra: dict = None):
    payload = {"error": reason}
    if extra:
        payload.update(extra)
    return jsonify(payload), http_code

# -------------------- Core LLM Path --------------------
def call_llm_and_parse(requirement: str, ctx: dict) -> dict:
    prompt = build_dynamic_prompt(requirement, ctx)
    raw = generate_with_openrouter(prompt)
    if not raw:
        raise RuntimeError("Empty response from LLM")

    corrected = auto_correct_json(raw)
    try:
        cfg = json.loads(corrected)
    except json.JSONDecodeError as e:
        raise ValueError(f"LLM JSON parse failed: {e}") from e

    cfg = normalize_schedule_in_config(cfg)

    # schema validation (LLM config)
    try:
        js_validate(instance(cfg), schema=ADF_CONFIG_SCHEMA)
    except ValidationError as ve:
        raise ValueError(f"Config schema invalid: {ve.message}") from ve

    # Ensure linked service names present
    cfg.setdefault("source", {}).setdefault("linked_service", DEFAULTS["blob_ls"])
    cfg.setdefault("sink", {}).setdefault("linked_service", DEFAULTS["snowflake_ls"])
    return cfg

# -------------------- Routes --------------------
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
        log_error(reason=str(e), error_type=classify_error(str(e)), data={"stage": "llm_parse"})
        return error_response(400, "Invalid structured output from LLM", {"reason": str(e)})

    # 2) Precheck + (optional) auto-fix BEFORE pipeline generation/deploy
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
        # Do NOT 400; return 200 with guidance so UI can prompt
        return jsonify({
            "status": "blocked",
            "stage": "precheck",
            "initial": initial,
            "missing_inputs": missing_inputs
        }), 200

    fixed_actions = []
    try:
        fixed_any, fixed_actions = auto_fix_prereqs(ctx, initial)
        if fixed_any:
            final = check_prerequisites(ctx)
        else:
            final = initial
    except Exception as e:
        log_error(reason=str(e), error_type="precheck_autofix_error", data={"stage": "precheck"})
        return error_response(500, "Precheck auto-fix failed", {"reason": str(e), "initial": initial})

    # 3) Build ADF pipeline JSON & validate hooks
    try:
        adf_json = create_copy_activity_pipeline(cfg)
        ok, msg = validate_pipeline_hooks(adf_json)
        if not ok:
            raise ValueError(msg)
    except Exception as e:
        log_error(reason=str(e), error_type="validation_error", data={"stage": "adf_json"})
        return error_response(400, "ADF JSON validation failed", {"reason": str(e)})

    # 4) Save + deploy (or simulate)
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
        # Inform the UI without failing the HTTP call
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

@app.route("/healthz", methods=["GET"])
def healthz():
    return jsonify({"status": "ok"}), 200

# Optional UI stubs (templates must exist if you keep these)
@app.route("/ui/precheck")
def ui_precheck():
    return render_template("precheck.html")

@app.route("/ui/generate")
def ui_generate():
    return render_template("generate.html")

if __name__ == "__main__":
    debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=debug)
