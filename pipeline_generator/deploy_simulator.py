import json
import os
import datetime

def validate_pipeline_hooks(pipeline):
    """
    Minimal structural validation used by the Flask app.
    Ensures 'properties' and at least one activity exist.
    """
    try:
        props = pipeline.get("properties", {})
        activities = props.get("activities", [])
        if not isinstance(activities, list) or not activities:
            return False, "Pipeline has no activities."
        return True, "Validation successful"
    except Exception as e:
        return False, f"Validation error: {e}"

def deploy_pipeline_simulator(pipeline_path):
    """
    Offline simulator: loads a pipeline JSON, validates, and stores optional feedback.
    This is only for local testing; not called by the Flask app in normal flow.
    """
    if not os.path.exists(pipeline_path):
        print(f"❌ File not found: {pipeline_path}")
        return

    with open(pipeline_path, "r", encoding="utf-8") as f:
        pipeline = json.load(f)

    ok, msg = validate_pipeline_hooks(pipeline)
    if not ok:
        print(f"❌ Validation failed: {msg}")
        return

    props = pipeline.get("properties", {})
    activities = props.get("activities", [])
    print(f"✅ Valid pipeline: {pipeline.get('name','<Unnamed>')} with {len(activities)} activity(ies).")

    # Optional feedback
    feedback = ""
    if os.getenv("CI", "false").lower() != "true":
        try:
            feedback = input("\n📝 Optional feedback for this pipeline? (press Enter to skip): ")
        except EOFError:
            feedback = ""
    else:
        print("\nCI mode detected; skipping interactive feedback.")

    # Ensure logs dir exists
    logs_dir = "logs"
    os.makedirs(logs_dir, exist_ok=True)
    feedback_log_path = os.path.join(logs_dir, "feedback_logs.jsonl")
    log_entry = {
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "pipeline": pipeline.get("name", "<Unnamed>"),
        "activities": [a.get("name", "") for a in activities],
        "feedback": feedback
    }
    with open(feedback_log_path, "a", encoding="utf-8") as log_file:
        log_file.write(json.dumps(log_entry) + "\n")
    print(f"📄 Feedback saved to: {os.path.abspath(feedback_log_path)}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python -m pipeline_generator.deploy_simulator <pipeline_json_path>")
        raise SystemExit(1)
    deploy_pipeline_simulator(sys.argv[1])
