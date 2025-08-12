import json
import os
import datetime
from jsonschema import validate, ValidationError

def validate_pipeline_hooks(pipeline):
    # ✅ Hook 1: Required keys
    if "name" not in pipeline or "properties" not in pipeline:
        return False, "Missing required keys: 'name' or 'properties'"

    # ✅ Hook 2: Activities must exist
    activities = pipeline["properties"].get("activities", [])
    if not isinstance(activities, list) or not activities:
        return False, "Pipeline must include a non-empty 'activities' list"

    # ✅ Hook 3: Each activity must have 'name' and 'type'
    for act in activities:
        if "name" not in act or "type" not in act:
            return False, f"Activity missing 'name' or 'type': {act}"

    # ✅ Hook 4: No duplicate activity names
    names = [a["name"] for a in activities]
    if len(names) != len(set(names)):
        return False, "Duplicate activity names found"

    # ✅ Hook 5: Warn on unknown activity types
    known_types = ["Copy", "ExecutePipeline", "ExecuteDataFlow"]
    for act in activities:
        if act["type"] not in known_types:
            print(f"⚠️ Warning: Unrecognized activity type: {act['type']}")

    return True, "Validation successful"

def deploy_pipeline_simulator(pipeline_path):
    if not os.path.exists(pipeline_path):
        print(f"❌ File not found: {pipeline_path}")
        return

    with open(pipeline_path, 'r') as f:
        try:
            pipeline = json.load(f)
        except json.JSONDecodeError as e:
            print("❌ Invalid JSON:", e)
            return

    print("\n✅ SIMULATION: Pipeline JSON loaded successfully")
    print("📌 Pipeline Name:", pipeline.get("name", "<Unnamed>"))

    # ✅ Ensure logs directory exists
    logs_dir = os.path.join(os.path.dirname(__file__), "..", "logs")
    os.makedirs(logs_dir, exist_ok=True)

    # ✅ Run validation hooks
    is_valid, message = validate_pipeline_hooks(pipeline)
    if not is_valid:
        print(f"❌ Validation failed: {message}")
        invalid_log_path = os.path.join(logs_dir, "invalid_pipelines.jsonl")
        log_entry = {
            "timestamp": datetime.datetime.now().isoformat(),
            "pipeline": pipeline.get("name", "<Unknown>"),
            "activities": [a.get("name", "") for a in pipeline.get("properties", {}).get("activities", [])],
            "error": message,
            "source_file": pipeline_path
        }
        with open(invalid_log_path, "a") as log_file:
            log_file.write(json.dumps(log_entry) + "\n")
        print(f"📄 Validation error logged to: {os.path.abspath(invalid_log_path)}\n")
        return
    else:
        print(f"✅ Validation passed: {message}")
        activities = pipeline.get("properties", {}).get("activities", [])
        success_log_path = os.path.join(logs_dir, "successful_pipelines.jsonl")
        success_entry = {
            "timestamp": datetime.datetime.now().isoformat(),
            "pipeline": pipeline.get("name", "<Unnamed>"),
            "activities": [a.get("name", "") for a in activities],
            "status": "validated",
            "source_file": pipeline_path
        }
        with open(success_log_path, "a") as log_file:
            log_file.write(json.dumps(success_entry) + "\n")
        print(f"📄 Successful validation logged to: {os.path.abspath(success_log_path)}")

    # ✅ Optional manual feedback
    feedback = input("\n📝 Optional feedback for this pipeline? (press Enter to skip): ")
    feedback_log_path = os.path.join(logs_dir, "feedback_logs.json")
    log_entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "pipeline": pipeline.get("name", "<Unnamed>"),
        "activities": [a.get("name", "") for a in activities],
        "feedback": feedback
    }
    with open(feedback_log_path, "a") as log_file:
        log_file.write(json.dumps(log_entry) + "\n")
    print(f"📄 Feedback saved to: {os.path.abspath(feedback_log_path)}\n")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("❌ Usage: python3 deploy_simulator.py <pipeline_file.json>")
    else:
        deploy_pipeline_simulator(sys.argv[1])
