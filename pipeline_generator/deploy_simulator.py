import json
import os
import datetime
from jsonschema import validate, ValidationError

def validate_pipeline_hooks(pipeline):
    # (unchanged)
    # ...
    return True, "Validation successful"

def deploy_pipeline_simulator(pipeline_path):
    # (unchanged load/validate)
    # ...

    # ✅ Optional manual feedback (skip in CI)
    feedback = ""
    if os.getenv("CI", "false").lower() != "true":
        try:
            feedback = input("\n📝 Optional feedback for this pipeline? (press Enter to skip): ")
        except EOFError:
            feedback = ""
    else:
        print("\nCI mode detected; skipping interactive feedback.")

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
    # (unchanged)
    pass
