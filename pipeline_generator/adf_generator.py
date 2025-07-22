import json
import os
from datetime import datetime


def create_copy_activity_pipeline(config):
    source = config.get("source", {})
    sink = config.get("sink", {})
    schedule = config.get("schedule", "once")

    source_type = source.get("type", "").lower()
    sink_type = sink.get("type", "").lower()  # ✅ Fix: get 'type' from dict

    pipeline = {
        "name": "CopyPipeline",
        "properties": {
            "activities": [
                {
                    "name": "CopyActivity",
                    "type": "Copy",
                    "inputs": [{"referenceName": "SourceDataset", "type": "DatasetReference"}],
                    "outputs": [{"referenceName": "SinkDataset", "type": "DatasetReference"}],
                    "typeProperties": {
                        "source": {
                            "type": "BlobSource" if "blob" in source_type else "JsonSource"
                        },
                        "sink": {
                            "type": "SqlSink" if "sql" in sink_type else "BlobSink"
                        }
                    }
                }
            ],
            "annotations": [],
            "runtimeConfiguration": {
                "frequency": schedule
            }
        }
    }

    return pipeline



def save_pipeline_to_file(pipeline_json, path="output", pipeline_name="GeneratedPipeline"):
    os.makedirs(path, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(path, f"{pipeline_name}_{timestamp}.json")
    with open(file_path, "w") as f:
        json.dump(pipeline_json, f, indent=4)
    return file_path

