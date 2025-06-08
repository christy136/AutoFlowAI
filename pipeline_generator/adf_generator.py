import json
import os
from datetime import datetime


def create_copy_activity_pipeline(config: dict, pipeline_name="GeneratedPipeline"):
    source_type = config.get("source", "AzureBlob")
    sink_type = config.get("sink", "AzureSqlDatabase")
    file_format = config.get("format", "CSV")
    schedule = config.get("schedule", "Daily")

    # Template copy activity pipeline JSON
    pipeline_json = {
        "name": pipeline_name,
        "properties": {
            "description": "Auto-generated pipeline",
            "activities": [
                {
                    "name": "CopyData",
                    "type": "Copy",
                    "dependsOn": [],
                    "policy": {
                        "timeout": "7.00:00:00",
                        "retry": 0,
                        "retryIntervalInSeconds": 30,
                        "secureOutput": False,
                        "secureInput": False
                    },
                    "userProperties": [],
                    "typeProperties": {
                        "source": {
                            "type": "DelimitedTextSource" if file_format.lower() == "csv" else "JsonSource"
                        },
                        "sink": {
                            "type": "SqlSink" if "sql" in sink_type.lower() else "BlobSink"
                        }
                    },
                    "inputs": [
                        {
                            "referenceName": "InputDataset",
                            "type": "DatasetReference"
                        }
                    ],
                    "outputs": [
                        {
                            "referenceName": "OutputDataset",
                            "type": "DatasetReference"
                        }
                    ]
                }
            ],
            "annotations": [],
            "parameters": {},
            "runConcurrently": False
        }
    }

    return pipeline_json


def save_pipeline_to_file(pipeline_json, path="output", pipeline_name="GeneratedPipeline"):
    os.makedirs(path, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(path, f"{pipeline_name}_{timestamp}.json")
    with open(file_path, "w") as f:
        json.dump(pipeline_json, f, indent=4)
    return file_path

