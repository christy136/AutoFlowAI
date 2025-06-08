from flask import Flask, request, jsonify
from openai import OpenAI
from dotenv import load_dotenv
import os
from pipeline_generator.adf_generator import create_copy_activity_pipeline, save_pipeline_to_file
import json
import requests
import re

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

app = Flask(__name__)

OLLAMA_URL = "http://localhost:11434/api/generate"

def extract_json(text):
    try:
        # Remove markdown-style code fences (```json ... ```)
        cleaned_text = text.replace("```json", "").replace("```", "").strip()

        # Extract JSON-looking block using regex
        match = re.search(r'\{[\s\S]*?\}', cleaned_text)
        if match:
            json_str = match.group()
            print("EXTRACTED JSON:\n", json_str)
            return json.loads(json_str)
        else:
            print("No JSON found in the model response.")
            return None
    except Exception as e:
        print("Failed to parse JSON:", e)
        return None



def generate_pipeline(user_input):
    prompt = f"""Return only a JSON object with the fields: source, sink, format, schedule.
Example: {{
  "source": "AzureBlob",
  "sink": "AzureSqlDatabase",
  "format": "CSV",
  "schedule": "Daily"
}}
Now generate one for this requirement: {user_input}
"""

    response = requests.post("http://localhost:11434/api/generate", json={
        "model": "llama3",
        "prompt": prompt,
        "stream": False
    })

    result = response.json()
    return result.get("response", "")


@app.route('/generate', methods=['POST'])
def generate():
    data = request.json
    user_input = data.get("requirement")
    raw_output = generate_pipeline(user_input)

    print("----- RAW LLM OUTPUT -----")
    print(raw_output)
    print("--------------------------")

    config = extract_json(raw_output)
    if config is None:
        return jsonify({"error": "Invalid structured output from LLM"}), 400

    adf_json = create_copy_activity_pipeline(config)
    file_path = save_pipeline_to_file(adf_json)

    return jsonify({
        "pipeline": adf_json,
        "saved_to": file_path
    })



if __name__ == "__main__":
    app.run(debug=True)

