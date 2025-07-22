'''Libraries'''
from flask import Flask, request, jsonify
from dotenv import load_dotenv
import os
import json
import re

from llm_clients.openrouter_client import generate_with_openrouter
from pipeline_generator.adf_generator import create_copy_activity_pipeline, save_pipeline_to_file
from prompt_engine.prompt_router import PromptRouter
from prompt_engine.prompt_manager import PromptManager


'''Application'''
app = Flask(__name__)
load_dotenv()


'''Utility'''
def extract_json(text):
    try:
        # Remove markdown-style code fences
        cleaned = text.replace("```json", "").replace("```", "").strip()

        # Attempt full JSON load directly (most robust)
        return json.loads(cleaned)
    except json.JSONDecodeError as e:
        print("⚠️ JSON Decode Error:", e)
        return None



'''Pipeline Generation'''
def generate_pipeline(user_input, context):
    router = PromptRouter()
    template = router.detect_template(user_input)
    if not template:
        template = "default_pipeline.j2"
        context["user_input"] = user_input

    prompt_mgr = PromptManager()
    rendered_prompt = prompt_mgr.render_prompt(template, context)

    llm_output = generate_with_openrouter(rendered_prompt)
    return llm_output, None


'''API'''
@app.route('/generate', methods=['POST'])
def generate():
    data = request.json
    user_input = data.get("requirement")
    context = data.get("context", {})  # should include source/sink/schedule/etc.

    raw_output, error = generate_pipeline(user_input, context)
    if error:
        return jsonify({"error": error}), 400

    # print("===== RAW LLM OUTPUT =====")
    # print(raw_output)
    # print("==========================")

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
