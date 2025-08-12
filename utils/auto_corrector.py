import re

def auto_correct_json(raw_text: str) -> str:
    text = raw_text.replace("```json", "").replace("```", "").strip()
    json_block = re.search(r'\{(?:[^{}]|(?:\{[^{}]*\}))*\}', text, re.DOTALL)
    if not json_block:
        return "{}"  # fallback

    json_text = json_block.group()
    fixed_json = re.sub(r",\s*([\]}])", r"\1", json_text)  # fix trailing commas
    fixed_json = re.sub(r"[\r\n]+", "", fixed_json)        # remove line breaks
    return fixed_json
