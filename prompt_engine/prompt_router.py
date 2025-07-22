import yaml

class PromptRouter:
    def __init__(self, registry_path="prompt_registry.yaml"):
        with open(registry_path, "r") as f:
            self.registry = yaml.safe_load(f)

    def detect_template(self, user_prompt: str) -> str:
        prompt_lower = user_prompt.lower()
        for intent in self.registry["intents"]:
            if all(k in prompt_lower for k in intent["keywords"]):
                return intent["template"]
        return "default_pipeline.j2"  # fallback template
