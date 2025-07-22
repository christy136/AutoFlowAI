from jinja2 import Environment, FileSystemLoader
import os

class PromptManager:
    def __init__(self, template_dir="prompt_templates"):
        self.env = Environment(loader=FileSystemLoader(template_dir))

    def render_prompt(self, template_name, context):
        template = self.env.get_template(template_name)
        return template.render(context)
