import os
from dotenv import load_dotenv
from openai import OpenAI
from utils.settings import OPENROUTER_MODEL_DEFAULT  

load_dotenv()

client = OpenAI(
    api_key=os.getenv("OPENROUTER_API_KEY"),
    base_url="https://openrouter.ai/api/v1"
)

def generate_with_openrouter(prompt: str) -> str:
    try:
        model = os.getenv("OPENROUTER_MODEL", OPENROUTER_MODEL_DEFAULT) 
        completion = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            extra_headers={
                "HTTP-Referer": os.getenv("YOUR_SITE_URL", ""),
                "X-Title": os.getenv("YOUR_SITE_NAME", "")
            }
        )
        return completion.choices[0].message.content
    except Exception as e:
        print("OpenRouter API error:", e)
        return ""
