import sys
sys.path.pop(0)

import os
from dotenv import load_dotenv
from google import genai

load_dotenv()
client = genai.Client(api_key=os.getenv("GEMINI_KEY"))

script_directory = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_directory, "details.txt")
output_path = os.path.join(script_directory, "short_caption.txt")

try:
    with open(file_path, "r", encoding="utf-8") as file:
        original_text = file.read()

    if not original_text.strip():
        raise ValueError("details.txt is empty.")

    prompt = f"""You are a Facebook Marketplace car ad writer for the Philippines.

Given the following car details, generate a SHORT attention-grabbing car sale caption (2-6 lines).

Rules:
- Extract these if present: Year, Brand, Model, Variant, Transmission (Matic/Manual), Price, Key selling points.
- Put the price prominently.
- Use "‼️" for attention when appropriate.
- Maximum 6 lines. Keep it very short.
- No long descriptions, no marketing fluff.
- No emojis except "‼️".
- Use natural Filipino car-selling language where suitable: "Matic", "Manual", "First Owner", "Complete Papers", "No Issue", "Running Condition".
- Never invent details. Only use what is provided.
- Output ONLY the caption, nothing else. No quotes, no markdown.

Examples of the style:
For Sale 2017 Toyota Fortuner V
₱930K

₱380K ‼️
2016 Toyota Altis G

₱450K ‼️
2019 Toyota Rush G
Matic

Toyota Rush 2019
Manual
First owner
Complete papers
Running condition
No issue
₱420K

Here are the car details:
{original_text}"""

    response = client.models.generate_content(
        model='gemini-2.5-flash',
        contents=prompt
    )
    caption_text = response.text.strip()

    with open(output_path, "w", encoding="utf-8") as file:
        file.write(caption_text)

    print(f"Success! Short caption saved to: {output_path}")

except FileNotFoundError:
    print(f"Error: File '{file_path}' not found.")
except ValueError as e:
    print(f"Error: {e}")
except Exception as e:
    print(f"An error occurred: {e}")
