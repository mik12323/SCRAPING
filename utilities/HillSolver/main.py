import os
import json
from dotenv import load_dotenv
from eth_utils import keccak
import google.generativeai as genai

load_dotenv()

genai.configure(api_key=os.getenv("GEMINI_KEY"))

model = genai.GenerativeModel("gemini-2.5-flash")

# -----------------------------
# INPUT
# -----------------------------

PROMPT = "LOONA January"

ANSWER_HASH = "0x7f985dccc023a1f90a3ec905fdfdde634c027ac9b1d47d050e1ef4c33198caee"

# -----------------------------
# GEMINI
# -----------------------------

prompt = f"""
You are solving a blockchain puzzle.

Question:

{PROMPT}

Generate EVERY possible answer.

Rules:

- Return ONLY JSON.
- No explanation.
- One category.
- Up to 200 possible answers.
- Include spelling variations.
- Include uppercase/lowercase if appropriate.

Example:

{{
    "answers":[
        "HeeJin",
        "HyunJin"
    ]
}}
"""

response = model.generate_content(prompt)

text = response.text.strip()

print("\nGemini Response\n")
print(text)

data = json.loads(text)

answers = data["answers"]

print("\nChecking...\n")

found = False

for answer in answers:

    variations = list(dict.fromkeys([
        answer,
        answer.lower(),
        answer.upper(),
        answer.title(),
        answer.replace(" ", ""),
        answer.replace(" ", "").lower()
    ]))

    for candidate in variations:

        h = "0x" + keccak(candidate.encode()).hex()

        if h.lower() == ANSWER_HASH.lower():

            print("="*50)
            print("FOUND!")
            print(candidate)
            print("="*50)

            found = True
            break

    if found:
        break

if not found:
    print("\nNo match found.")