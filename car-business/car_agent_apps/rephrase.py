import sys
sys.path.pop(0) # Put this at the very top!

import os
from dotenv import load_dotenv
import pyperclip
from google import genai

load_dotenv()
client = genai.Client(api_key=os.getenv("GEMINI_KEY"))

# Get the directory and file path
script_directory = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_directory, "details.txt")

try:
    # Read the old description from the notepad
    with open(file_path, "r", encoding="utf-8") as file:
        original_text = file.read()

    # Tell the AI exactly how to format the text
    
    prompt = f"""You are rewriting a Facebook car-for-sale post for the Philippine market.

Your goal is to make the post look like a NEW listing every time so Facebook will not detect it as repetitive or copied.

STRICT RULES:

Create a NEW attention-grabbing header every time. Focus on things Filipino buyers care about like LOW PRICE, BELOW MARKET, MURA, RUSH, PAUNAHAN, LOW ODO, FIRST OWNER. The header can be English, Tagalog, or Taglish.

The description must be SHORT, DIRECT, and factual. Avoid storytelling and avoid sweet marketing phrases.

Only focus on key details:
Transmission
Fuel type (Gas / Diesel / Hybrid)
Mileage / Odometer
First owner or not
Location
Very important features only

You may reorder the details slightly every time so it looks different, but keep the same information.

Avoid promotional phrases like:
Drive home today
Don't miss this opportunity
Highly sought-after
Best deal

Use MINIMAL or NO emoji.

Make it sound like a normal Filipino Facebook seller wrote it. It must NOT sound like AI.

FORMAT RULE (VERY IMPORTANT):

The output MUST have GOOD SPACING.

The header stays on the first line.

Every important detail MUST be on its own line.

Leave a blank line after the header.

Example format:

HEADER HERE

Year Model Variant
Color
Transmission
Fuel Type
Mileage
Owner info

Location

Do NOT compress sentences into one paragraph.

Do NOT use bullet points, symbols, hashtags, asterisks, or formatting.

Only return the final post ready for COPY PASTE to Facebook.

No explanations. No extra text. Only the final listing.

    Here are the car details to refer on: 
    {original_text}"""
    # Get the new text from the AI using the new client format
    response = client.models.generate_content(
        model='gemini-2.5-flash',
        contents=prompt
    )
    new_text = response.text.strip()

    # Overwrite the notepad file with the new text
    with open(file_path, "w", encoding="utf-8") as file:
        file.write(new_text)

    # Copy the new text to the clipboard
    pyperclip.copy(new_text)

    print("Success! The description was rewritten, saved to notepad, and copied.")

except FileNotFoundError:
    print(f"File '{file_path}' not found.")
except Exception as e:
    print(f"An error occurred: {e}")






    