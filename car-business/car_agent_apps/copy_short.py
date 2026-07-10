import sys
sys.path.pop(0)

import os
import pyperclip

script_directory = os.path.dirname(os.path.abspath(__file__))
caption_path = os.path.join(script_directory, "short_caption.txt")

try:
    with open(caption_path, "r", encoding="utf-8") as file:
        caption = file.read()

    if not caption.strip():
        raise ValueError("short_caption.txt is empty. Run short_caption.py first.")

    pyperclip.copy(caption)
    print(f"Copied short caption to clipboard from: {caption_path}")

except FileNotFoundError:
    print(f"Error: '{caption_path}' not found. Run short_caption.py first.")
except ValueError as e:
    print(f"Error: {e}")
except Exception as e:
    print(f"An error occurred: {e}")
