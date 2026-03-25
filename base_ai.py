import sys
sys.path.pop(0) # Put this at the very top!
import os
from bs4 import BeautifulSoup
import requests
from google import genai
from google.genai import types

class Base_Ai:
    @staticmethod
    def bullish_or_bearish(url, user_prompt):
        my_key = os.getenv("GEMINI_KEY")
        client = genai.Client(api_key=my_key)
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')

        speech_text = soup.find("div",  class_="col-xs-12 col-sm-8 col-md-8")

        prompt = f"""{user_prompt}

        Here are the speech to refer on: 
        {speech_text}"""

        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt
        )
        new_text = response.text.strip()

        return new_text
    
    @staticmethod
    def general_question(text_content, user_prompt, image_url=None):
        # Use GEMINI_API_KEY to match your .env
        my_key = os.getenv("GEMINI_KEY") 
        client = genai.Client(api_key=my_key)

        contents = [user_prompt, text_content]

        # If an image URL is provided, download it for Gemini to "see"
        if image_url:
            try:
                img_data = requests.get(image_url).content
                image_part = types.Part.from_bytes(data=img_data, mime_type="image/jpeg")
                contents.append(image_part)
            except Exception as e:
                print(f"AI could not load image: {e}")

        response = client.models.generate_content(
            model='gemini-2.0-flash', 
            contents=contents
        )
        return response.text.strip()