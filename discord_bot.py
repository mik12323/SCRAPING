import requests

class DiscordNotifier:
    def __init__(self, webhook_url):
        self.url = webhook_url

    def post(self, title, description, url):
        # 3447003 is a 'Decimal' code for blue. 
        # You can find these codes online (Discord Color Picker).
        
        if len(description) > 3900:
            description = description[:3900] + "..."
        payload = {
            "content": "@everyone",  # This line triggers the ping!
            "embeds": [
                {
                    "title": title,
                    "description": description,
                    "url": url,
                    # You can even add a footer at the bottom
                    "footer": {
                        "text": "Sent by Sauce's Fed Monitor Bot"
                    }
                }
            ]
        }
        
        requests.post(self.url, json=payload)