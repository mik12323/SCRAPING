import requests

class DiscordNotifier:
    @staticmethod # Use staticmethod so you don't need 'self'
    def post(webhook_url, title, description, post_link, image_url=None):
        if len(description) > 3900:
            description = description[:3900] + "..."

        embed = {
            "title": title,
            "description": description,
            "url": post_link,
            "footer": {"text": "Sent by Sauce's Fed Monitor Bot"},
            "color": 0x00ff00 # Green color
        }

        # If there is an image, add it to the embed
        if image_url:
            embed["image"] = {"url": image_url}

        payload = {
            "content": "@everyone",
            "embeds": [embed]
        }
        
        requests.post(webhook_url, json=payload)