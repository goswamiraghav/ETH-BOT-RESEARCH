import os

import requests
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")

url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage'
r = requests.post(url, data={'chat_id': CHAT_ID, 'text': 'Bot is connected and ready!'})
print(r.status_code, r.json())