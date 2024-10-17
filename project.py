import os
import requests
from dotenv import load_dotenv
from typing import List
import logging
from flask import Flask, request, jsonify

load_dotenv()

COINSTATS_API_URL = os.getenv('COINSTATS_API_URL')
COINSTATS_API_KEY = os.getenv('COINSTATS_API_KEY')
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_API_KEY = os.getenv('SUPABASE_API_KEY')
SUPABASE_SERVICE_ROLE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
SUPABASE_TABLE = os.getenv('SUPABASE_TABLE', 'crypto_coins')
LIMIT = int(os.getenv('LIMIT', 1000))
# TOTAL_PAGES = int(os.getenv('TOTAL_PAGES', 3))

print(f"COINSTATS_API_URL: {COINSTATS_API_URL}")
print(f"SUPABASE_URL: {SUPABASE_URL}")
print(f"SUPABASE_API_KEY: {SUPABASE_API_KEY}")
print(f"SUPABASE_SERVICE_ROLE_KEY: {SUPABASE_SERVICE_ROLE_KEY}")
print(f"SUPABASE_TABLE: {SUPABASE_TABLE}")
print(f"LIMIT: {LIMIT}")

logging.basicConfig(
    filename='coinstats_supabase.log',
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

def fetch_coins(limit: int, api_key: str = None) -> List[dict]:
    headers = {}
    if api_key:
        headers['X-API-KEY'] = COINSTATS_API_KEY

    params = {
        'page': 1,
        'limit': limit
    }

    try:
        response = requests.get(COINSTATS_API_URL, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        logging.info(f"Fetched {len(data.get('result', []))} coins successfully.")
        return data.get('result', [])
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching coins: {e}")
        return []

def upsert_batch_to_supabase(coins: List[dict], batch_size: int = 100):
    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
    headers = {
        'apikey': SUPABASE_API_KEY,
        'Authorization': f'Bearer {SUPABASE_SERVICE_ROLE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'resolution=merge-duplicates'
    }

    for i in range(0, len(coins), batch_size):
        batch = coins[i:i + batch_size]
        payload = []
        for coin in batch:
            payload.append({
                "id": coin.get("id"),
                "icon": coin.get("icon"),
                "name": coin.get("name"),
                "symbol": coin.get("symbol"),
                "rank": coin.get("rank"),
                "price": coin.get("price"),
                "volume": coin.get("volume"),
                "marketCap": coin.get("marketCap"),
                "availableSupply": coin.get("availableSupply"),
                "totalSupply": coin.get("totalSupply"),
                "fullyDilutedValuation": coin.get("fullyDilutedValuation"),
                "priceChange1h": coin.get("priceChange1h"),
                "priceChange1d": coin.get("priceChange1d"),
                "priceChange1w": coin.get("priceChange1w"),
                "websiteUrl": coin.get("websiteUrl")
            })

        try:
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            logging.info(f"Upserted batch of {len(batch)} coins")
        except requests.exceptions.RequestException as e:
            logging.error(f"Error upserting batch starting at index {i}: {e}")

def main():
    
    logging.info("Starting CoinStats to Supabase sync.")

    coins = fetch_coins(LIMIT, COINSTATS_API_KEY)

    if coins:
        upsert_batch_to_supabase(coins)
    else:
        logging.warning("No coins fetched")

    logging.info("CoinStats to Supabase sync completed")

if __name__ == "__main__":
    main()