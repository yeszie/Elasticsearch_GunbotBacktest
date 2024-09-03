from elasticsearch import Elasticsearch
import os
import json
import hashlib

# Wstaw swój klucz API
api_key = "TnRLZnVaRUIyVEo3LURNbFc1Vjg6YnVXWU5EbTlSUS1DaGxLMjJ3Y21xQQ=="

# Połączenie z Elasticsearch z użyciem klucza API
es = Elasticsearch(
    "https://2910a2c865064c4eba853543e4f803e8.us-central1.gcp.cloud.es.io:443",
    api_key=api_key
)

directory = 'upload1'

for filename in os.listdir(directory):
    if filename.endswith(".json"):
        with open(os.path.join(directory, filename), 'r') as file:
            data = json.load(file)

            # Konwersja wartości 'candles.volume.value' na float
            if 'candles' in data and isinstance(data['candles'], dict):
                for candle in data['candles'].get('volume', []):
                    if 'value' in candle and candle['value'] is not None:
                        candle['value'] = float(candle['value'])

            # Konwersja wartości 'orders.averagePrice', 'orders.fees', 'orders.pnl', 'orders.ABP', 'orders.baseValue' na float, jeśli nie są None
            if 'orders' in data and isinstance(data['orders'], list):
                for order in data['orders']:
                    if 'averagePrice' in order and order['averagePrice'] is not None:
                        order['averagePrice'] = float(order['averagePrice'])
                    if 'fees' in order and order['fees'] is not None:
                        order['fees'] = float(order['fees'])
                    if 'pnl' in order and order['pnl'] is not None:
                        order['pnl'] = float(order['pnl'])
                    if 'ABP' in order and order['ABP'] is not None:
                        order['ABP'] = float(order['ABP'])
                    if 'baseValue' in order and order['baseValue'] is not None:
                        order['baseValue'] = float(order['baseValue'])

            # Konwersja wartości 'performance' na float tam gdzie to konieczne
            if 'performance' in data and isinstance(data['performance'], dict):
                performance = data['performance']
                fields_to_convert = ['startingFunds', 'Realized PnL', 'Realized Profit', 
                                     'Realized Loss', 'Volume', 'Buy volume', 'Sell volume', 
                                     'Fees paid']
                for field in fields_to_convert:
                    if field in performance:
                        # Usuwanie jednostki BTC przed konwersją
                        value = performance[field].replace(" BTC", "").strip()
                        performance[field] = float(value) if value else 0.0

            # Tworzenie unikalnego ID na podstawie zawartości pliku
            doc_id = hashlib.md5(json.dumps(data, sort_keys=True).encode('utf-8')).hexdigest()

            # Upload z unikalnym ID
            es.index(index='gunbot-backtest', id=doc_id, document=data)
