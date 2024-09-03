from elasticsearch import Elasticsearch

# Wstaw swój klucz API
api_key = "TnRLZnVaRUIyVEo3LURNbFc1Vjg6YnVXWU5EbTlSUS1DaGxLMjJ3Y21xQQ=="

# Połączenie z Elasticsearch z użyciem klucza API
es = Elasticsearch(
    "https://2910a2c865064c4eba853543e4f803e8.us-central1.gcp.cloud.es.io:443",
    api_key=api_key
)

# Nazwa indeksu
index_name = 'gunbot-backtest'

# Zapytanie o kilka dokumentów z indeksu
response = es.search(index=index_name, body={"query": {"match_all": {}}, "size": 5})

# Wyświetlanie wyników
for hit in response['hits']['hits']:
    print(hit['_source'])
