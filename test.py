import requests

url = "http://localhost:8000"
res = requests.get(url)
print(res.text)