import requests


ID = '666'
URL = f"https://d5d5g2fjv5qo1nui0cgs.apigw.yandexcloud.net/rating/{ID}"
page = requests.post(URL, params={'rate' : 10})


URL = "https://d5d5g2fjv5qo1nui0cgs.apigw.yandexcloud.net/rating"
page = requests.get(URL)
page.text


ID = '666'
URL = f"https://d5d5g2fjv5qo1nui0cgs.apigw.yandexcloud.net/rating/{ID}"
page = requests.delete(URL)