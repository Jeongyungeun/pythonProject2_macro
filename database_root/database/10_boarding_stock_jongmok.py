import requests
import re
from bs4 import BeautifulSoup
import pandas as pd
import json
import time

header = {'Accept' : 'application/json, text/javascript, */*; q=0.01',
          'Host' : 'kr.investing.com',
          'Referer'	:'https://kr.investing.com/stock-screener/?sp=country::5|sector::a|industry::a|equityType::a%3Ceq_market_cap;1',
          'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36',
          'X-Requested-With' : 'XMLHttpRequest'}


r= requests.get("https://kr.investing.com/stock-screener/", headers=header)

print(r.status_code) # 요청값 잘 받았는지 확인하기

bs = BeautifulSoup(r.text, 'lxml')
# bs.find("ul", {'id':"countriesUL"}).findAll("li")

country_dict = {}
for country in bs.find("ul", {'id':"countriesUL"}).findAll("li"):  # ul태그에 id countriesUL값을 가지는 li 태그 값 다 가져오기.
    print (country.text.strip(), country['data-value'])
    country_dict[country.text.strip()] = country['data-value']

len(country_dict)
