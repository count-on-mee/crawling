import requests
import re
from bs4 import BeautifulSoup
from selenium import webdriver
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from webdriver_manager.chrome import ChromeDriverManager
import re
import json
import random
import time

user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36"

# Webdriver headless mode setting
options = webdriver.ChromeOptions()
options.add_argument('headless')
options.add_argument('window-size=1920x1080')
options.add_argument("disable-gpu")
options.add_argument("user-agent=" + user_agent)

# Setting the Chrome driver
service = ChromeService(executable_path=ChromeDriverManager().install())
# Execute the Chrome driver
driver = webdriver.Chrome(service=service, options=options)

# BS4 setting for secondary access
session = requests.Session()

retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
session.mount('http://', HTTPAdapter(max_retries=retries))
#서울 전체의 식당, 카페, 숙소, 관광지, 박물관, 전시관
queryA = "서울"
queryB = [ "강남구", "강동구", "강북구", "강서구", "관악구", "광진구", "구로구", "금천구", "노원구", "도봉구", "동대문구", "동작구", "마포구", "서대문구", "서초구", "성동구", "성북구", "송파구", "양천구", "영등포구", "용산구", "은평구", "종로구", "중구", "중랑구" ]
queryC = "박물관"
url_base = "https://pcmap.place.naver.com/place/list?query="
urls = [ url_base + queryA + b + queryC for b in queryB ]

cnt = 0

all_data = []
file_name = f"{queryA + queryC}.json"

for url in urls:

    driver.get(url)

    random_wait_time = random.uniform(1, 5)

    time.sleep(random_wait_time)
    print("start")

    source = driver.page_source

    pattern = r'window\.__APOLLO_STATE__\s*=\s*({.*?});'

    match = re.search(pattern, source, re.DOTALL)

    parsed_data = json.loads(match.group(1))

    for key in parsed_data.keys():
        #식당은 RestaurantListSummary, 관광지쪽은 AttractionListItem, 숙소는 PlaceSummary
        if "PlaceSummary" in key:
            data = parsed_data[key]
            all_data.append({
                "id": data.get("id"),
                "x": data.get("x"),
                "y": data.get("y"),
                "visitorReviewCount": data.get("visitorReviewCount"),
                "visitorReviewScore": data.get("visitorReviewScore"),
                "name": data.get("name"),
                "category": data.get("category"),
                "roadAddress": data.get("roadAddress"),
                "address": data.get("address"),
                "businessHours": data.get("businessHours"),
                "imageUrl": data.get("imageUrl"),
                "phone": data.get("phone"),
            })
            cnt += 1
        if "RestaurantListSummary" in key:
            data = parsed_data[key]
            all_data.append({
                "id": data.get("id"),
                "x": data.get("x"),
                "y": data.get("y"),
                "visitorReviewCount": data.get("visitorReviewCount"),
                "visitorReviewScore": data.get("visitorReviewScore"),
                "name": data.get("name"),
                "businessCategory": data.get("businessCategory"),
                "roadAddress": data.get("roadAddress"),
                "address": data.get("address"),
                "businessHours": data.get("businessHours"),
                "imageUrls": data.get("imageUrls"),
                "phone": data.get("phone"),
            })
            cnt += 1
        if "AttractionListItem" in key:
            data = parsed_data[key]
            all_data.append({
                "id": data.get("id"),
                "x": data.get("x"),
                "y": data.get("y"),
                "visitorReviewCount": data.get("visitorReviewCount"),
                "visitorReviewScore": data.get("visitorReviewScore"),
                "name": data.get("name"),
                "category": data.get("category"),
                "roadAddress": data.get("roadAddress"),
                "address": data.get("address"),
                "businessHours": data.get("businessHours"),
                "imageUrls": data.get("imageUrls"),
                "phone": data.get("phone"),
            })
            cnt += 1

            # id = data["id"]
            # x = data["x"]
            # y = data["y"]
            # visitorReviewCount = data["visitorReviewCount"]
            # visitorReviewScore = data["visitorReviewScore"]
            # name = data["name"]
            # category = data["category"]
            # roadAddress = data["roadAddress"]
            # address = data["address"]
            # businessHours = data["businessHours"]
            # imageUrl = data["imageUrl"]
            # phone = data["phone"]

            # print(id, x, y, visitorReviewCount, visitorReviewScore, name)
            # print(category, roadAddress, address, businessHours, imageUrl, phone)

    print("완료:",url);   

driver.quit()

with open(file_name, "w", encoding="utf-8") as json_file:
    json.dump(all_data, json_file, ensure_ascii=False, indent=4)

print("전체 장소의 수:", cnt)



