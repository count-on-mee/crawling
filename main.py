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

query = "신촌"
url = "https://pcmap.place.naver.com/place/list?query=" + query

driver.get(url)
source = driver.page_source

pattern = r'window\.__APOLLO_STATE__\s*=\s*({.*?});'
match = re.search(pattern, source, re.DOTALL)

parsed_data = json.loads(match.group(1))
cnt = 1

for key in parsed_data.keys():
    if "RestaurantListSummary" in key:
        data = parsed_data[key]

        id = data["id"]
        x = data["x"]
        y = data["y"]
        visitorReviewCount = data["visitorReviewCount"]
        visitorReviewScore = data["visitorReviewScore"]
        name = data["name"]
        businessCategory = data["businessCategory"]
        roadAddress = data["roadAddress"]
        address = data["address"]
        businessHours = data["businessHours"]
        imageUrls = data["imageUrls"]
        virtualPhone = data["virtualPhone"]

        print(id, x, y, visitorReviewCount, visitorReviewScore, name)
        print(businessCategory, roadAddress, address, businessHours, imageUrls, virtualPhone)

        cnt += 1

print("전체 장소의 수:", cnt)