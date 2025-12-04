import pandas as pd
import requests
import json
import os 
import time 

url = 'https://api.yocket.com/explore/filter?apiVersion=v2'

headers = {
    'accept': 'application/json',
    'accept-language': 'en-US,en;q=0.9',
    'content-type': 'application/json',
    'origin': 'https://yocket.com',
    'priority': 'u=1, i',
    'referer': 'https://yocket.com/',
    'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36'
}

data = {
    "page": 1,
    "items": 1000,
    "sort_by": "rank",
    "level": 2,
    "country_abbreviations": ["US"]
}

# Make the POST request
response = requests.post(url, headers=headers, json=data)

# Check response status
print(f"Status Code: {response.status_code}")

# Parse JSON response
if response.status_code == 200:
    result = response.json()


#download a jpg from the url
def download_logo(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.content
    else:
        return None

#save the jpg to the current directory
#save to a new folder called logos
def save_logo(content, filename):
    os.makedirs('logos', exist_ok=True)
    with open(os.path.join('logos', filename), 'wb') as f:
        f.write(content)
        print(f"Saved {filename}")

predef_url = "https://yocket.com/_ipx/f_webp&q_80&s_70x70/https://d15gkqt2d16c1n.cloudfront.net/images/universities/logos/"
#download the logos

result_data = result['data']['result']

for college in result_data:
    logo_url = college['university_logo_url']
    full_url = f"{predef_url}{logo_url}"
    print(full_url)
    logo_content = download_logo(full_url)
    save_logo(logo_content, f"{college['university_name']}.jpg")
    print(f"Downloaded {college['university_name']}")
    time.sleep(1)

print("Logos downloaded successfully")