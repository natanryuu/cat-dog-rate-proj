import requests
import pandas as pd
import time

API_KEY = "AIzaSyBsN_Xf5Wp9NLFRoBAcsKrcKFh-k852ng4"
API_KEY = API_KEY.strip()  # ← 加這行，清除頭尾空白或隱藏字元

# 驗證 key 是否乾淨
assert all(ord(c) < 128 for c in API_KEY), f"API Key 含非ASCII字元，請重新複製！"
print(f"API Key 長度：{len(API_KEY)}，前5碼：{API_KEY[:5]}")

districts = [
    "松山區", "信義區", "大安區", "中山區", "中正區", "大同區",
    "萬華區", "文山區", "南港區", "內湖區", "士林區", "北投區"
]

keywords = ["美甲", "美睫", "髮廊", "婚紗", "美容護膚", "瑜珈", "SPA"]

url = "https://places.googleapis.com/v1/places:searchText"
headers = {
    "Content-Type": "application/json",
    "X-Goog-Api-Key": API_KEY,
    "X-Goog-FieldMask": "places.displayName,places.formattedAddress,places.rating,places.userRatingCount,places.id,places.location,nextPageToken"
}

results = []

for district in districts:
    for keyword in keywords:
        print(f"搜尋中：{keyword} × {district}")
        
        body = {
            "textQuery": f"{keyword} {district} 台北市",
            "languageCode": "zh-TW"
        }
        
        while True:
            resp = requests.post(url, headers=headers, json=body).json()
            
            for place in resp.get("places", []):
                results.append({
                    "district": district,
                    "keyword": keyword,
                    "name": place.get("displayName", {}).get("text"),
                    "address": place.get("formattedAddress"),
                    "rating": place.get("rating"),
                    "reviews": place.get("userRatingCount"),
                    "place_id": place.get("id"),
                    "lat": place.get("location", {}).get("latitude"),
                    "lng": place.get("location", {}).get("longitude"),
                })
            
            next_token = resp.get("nextPageToken")
            if next_token:
                time.sleep(2)
                body = {"textQuery": f"{keyword} {district} 台北市", "languageCode": "zh-TW", "pageToken": next_token}
            else:
                break
        
        time.sleep(0.5)

df = pd.DataFrame(results).drop_duplicates(subset=["place_id"])
print(f"\n✅ 共 {len(df)} 家店")
df.to_csv("data/taipei_female_business.csv", index=False, encoding="utf-8-sig")
print("已存成 data/taipei_female_business.csv")