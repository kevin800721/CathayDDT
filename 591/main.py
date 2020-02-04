import requests
#import urllib.request
import re
import pymongo
#import warnings
from bs4 import BeautifulSoup

## 參數
list_urlJumpIp = ["1", "3"]   # 1: 臺北市; 3: 新北市
#list_urlJumpIp = ["3"]   # 3: 新北市

## 建立資料庫
client = pymongo.MongoClient(host='localhost')
db = client["591scrap"]
col = db["rent_tp_ntp"]
dict_city = {"1": "臺北市", "2": "基隆市", "3": "新北市"}

## "物件列表"頁
url = "https://rent.591.com.tw/?kind=0&region=1"
for urlJumpIp in list_urlJumpIp:
    lastp = 0  # 判定是否為最終頁
    firstRow = "0"

    while True:
        params = {"firstRow": firstRow,
                  "order": "posttime", "orderType": "asc"}      # 刊登時間由舊到新，不漏掉最新資料
        cookies = {"urlJumpIp": urlJumpIp}
        req = requests.get(url, params=params, cookies=cookies)
        soup = BeautifulSoup(req.text, "html5lib")
        #soup = BeautifulSoup(urllib.request.urlopen(url))
        data = soup.find_all("li", class_="pull-left infoContent")

        # last page condition -- make sure not to miss the latest data posted during this scraping
        if len(data) < 30:   # occasionally there are <30 objects in a previous page, so need to take more care
            # total number of objects
            num_obj = int(soup.find_all("div", class_="pull-left hasData")[0].findChild().text.strip().replace(",", ""))
            if int(firstRow)+30 >= num_obj:
                lastp = 1

        dict_data = []  # dictionary to store the data in one page (<=30 objects)
        for i in range(len(data)):
            # 現況/房廳衛數/坪數/樓層
            info1 = [text for text in data[i].findChildren("p")[0].stripped_strings]
            type_home = info1[0]    # 現況
            area = [t for t in info1 if "坪" in t][0]        # 坪數
            # 樓層
            floor = [t for t in info1 if "樓" in t]
            if len(floor) == 1:          # 有樓層資訊
                floor = floor[0][3:]     # 去除"樓層："字串
            if len(floor) == 0:          # 無樓層資訊，如車位
                floor = ''
            # 物件為車位時的型態（其他情況定義下方"物件"頁部分）
            if type_home == "車位":
                type_bldg = [s for s in info1 if "類型" in s][0][3:]
            # 地址
            addr = data[i].findChildren("em")[0].get_text(strip=True)
            # 出租者身分/出租者
            info2 = [text for text in data[i].findChildren("p")[2].stripped_strings]
            [contact_id, contact] = info2[0].split(" ", 1)       # only split 1 times with space delimiter
            # (月)租費
            price = data[i].find_next("div").get_text(strip=True)

            ## "物件"頁
            req1 = requests.get("https:"+data[i].findChild("a")["href"])
            soup1 = BeautifulSoup(req1.text, "html5lib")
            #url1 = "https:"+data[i].findChild("a")["href"]
            #soup1 = BeautifulSoup(urllib.request.urlopen(url1))

            # 若物件基本資料為空則跳過
            info3 = soup1.find("ul", class_="clearfix labelList labelList-1")
            if info3 is None:
                #warnings.warn("Warning: This object is ignore due to lack of information.")
                print("Ignored object url:"+req1.url)
                continue
            # 性別要求
            if info3.findChild("em", title={re.compile("男"), re.compile("女")}) is None:     # 無寫明性別要求
                gender = ''
            else:
                gender = info3.findChild("em", title={re.compile("男"), re.compile("女")}).text
            # 連絡電話
            if soup1.find("span", class_="dialPhoneNum") is None:       # 無電話資訊
                phone = ""
            else:
                phone = soup1.find("span", class_="dialPhoneNum")["data-value"]
                if len(phone) == 0:     # 電話資訊為市話
                    phone = soup1.find("div", class_="hidtel").text
            # 型態（物件為車位時，已於上方處理）
            if type_home != "車位":
                info4 = str([text for text in soup1.find("ul", class_="attr").find_all("li")]).split("<li>")    # findChildren 有可能找不到故用 find_all
                info40 = [t for t in info4 if "型" in t][0]      # 字串包含"型態"的"型"的第一個結果
                type_bldg = re.sub(r"<.*?>", "", info40).replace("\xa0", "").replace(",", "").strip().split(":")[-1]    # 處理雜訊
                ## to circumvent strange parse results like "<li>型<ibdblfa></ibdblfa>態 :  透天厝</li>"
                #info4 = [text for text in soup1.find("ul", class_="attr").findChildren(string=re.compile(""))]
                #info4j = "".join(info4).replace("\xa0", "")
                #type_bldg = re.search("型(.*?)現", info4j).group(1).split()[-1]        # 在型態最前字(型)與現狀最前字(現)之間
                ##type_bldg = soup1.find("ul", class_="attr").findChild("li", string=re.compile("型態")).text.split()[-1]

            # 存取物件資料
            dict_data0 = {
            #             "_id": i,
                          "出租者": contact, "出租者身分": contact_id,
                          "城市": dict_city[urlJumpIp], "地址": addr, "聯絡電話": phone,
                          "型態": type_bldg, "樓層": floor, "坪數": area,
                          "現況": type_home, "性別要求": gender,
                          "租金": price
                         }
            # print(dict_data0)
            dict_data.append(dict_data0)
        col.insert_many(dict_data)
        if lastp == 1:     # last page
            break
        firstRow = str(int(firstRow) + 30)