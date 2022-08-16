import requests
from operator import itemgetter
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
from googleapiclient import discovery
from google.oauth2 import service_account
from googleapiclient.discovery import build
import logging
import os
import json
import datetime as dt


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
log_dir = os.path.join(BASE_DIR, 'logs/')
log_file = os.path.join(BASE_DIR, 'logs/pars_table.log')
console_handler = logging.StreamHandler()
file_handler = RotatingFileHandler(
    log_file,
    maxBytes=100000,
    backupCount=3,
    encoding='utf-8'
)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s, %(levelname)s, %(message)s',
    handlers=(
        file_handler,
        console_handler
    )
)

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
CREDENTIALS_FILE = 'credentials_service.json'
credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE)
service = discovery.build('sheets', 'v4', credentials=credentials)
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')

if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
load_dotenv('.env ')
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')

def get_feedback(rootId, last_day, needed_valuation=None):
    count = 0
    raw_data = {"imtId": rootId, "skip": 0, "take": 30, "order": "dateDesc"}
    url = "https://public-feedbacks.wildberries.ru/api/v1/summary/full"
    response = requests.post(
        url=url, headers={"Content-Type": "application/json"}, json=raw_data
    )
    response_message = response.json()
    for items in response_message["feedbacks"]:
        # print(items)
        date = items["createdDate"][:10].replace("T", " ")
        if int(items["productValuation"]) != needed_valuation or not needed_valuation:
            if date in last_day:
                count+=1
    return count


def search_rootId(imtId):
    url = (
            "https://card.wb.ru/cards/detail?spp=0&regions=68,64,83,4,38,80,33,70,82,86,75,30,69,22,66,31,48,1,40,71&stores=117673,122258,122259,125238,125239,125240,6159,507,3158,117501,120602,120762,6158,121709,124731,159402,2737,130744,117986,1733,686,132043&pricemarginCoeff=1.0&reg=0&appType=1&emp=0&locale=ru&lang=ru&curr=rub&couponsGeo=12,3,18,15,21&dest=-1029256,-102269,-1278703,-1255563&nm="
            + str(imtId)
            + ";64245978;64245979%27"
    )
    response = requests.get(url=url)
    response_message = response.json()
    for item in response_message["data"]["products"]:
        if item["id"] == imtId:
            rootId = int(item["root"])
    return rootId


def get_list_articles(table_id, month, year):
    list_article = []
    service = build('sheets', 'v4', credentials=credentials)
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=table_id,
                                range=f'{month}.{year}', majorDimension='ROWS').execute()
    values = result.get('values', [])
    try:
        if not values:
            logging.info('No data found.')
        else:
            for row in values[2:]:
                if row[2] == 'ТОП 10 уже' or row[2] == 'ТОП 5 продвижение':
                    list_article.append(row[6])
    except:
        pass
    return list_article


if __name__ == "__main__":
    last_day = str(dt.datetime.date(dt.datetime.now()) - dt.timedelta(days=1))
    table_id = SPREADSHEET_ID
    get_list_articles(table_id)
    spisok = get_feedback(search_rootId(83818558),last_day)
    print(spisok)