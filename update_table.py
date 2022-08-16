import json
from logging.handlers import RotatingFileHandler

from dotenv import load_dotenv
from googleapiclient import discovery
from google.oauth2 import service_account
from googleapiclient.discovery import build
import logging
import os
import datetime as dt

import warnings

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
log_dir = os.path.join(BASE_DIR, 'logs/')
log_file = os.path.join(BASE_DIR, 'logs/pars_stocks_table.log')
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
START_POSITION_FOR_PLACE = 1

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')

if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
load_dotenv('.env ')
SPREADSHEET_REVIEW = os.getenv('SPREADSHEET_REVIEW')


def convert_to_column_letter(column_number):
    column_letter = ''
    while column_number != 0:
        c = ((column_number - 1) % 26)
        column_letter = chr(c + 65) + column_letter
        column_number = (column_number - c) // 26
    return column_letter


def update_table_article_WB(table_id, article_dicts):
    position_for_place = START_POSITION_FOR_PLACE
    service = build('sheets', 'v4', credentials=credentials)
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=table_id,
                                range=range_name, majorDimension='ROWS').execute()
    values = result.get('values', [])
    i = 2
    body_data = []
    if not values:
        logging.info('No data found.')
    else:

        try:
            if len(article_dicts) != 0:
                for keyword in article_dicts:
                    value = keyword
                    body_data += [
                        {'range': f'{range_name}!{convert_to_column_letter(position_for_place + 1)}{i}',
                         'values': [[f'{value}']]}]
                    i += 1
                    body = {
                        'valueInputOption': 'USER_ENTERED',
                        'data': body_data}
        except Exception as e:
            print(e)
    sheet.values().batchUpdate(spreadsheetId=table_id, body=body).execute()


def update_table_article(table_id, article_dicts):
    position_for_place = START_POSITION_FOR_PLACE
    service = build('sheets', 'v4', credentials=credentials)
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=table_id,
                                range=range_name, majorDimension='ROWS').execute()
    values = result.get('values', [])
    i = 2
    body_data = []
    if not values:
        logging.info('No data found.')
    else:
        for row in values[1:]:
            try:
                article_WB = row[1].strip().upper()
                value = article_dicts.get(article_WB)['supplierArticle']
                body_data += [
                    {'range': f'{range_name}!{convert_to_column_letter(position_for_place)}{i}',
                     'values': [[f'{value}']]}]
            except:
                pass
            finally:
                i += 1
                body = {
                    'valueInputOption': 'USER_ENTERED',
                    'data': body_data}
    sheet.values().batchUpdate(spreadsheetId=table_id, body=body).execute()


def update_table_review(table_id, article_dicts):
    day = date_from.strftime('%d')
    month = date_from.strftime('%m')
    date = f'{day}.{month}'

    service = build('sheets', 'v4', credentials=credentials)
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=table_id,
                                range=range_name, majorDimension='ROWS').execute()
    result = result.get('values', [])
    row = 1
    body_data = []
    if not result:
        logging.info('No data found.')
    else:
        for values in result:
            if date in values:
                column = values.index(date)+1
            try:
                article_WB = values[1].strip().upper()
                if article_WB != 'Итого за день':
                    value = article_dicts.get(article_WB)['review']
                    body_data += [
                        {'range': f'{range_name}!{convert_to_column_letter(column)}{row}',
                         'values': [[f'{value}']]}]
            except Exception as e:
                pass
            finally:
                row += 1
                body = {
                    'valueInputOption': 'USER_ENTERED',
                    'data': body_data}
    sheet.values().batchUpdate(spreadsheetId=table_id, body=body).execute()




if __name__ == '__main__':
    date_from = dt.datetime.date(dt.datetime.now()-dt.timedelta(days=1))
    range_name = 'Август 2022'
    table_id = SPREADSHEET_REVIEW
    with open('article_dicts.json', encoding='UTF-8') as f:
        article_dicts = json.load(f)
    update_table_article_WB(table_id, article_dicts)
    update_table_article(table_id, article_dicts)
    update_table_review(table_id, article_dicts)
