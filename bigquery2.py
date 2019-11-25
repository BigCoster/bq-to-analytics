from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
from google2pandas import GoogleAnalyticsQueryV4
import pandas as pd
from google_measurement_protocol import event, report
from config import GOOGLE_CRED_PATH, GOOGLE_PROJ_ID, PROJ, LOG_PATH
from googleapiclient.errors import HttpError
import logging.handlers

# conf logging
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
console = logging.StreamHandler()
console.setFormatter(formatter)
filehandler = logging.handlers.RotatingFileHandler(LOG_PATH, maxBytes=1048576, backupCount=5, encoding='utf-8')
# filehandler = logging.handlers.TimedRotatingFileHandler('app.log', when='M', backupCount=7)

filehandler.setFormatter(formatter)
log.addHandler(console)
log.addHandler(filehandler)


class ProcessData:
    def __init__(self, google_cred_path, google_proj_id, date_time):
        # client init ga
        self.conn = GoogleAnalyticsQueryV4(secrets=google_cred_path)
        # client init bq
        self.cred = service_account.Credentials.from_service_account_file(google_cred_path)
        self.client = bigquery.Client(project=google_proj_id, credentials=self.cred)
        self.dt = date_time

    def ga(self, view):
        startDate = 4
        endDate = 0
        df_ga = pd.DataFrame()
        df_ga_temp = pd.DataFrame()
        for i in range(5):
            query = {
                'reportRequests': [{
                    'viewId': view,
                    'dateRanges': [{
                        'startDate': str(startDate) + 'daysAgo',
                        'endDate': str(endDate) + 'daysAgo'}],
                    'dimensions': [
                        {'name': 'ga:clientId'},
                        {'name': 'ga:eventLabel'},
                        {'name': 'ga:date'}
                    ],
                    'metrics': [
                        {'expression': 'ga:uniqueEvents'}],
                    'dimensionFilterClauses': [{
                        'filters': [
                            {'dimensionName': 'ga:eventCategory',
                                'operator': 'EXACT',
                                'expressions': ['Order']}]
                    }]
                }]
            }

            for _ in range(10):
                try:
                    df_ga_temp = self.conn.execute_query(query)
                    break
                except HttpError as msg:
                    log.exception(msg)
            df_ga = pd.concat([df_ga, df_ga_temp], sort=False)
            startDate = startDate + 5
            endDate = endDate + 5
        df_ga['eventLabel'] = df_ga['eventLabel'].str.replace('[^0-9]', '', regex=True)
        return df_ga

    def bq(self, site):
        # get all orders from bq
        df_order = self.client.query("""
            SELECT *
            FROM ones.order
            WHERE DATE(date) = '{0}'
              AND site LIKE  '{1}%'
         """.format(self.dt.strftime('%Y-%m-%d'), site)
                                 ).to_dataframe()

        # get api.request from bq only with cid
        df_api = self.client.query("""
            SELECT *
            FROM api.request
            WHERE date >= '{0}'
              AND site LIKE  '{1}%'
              AND cid IS NOT NULL
              AND cid != '0'
              AND cid != ''
        """.format((self.dt-timedelta(45)).strftime('%Y-%m-%d'), site)
                                  ).to_dataframe()
        order_phones = list(df_order['phone'].unique())
        return order_phones, df_api

    @staticmethod
    def sender(tracker, order_phones, df_ga, df_api):
        cnt = 0
        for phone in order_phones:
            cid = None
            # check if phone exist in GA
            match = df_ga[df_ga['eventLabel'].str.contains(phone)]
            if len(match):
                cid = match['clientId'].iloc[0]
            elif not df_api.empty:
                # check if phone exist in BQ api.requests
                match = df_api[df_api['phone'].str.contains(phone)]
                if len(match):
                    cid = match['cid'].iloc[0]
            if cid:
                cnt = cnt + 1
                # send event to GA
                report(tracker, cid, event('Verified Order', cid))
        try:
            prc = cnt / len(order_phones) * 100
            prc = round(prc, 2)
        except ZeroDivisionError:
            prc = 'inf'
        log.info('{}, {} matched orders, {} all orders, {}% matched'.format(tracker, cnt, len(order_phones), prc))

    def full(self, view, site, tracker):
        df_ga = self.ga(view)
        order_phones, df_api = self.bq(site)
        self.sender(tracker, order_phones, df_ga, df_api)


if __name__ == '__main__':
    date_time = datetime.now()
    # date_time = date_time - timedelta(2)  # testing
    proc = ProcessData(GOOGLE_CRED_PATH, GOOGLE_PROJ_ID, date_time)
    for proj in PROJ:
        proc.full(**proj)
