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
    def __init__(self, google_cred_path, google_proj_id):
        # client init ga
        self.conn = GoogleAnalyticsQueryV4(secrets=google_cred_path)
        # client init bq
        self.cred = service_account.Credentials.from_service_account_file(google_cred_path)
        self.client = bigquery.Client(project=google_proj_id, credentials=self.cred)

    def ga(self, view):
        startDate = 4
        endDate = 0
        dfga = pd.DataFrame()
        dfga_temp = pd.DataFrame()
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
                    dfga_temp = self.conn.execute_query(query)
                    break
                except HttpError as msg:
                    log.exception(msg)
            dfga = pd.concat([dfga, dfga_temp], sort=False)
            startDate = startDate + 5
            endDate = endDate + 5
        dfga['eventLabel'] = dfga['eventLabel'].str.replace('[^0-9]', '', regex=True)
        return dfga

    def bq(self, site):
        # get all phones in 40 days
        # bq sql query and saving to df
        dfbq = self.client.query("""
            SELECT *
            FROM ones.invoice
            WHERE date >= '{}'
        """.format((datetime.now()-timedelta(15)).strftime('%Y-%m-%d'))).to_dataframe()

        # get all contacts from bq
        dfContact = self.client.query("""
            SELECT *
            FROM api.request
            WHERE date >= '{}'
        """.format((datetime.now()-timedelta(45)).strftime('%Y-%m-%d'))).to_dataframe()
        # strip empty rows
        dfContactCid = dfContact[~((dfContact.cid == '') | (dfContact.cid == '0') | (dfContact.cid.isna()))]
        # get timeframe (based on bq table updating frequency)
        # select sub dataframe by timeframe
        dfcurrent = dfbq[dfbq['date'] == datetime.now().strftime('%Y-%m-%d')]
        # dfcurrent = dfbq[dfbq['date'] == (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')]  # test
        # select sub dataframe by domain
        dfcurrentsite = dfcurrent[dfcurrent['site'].str.contains(site)]
        # create list with phones  from sub dataframe
        list_phones = list(dfcurrentsite['phone'].unique())
        return list_phones, dfContactCid

    @staticmethod
    def sender(dfga, list_phones, dfContactCid, tracker):
        cnt = 0
        for phone in list_phones:
            matcher = dfga[dfga['eventLabel'].str.contains(str(phone))]
            if len(matcher):
                cid = matcher['clientId'].iloc[0]
                cnt = cnt + 1
                data = event('Verified Order', cid)
                report(tracker, cid, data)
            else:
                cidMatch = dfContactCid[dfContactCid['phone'].str.contains(str(phone))]
                if len(cidMatch):
                    cid = cidMatch['cid'].iloc[0]
                    cnt = cnt + 1
                    data = event('Verified Order', cid)
                    report(tracker, cid, data)
        try:
            prc = cnt / len(list_phones) * 100
            prc = round(prc, 2)
        except ZeroDivisionError:
            prc = 'inf'
        log.info('{}, {} matched orders, {} all orders, {}% matched'.format(tracker, cnt, len(list_phones), prc))

    def full(self, view, site, tracker):
        dfga = self.ga(view)
        dfcurrentsite, dfContactCid = self.bq(site)
        self.sender(dfga, dfcurrentsite, dfContactCid, tracker)


if __name__ == '__main__':
    proc = ProcessData(GOOGLE_CRED_PATH, GOOGLE_PROJ_ID)
    for proj in PROJ:
        proc.full(**proj)
