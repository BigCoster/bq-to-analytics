from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pandas as pd
from google_measurement_protocol import event, report
from config import GOOGLE_CRED_PATH, GOOGLE_PROJ_ID, PROJ, LOG_PATH
from googleapiclient.errors import HttpError
import logging.handlers
import asyncio
import concurrent.futures
from utils import resp2frame

import httplib2
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
_default_scope = ['https://www.googleapis.com/auth/analytics.readonly']

# conf logging
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
console = logging.StreamHandler()
console.setFormatter(formatter)
log.addHandler(console)
# filehandler = logging.handlers.TimedRotatingFileHandler('app.log', when='M', backupCount=7)
filehandler = logging.handlers.RotatingFileHandler(LOG_PATH, maxBytes=1048576, backupCount=5, encoding='utf-8')
filehandler.setFormatter(formatter)
log.addHandler(filehandler)


class BQ:
    def __init__(self, google_cred_path, google_proj_id, ):
        self.cred = service_account.Credentials.from_service_account_file(google_cred_path)
        self.client = bigquery.Client(project=google_proj_id, credentials=self.cred)

    def order_with_sid(self, in_sites=None, date=datetime.now()):
        """
        get order with sid
        :param in_sites: list of sites
        :param date: datetime
        :return: pandas.DataFrame
        """
        filter_sites = ""
        if in_sites:
            filter_sites = "and split(split(ord.site, ' ')[offset(0)], '_')[offset(0)] in ('{}')".format('\',\''.join(in_sites))
        df = self.client.query("""
            select ord.date, ord.doc, ord.site, ord.phone phone, coalesce(req.cid, con.cid, jiv.cid, comm.cid) cid, ord.amount
            from
            (select date, doc, substr(REGEXP_REPLACE(phone, '[^0-9]',''), -10) phone, 
            split(split(site, ' ')[offset(0)], '_')[offset(0)] site, amount
            from `vocal-framework-241518.ones.order_wo_probes` o) ord
            left join(
                select substr(REGEXP_REPLACE(phone, '[^0-9]',''), -10) phone, 
                split(split(site, ' ')[offset(0)], '_')[offset(0)] site, 
                array_agg(ar.cid order by ar.date desc limit 1)[OFFSET(0)] cid
                from `vocal-framework-241518.api.request` ar
                where length(ar.phone) >= 6
                and ar.cid is not null
                and ar.cid not in('0', '', 'cidTest')
                group by phone, site
            ) req on req.phone = ord.phone and req.site = ord.site
            left join(
                select substr(REGEXP_REPLACE(phone, '[^0-9]',''), -10) phone, 
                split(split(site, ' ')[offset(0)], '_')[offset(0)] site, 
                array_agg(c.cid order by c.date desc limit 1)[OFFSET(0)] cid
                from `vocal-framework-241518.ones.contact` c
                where length(c.phone) >= 6 
                and c.cid is not null 
                and c.cid not in ('0', '', 'cidTest')
                group by phone, site
            ) con on con.phone = ord.phone and con.site = ord.site
            left join(
                select substr(REGEXP_REPLACE(phone, '[^0-9]',''), -10) phone, 
                split(split(site, ' ')[offset(0)], '_')[offset(0)] site, 
                array_agg(ar.ga_cid order by ar.date desc limit 1)[OFFSET(0)] cid
                from `vocal-framework-241518.api.jivohooks` ar
                where length(ar.phone) >= 6
                and ar.ga_cid is not null
                and ar.ga_cid not in('0', '', 'cidTest')
                group by phone, site
            ) jiv on jiv.phone = ord.phone and jiv.site = ord.site
            left join (
                select substr(REGEXP_REPLACE(phone, '[^0-9]',''), -10) phone, 
                split(split(site, ' ')[offset(0)], '_')[offset(0)] site, 
                array_agg(ar.cid order by ar.date desc limit 1)[OFFSET(0)] cid
                from `vocal-framework-241518.eis.communication` ar
                where length(ar.phone) >= 6
                and ar.cid is not null
                and ar.cid not in('0', '', 'cidTest')
                group by phone, site            
            ) comm on comm.phone = ord.phone and comm.site = ord.site
            where ord.phone != ''
            and date(ord.date) = '{date}'
            {filter_sites}
        """.format(date=date.strftime('%Y-%m-%d'), filter_sites=filter_sites)
                                  ).to_dataframe()
        return df


class GA:
    def __init__(self, google_cred_path, projects, step=5, samples=9, retries=10):
        """
        get data from GA
        :param google_cred_path: client_secret.json path
        :param projects: list of parameters of projects
        :param step: days for one query
        :param samples: number of queries
        :param retries: retries get data from GA
        """
        self.credentials = ServiceAccountCredentials.from_json_keyfile_name(
            google_cred_path, _default_scope)
        # self.service = build('analyticsreporting', 'v4', credentials=self.credentials)
        self.projects = projects
        self.step = step
        self.samples = samples
        self.retries = retries
        self._task_args = None
        if not self.step > 0:
            raise Exception('step should be > 0')

    @property
    def task_args(self):
        if not self._task_args:
            view_site = [(p['view'], p['site']) for p in self.projects]
            self._task_args = [(v[0], str(a + self.step - 1), str(a), v[1]) for v in view_site for a in
                               range(0, self.step * self.samples, self.step)]
        return self._task_args

    def query_exec(self, view, ago_start, ago_end, site=None):
        """
        execute one query
        :param view: GA viewId
        :param ago_start: days ago
        :param ago_end: days ago
        :param site: for pas them to result
        :return: pandas.DataFrame
        """
        log.debug('{} ago_start_end({},{})'.format(site, ago_start, ago_end))
        df = pd.DataFrame()
        body = {
            'reportRequests': [{
                'viewId': view,
                'dateRanges': [{
                    'startDate': ago_start + 'daysAgo',
                    'endDate': ago_end + 'daysAgo'}],
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

        http = httplib2.Http()
        try:
            http = self.credentials.authorize(http)
        except HttpError as msg:
            log.exception('{} ago_start_end({},{})\n{}'.format(site, ago_start, ago_end, msg))
        for _ in range(self.retries):
            try:
                # resp = self.service.reports().batchGet(body=body).execute()
                service = build('analyticsreporting', 'v4', http=http)
                resp = service.reports().batchGet(body=body).execute()
                df = resp2frame(resp)
                df.rename(columns={'eventLabel': 'phone', 'clientId': 'cid'}, inplace=True)
                if df.empty:
                    break
                df.loc[:, 'site'] = site
                df['phone'] = df['phone'].str.replace('[^0-9]', '', regex=True)
                df['phone'] = df['phone'].str.slice(-10)
                break
            except HttpError as msg:
                log.error('{} ago_start_end({},{})\n{}'.format(site, ago_start, ago_end, msg))
        log.debug('{} ago_start_end({},{}) records({})'.format(site, ago_start, ago_end, df.shape[0]))
        return df

    def get_results(self):
        """
        make multiple queries and concat them all together
        :return: pandas.DataFrame
        """
        df = pd.DataFrame()
        results = [self.query_exec(*args) for args in self.task_args]
        for d in results:
            df = df.append(d, sort=False)
        return df

    async def query_exec_task(self, loop, executor):
        """
        get data for multiply queries coroutine
        :param loop: asyncio event_loop
        :param executor: ThreadPoolExecutor or ProcessPoolExecutor
        :return: pandas.DataFrame
        """
        done, pending = await asyncio.wait(
            fs=[loop.run_in_executor(executor, self.query_exec, *args) for args in self.task_args],
            return_when=asyncio.ALL_COMPLETED
        )
        return done

    def get_results_async(self):
        """
        run queries async
        :return: pandas.DataFrame
        """
        df = pd.DataFrame()
        loop = asyncio.get_event_loop()
        # with concurrent.futures.ProcessPoolExecutor() as executor:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            f_results = loop.run_until_complete(self.query_exec_task(loop, executor))
        results = [r.result() for r in f_results]
        for d in results:
            df = df.append(d, sort=False)
        return df


class GAEvent:
    def __init__(self, category):
        self.category = category

    def send_event(self, tracker, cid, doc, amount):
        resp = report(tracker, cid, event(category=self.category, action=cid, label=doc, value=amount))
        return resp

    async def send_event_task(self, loop, executor, events):
        done, pending = await asyncio.wait(
            fs=[loop.run_in_executor(executor, self.send_event,
                                     *(args['tracker'], args['cid'], args['doc'], args['amount'])) for args in events],
            return_when=asyncio.ALL_COMPLETED
        )
        return done

    def send_events_async(self, events):
        loop = asyncio.get_event_loop()
        # with concurrent.futures.ProcessPoolExecutor() as executor:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            f_results = loop.run_until_complete(self.send_event_task(loop, executor, events))
        results = [r.result() for r in f_results]
        return results


def process(date_time=datetime.now(), send=True, events_label='Verified Order'):
    log.info('orders for day: {}'.format(date_time.date()))
    execution_start = datetime.now()  # start time for debug
    # get data from BQ
    bq = BQ(GOOGLE_CRED_PATH, GOOGLE_PROJ_ID)
    sites = [p['site'] for p in PROJ]
    # orders with partial cid
    df_bq = bq.order_with_sid(sites, date_time)
    log.info('BQ orders: {} '.format(df_bq.shape[0]))

    # get data from GA
    ga = GA(GOOGLE_CRED_PATH, PROJ)
    # df_ga = ga.get_results()  # get data in synchronously way
    df_ga = ga.get_results_async()  # get data asynchronously
    log.info('GA orders: {} '.format(df_ga.shape[0]))
    # drop duplicates
    df_ga = df_ga.sort_values(by='date').groupby(['site', 'phone']).last().reset_index()

    # merge BQ and GA data
    df_cid = pd.merge(df_bq[['site', 'doc', 'phone', 'cid', 'amount']],
                      df_ga.reset_index()[['site', 'phone', 'cid']], how='left',
                      on=['site', 'phone'], copy=False, indicator='exist', suffixes=('_bq', '_ga'))
    df_cid['cid'] = df_cid['cid_bq'].where(~df_cid['cid_bq'].isna(), df_cid['cid_ga'], axis=0)
    # calc statistic
    for site, group in df_cid.groupby(['site']):
        rec_all = group.shape[0]
        rec_match = group[~group['cid'].isna()].shape[0]
        try:
            match_prc = round((rec_match / rec_all * 100), 2)
        except ZeroDivisionError:
            match_prc = 'inf'
        log.info('{} orders({}) matched({}) matched_prc({})%'.format(site, rec_all, rec_match, match_prc))

    # send data to GA
    if send:
        # append tracker by site to df
        tracker_map = {p['site']: p['tracker'] for p in PROJ}
        df_cid.loc[:, 'tracker'] = df_cid['site'].map(tracker_map)
        # only with cid
        events = df_cid[['tracker', 'cid', 'doc', 'amount']][~df_cid['cid'].isna()].fillna(0).round(0).to_dict('records')
        # all
        # events = df_cid[['tracker', 'cid']].to_dict('records')

        # send events to GA
        gae = GAEvent(events_label)
        res = gae.send_events_async(events)
        log.info('send GA events: {}'.format(len(res)))
        errors = {r[0].status_code for r in res}.difference((200,))
        if errors:
            log.error('send some evens fails with status codes {}'.format(errors))
    log.info('exec time: {}'.format(datetime.now()-execution_start))


def main():
    dt = datetime.now()
    # dt = datetime(year=2021, month=12, day=19)  # manual seeding
    events_label = 'Verified Order'
    process(date_time=dt, send=True, events_label=events_label)


def main_debug():
    # for testing
    dt = datetime.now() - timedelta(1)
    # dt = datetime(year=2021, month=12, day=15)  # manual seeding
    log.setLevel(logging.DEBUG)
    events_label = 'Verified Order Testing'
    process(date_time=dt, send=True, events_label=events_label)


if __name__ == '__main__':
    # main_debug()
    main()
