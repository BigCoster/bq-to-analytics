import unittest
from bigquery2 import ProcessData
from config import GOOGLE_CRED_PATH, GOOGLE_PROJ_ID, PROJ, LOG_PATH
from datetime import datetime, timedelta


class Testing(unittest.TestCase):
    def setUp(self) -> None:
        self.dt = datetime.now() - timedelta(1)

    def test_match(self):
        proc = ProcessData(GOOGLE_CRED_PATH, GOOGLE_PROJ_ID, self.dt, send=False)
        for proj in PROJ:
            proc.full(**proj)
