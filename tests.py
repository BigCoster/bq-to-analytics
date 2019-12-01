import unittest
from datetime import datetime, timedelta
from bq2ga import process


class Testing(unittest.TestCase):
    def setUp(self) -> None:
        self.dt = datetime.now() - timedelta(1)

    def test_main(self):
        process(date_time=self.dt, send=True, events_label='Verified Order Testing')
