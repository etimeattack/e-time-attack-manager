import logging
import json
import os

import duckdb
import pandas as pd
import requests

from config import cfg

logger = logging.getLogger(__name__)


class ResultParser:
    def __init__(self, event_id, report_name, report_format='csv', cache_folder=cfg.tmp_path, result_url=None):
        assert report_format == 'csv', 'Only csv format is supported as for now'
        self.event_id = event_id
        self.report_name = report_name
        self.report_format = report_format
        self._cache_folder = cache_folder
        self.raw_file_path = f"{cfg.raw_restuls_path}/ta-{event_id}.json"
        self.result_url = result_url
        self.event_result = self.read_event_result()
        self.cursor = duckdb.connect()

    def read_event_result(self):
        if not os.path.isfile(self.raw_file_path) and self.result_url:
            return self.get_and_save_event_result(self.result_url)

        return json.load(open(self.raw_file_path, 'r'))

    def get_and_save_event_result(self, url):
        res = requests.get(url).json()
        file_path = f"{cfg.raw_restuls_path}/ta-{self.event_id}.json"
        json.dump(res, open(file_path, 'w+'))
        return res

    def generate(self):
        raise NotImplementedError("This must be implemented in a subclass")

    def load_table(self, table_name, rows):
        df = pd.json_normalize(rows)
        self.cursor.sql(f"CREATE TABLE {table_name} AS SELECT * FROM df")

    def save_report(self, pd):
        folder_path = f"{cfg.summaries_path}/ta-{self.event_id}"
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        report_path = f"{folder_path}/{self.report_name}.{self.report_format}"
        pd.to_csv(report_path, index=False)
        logger.info(f"Report {self.report_name} saved successfully, path: {report_path}")
