import logging
import json
import duckdb
import pandas as pd

from config import cfg

logger = logging.getLogger(__name__)


class ResultParser:
    def __init__(self, event_id, report_name, report_format='csv', cache_folder=cfg.tmp_path):
        assert report_format == 'csv', 'Only csv format is supported as for now'
        self.event_id = event_id
        self.event_result = self.read_event_result(event_id)
        self.report_name = report_name
        self.report_format = report_format
        self.cursor = duckdb.connect()
        self._cache_folder = cache_folder

    @staticmethod
    def read_event_result(event_id):
        file_path = f"{cfg.raw_restuls_path}/ta-{event_id}.json"
        return json.load(open(file_path))

    def generate(self):
        raise NotImplementedError("This must be implemented in a subclass")

    def load_table(self, table_name, rows):
        df = pd.json_normalize(rows)
        self.cursor.sql(f"CREATE TABLE {table_name} AS SELECT * FROM df")

    def save_report(self, pd):
        report_path = f"{cfg.summaries_path}/ta-{self.event_id}/{self.report_name}.{self.report_format}"
        pd.to_csv(report_path, index=False)
        logger.info(f"Report {self.report_name} saved successfully, path: {report_path}")
