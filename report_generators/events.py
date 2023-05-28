import pandas

from report_generators.result_parser import ResultParser

EVENTS_TABLE_NAME = 'events'


class EventsParser(ResultParser):

    def generate(self):
        self.load_table(EVENTS_TABLE_NAME, self.map_events())
        per_driver_events = self.transform_events()
        self.save_report(per_driver_events)
        return per_driver_events

    def transform_events(self) -> pandas.DataFrame:
        sql = """
            SELECT DriverName, Type, count(*)
            FROM events
            GROUP BY DriverName, Type
            ORDER BY 3 desc
        """

        return self.cursor.sql(sql).fetchdf()

    def map_events(self):
        events = self.event_result['Events']

        for ev in events:
            ev['DriverName'] = ev['Driver']['Name']
            del ev['Driver']

        return events
