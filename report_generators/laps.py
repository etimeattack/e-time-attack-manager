import pandas
import pandas as pd

from report_generators.result_parser import ResultParser
from utils.time_formatters import ms_to_min, ms_to_hours

EVENTS_TABLE_NAME = 'laps'


class LapsParser(ResultParser):

    def __init__(self, event_id, report_name, sectors_amount=3):
        super().__init__(event_id, report_name)
        self.sectors_amount = sectors_amount

    def generate(self):
        self.load_table(EVENTS_TABLE_NAME, self.map_laps())
        per_driver_events = self.transform()

        self.save_report(per_driver_events)

    def transform(self) -> pandas.DataFrame:
        laps_enriched = self.enrich_base_laps_table()
        theoretical_best_addition = '+'.join([f'min(S{s})' for s in self._get_sectors_range()])
        optimal_sectors_columns = ", ".join([f'min(S{s}) as OptimalS{s}' for s in self._get_sectors_range()])

        sql = f"""
            WITH fastest_times_num AS (
                SELECT DriverName, LapTime, LapNum, row_number() OVER (PARTITION BY DriverName ORDER BY LapTime) as FastestLapNum
                FROM laps_enriched
                WHERE Valid
            ),
            ftn as (
                SELECT DriverName, LapTime as FastestLap, cast(LapNum as int) as FastestLapAt
                FROM fastest_times_num
                WHERE FastestLapNum = 1
            ),
            valid_lap_stats as (
                SELECT le.DriverName,
                    {theoretical_best_addition} as TheoreticalBest,
                    {optimal_sectors_columns}
                FROM laps_enriched le
                WHERE le.Valid
                GROUP BY 1        
            ),
            overall_stats as (
                SELECT le.DriverName,
                    sum(LapTime)::int as TotalDrivingTime, 
                    count(*) as TotalLaps,
                    sum(case when Valid then 0 else 1 end)::int as InvalidLaps,
                FROM laps_enriched le
                GROUP BY 1
            )
            SELECT * 
            FROM overall_stats os
            LEFT JOIN ftn 
                ON os.DriverName = ftn.DriverName
            LEFT JOIN valid_lap_stats vls
                ON os.DriverName = vls.DriverName
        """

        df = self.cursor.sql(sql).fetchdf()
        optimal_sectors_cols = [f'OptimalS{i}' for i in self._get_sectors_range()]
        for col in ['FastestLap', 'TheoreticalBest'] + optimal_sectors_cols:
            df[col] = df[col].apply(ms_to_min)

        df['TotalDrivingTime'] = df['TotalDrivingTime'].apply(ms_to_hours)
        del df['DriverName_2']
        del df['DriverName_3']

        return df

    def enrich_base_laps_table(self) -> pandas.DataFrame:
        sql = """
           SELECT *, 
                row_number() OVER (PARTITION BY DriverName ORDER BY Timestamp) as LapNum, 
                Cuts = 0 as Valid
           FROM laps
           ORDER BY LapTime asc
        """

        df = self.cursor.sql(sql).fetchdf()
        df[[f'S{s}' for s in self._get_sectors_range()]] = df.Sectors.tolist()

        for s in range(1, self.sectors_amount + 1):
            df[f'S{s}'] = pd.to_numeric(df[f'S{s}'])

        del df['Sectors']
        return df

    def map_laps(self):
        return self.event_result['Laps']

    def _get_sectors_range(self):
        return range(1, self.sectors_amount + 1)
