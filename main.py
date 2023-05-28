import json
import os

import duckdb
import pandas
import pandas as pd

from utils.time_formatters import ms_to_min, ms_to_hours

RANKS_TABLE_NAME = 'ranks'
EVENTS_TABLE_NAME = 'events'
COLUMNS = ['BallastKG', 'CarId', 'CarModel', 'ClassID', 'ContributedToFastestLap',
           'Cuts', 'DriverGuid', 'DriverName', 'LapTime', 'Restrictor', 'Sectors', 'Timestamp', 'Tyre', 'SpeedTrapHits']

COLUMNS_ENRICHED = ['DriverName', 'LapTime', 'LapTimeMins', 'Timestamp', 'S1', 'S2', 'S3', 'LapNum']


def load_ranks_file(cursor: duckdb.DuckDBPyConnection, ta_event: int, table_name: str = RANKS_TABLE_NAME):
    cursor.sql(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('results/ta-{ta_event}-laps.csv');")


def load_events_file(cursor: duckdb.DuckDBPyConnection, ta_event: int, table_name: str = EVENTS_TABLE_NAME):
    cursor.sql(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('results/ta-{ta_event}-events.csv');")


def transform_base_laps_table(cursor: duckdb.DuckDBPyConnection) -> pandas.DataFrame:
    sql = """
        SELECT DriverName, row_number() OVER (PARTITION BY DriverName ORDER BY Timestamp) as LapNum, Timestamp, LapTime, 
            Sectors, Cuts, Cuts = 0 as Valid
        FROM ranks
        ORDER BY LapTime asc
    """
    df = cursor.sql(sql).fetchdf()
    df["LapTimeMins"] = df["LapTime"].apply(ms_to_min)
    df.Sectors = df.Sectors.str[1:-1]

    sectors_amount = 3
    df[[f'S{s}' for s in range(1, sectors_amount + 1)]] = df.Sectors.str.split(",", expand=True)

    for s in range(1, sectors_amount + 1):
        df[f'S{s}'] = pd.to_numeric(df[f'S{s}'])
        df[f'S{s}Mins'] = df[f'S{s}'].apply(ms_to_min)

    del df['Sectors']

    return df


def transform_base_events_table(cursor: duckdb.DuckDBPyConnection) -> pandas.DataFrame:
    sql = """
        SELECT DriverName, Type, count(*)
        FROM events
        GROUP BY DriverName, Type
        ORDER BY 3 desc
    """
    df = cursor.sql(sql).fetchdf()
    return df


def calculate_per_driver_results(laps_enriched: pandas.DataFrame):
    sql = """
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
                min(S1) + min(S2) + min(S3) as TheoreticalBest,
                min(S1) as OptimalS1,
                min(S2) as OptimalS2,
                min(S3) as OptimalS3,
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

    df = duckdb.sql(sql).fetchdf()
    for col in ['FastestLap', 'TheoreticalBest', 'OptimalS1', 'OptimalS2', 'OptimalS3']:
        df[col] = df[col].apply(ms_to_min)

    df['TotalDrivingTime'] = df['TotalDrivingTime'].apply(ms_to_hours)

    del df['DriverName_2']
    del df['DriverName_3']

    return df


def convert_raw_to_csv_file(ta_event):
    file_path = f"results/raw/ta-{ta_event}.json"
    out_path_laps = f"results/ta-{ta_event}-laps.csv"
    out_path_events = f"results/ta-{ta_event}-events.csv"

    res = json.load(open(file_path))
    laps = res['Laps']
    df = pd.json_normalize(laps)
    df.to_csv(out_path_laps)

    laps = res['Events']

    for lap in laps:
        lap['DriverName'] = lap['Driver']['Name']
        del lap['Driver']

    df = pd.json_normalize(laps)
    df.to_csv(out_path_events)


def run(ta_event: int):
    convert_raw_to_csv_file(ta_event)

    cursor = duckdb.connect()
    load_ranks_file(cursor, ta_event=ta_event)
    load_events_file(cursor, ta_event=ta_event)

    dir = f"summaries/ta-{ta_event}"
    if not os.path.exists(dir):
        os.mkdir(dir)

    laps_enriched = transform_base_laps_table(cursor)
    laps_enriched.to_csv(f"summaries/ta-{ta_event}/ranks_enriched_base.csv", index=False)

    per_driver_stats = calculate_per_driver_results(laps_enriched)
    per_driver_stats.to_csv(f"summaries/ta-{ta_event}/per_driver_stats.csv", index=False)

    per_driver_events = transform_base_events_table(cursor)
    per_driver_events.to_csv(f"summaries/ta-{ta_event}/per_driver_events.csv", index=False)


run(7)
