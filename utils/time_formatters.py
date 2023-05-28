import math
from datetime import timedelta


def ms_to_min(ms):
    return str(timedelta(milliseconds=ms))[2:-3] if not math.isnan(ms) else ms


def ms_to_hours(ms):
    return str(timedelta(milliseconds=ms))[:-3]
