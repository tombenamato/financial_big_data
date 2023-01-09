import concurrent
from datetime import timedelta, date
import calendar
from typing import List

from tqdm.auto import tqdm

from fetch import fetch_day
from parquet_dumper import ParquetDumper
from processor import decompress
from utils import Logger


def days(start: date, end: date) -> List[date]:
    if start > end:
        return []
    end = end + timedelta(days=1)
    today = date.today()
    while start != end:
        if start.weekday() != calendar.SATURDAY and start != today:
            yield start
        start = start + timedelta(days=1)


def how_many_days(start: date, end: date) -> int:
    return sum(1 for _ in days(start, end))


def do_work(symbol: str, day: date, parquet: ParquetDumper) -> None:
    Logger.info("Fetching day {0}".format(day))
    parquet.append(day, decompress(day, fetch_day(symbol, day)))


def app(symbols: List[str], start: date, end: date, threads: int, folder: str) -> None:
    if start > end:
        return
    total_days = how_many_days(start, end) * len(symbols)

    if total_days == 0:
        return

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor, \
            tqdm(total=total_days) as progress_bar:
        for symbol in symbols:
            with ParquetDumper(symbol, start, end, folder) as file:
                work_queue = [(symbol, day) for day in days(start, end)]
                futures = [executor.submit(do_work, symbol, day, file) for symbol, day in work_queue]
                for future in concurrent.futures.as_completed(futures):
                    if future.exception() is None:
                        progress_bar.update(1)
                    else:
                        Logger.error("An error happened when fetching data: ", future.exception())
    Logger.info("Fetching data terminated")

