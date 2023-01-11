import concurrent
from datetime import timedelta, date
import calendar
from typing import List

from tqdm.auto import tqdm

from fetch import fetch
from parquet_dumper import ParquetDumper
from processor import decompress
from utils import Logger


def days(start: date, end: date) -> List[date]:
    """Generator function that a list of possible trading dates from `start` to `end` (inclusive).

       Saturday and today's date are skipped.

       Args:
           start (date): The start date.
           end (date): The end date.

       Returns:
           List[date]: A possible trading date between `start` and `end`.
       """
    if start > end:
        return []
    end = end + timedelta(days=1)
    today = date.today()
    days = []
    while start != end:
        if start.weekday() != calendar.SATURDAY and start != today:
            days.append(start)
        start = start + timedelta(days=1)
    return days


def how_many_days(start: date, end: date) -> int:
    """Returns the number of possible trading days between `start` and `end` (inclusive).

        Args:
            start (date): The start date.
            end (date): The end date.

        Returns:
            int: The number of days between `start` and `end`.
        """
    return sum(1 for _ in days(start, end))


def app(symbols: List[str], start: date, end: date, threads: int, folder: str) -> None:
    """Fetches and processes data for the given symbols, dates, and threads, and stores the results in the given folder.

        Args:
            symbols (List[str]): The symbols to fetch data for.
            start (date): The start date.
            end (date): The end date.
            threads (int): The number of threads to use.
            folder (str): The folder to store the results in.
        """
    if start > end:
        return
    total_days = how_many_days(start, end) * len(symbols)

    if total_days == 0:
        return

    with tqdm(total=total_days) as progress_bar:
        for symbol in symbols:
            with ParquetDumper(symbol, start, end, folder) as file:
                for data, day in fetch(symbol, days(start, end), progress_bar):
                    file.append(day, decompress(day, data))
    Logger.info("Fetching data terminated")

