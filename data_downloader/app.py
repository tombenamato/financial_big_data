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
    """Generator function that yields possible trading dates from `start` to `end` (inclusive).

       Saturday and today's date are skipped.

       Args:
           start (date): The start date.
           end (date): The end date.

       Yields:
           date: A possible trading date between `start` and `end`.
       """
    if start > end:
        return []
    end = end + timedelta(days=1)
    today = date.today()
    while start != end:
        if start.weekday() != calendar.SATURDAY and start != today:
            yield start
        start = start + timedelta(days=1)


def how_many_days(start: date, end: date) -> int:
    """Returns the number of possible trading days between `start` and `end` (inclusive).

        Args:
            start (date): The start date.
            end (date): The end date.

        Returns:
            int: The number of days between `start` and `end`.
        """
    return sum(1 for _ in days(start, end))


def do_work(symbol: str, day: date, parquet: ParquetDumper) -> None:
    Logger.info("Fetching day {0}".format(day))
    try :
        parquet.append(day, decompress(day, fetch_day(symbol, day)))
    except Exception as e:
        do_work(symbol, day, parquet)


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

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor, \
            tqdm(total=total_days) as progress_bar:
        for symbol in symbols:
            with ParquetDumper(symbol, start, end, folder) as file:
                futures = [executor.submit(do_work, symbol, day, file) for day in days(start, end)]
                for future in concurrent.futures.as_completed(futures):
                    if future.exception() is None:
                        progress_bar.update(1)
                    else:
                        Logger.error("An error happened when fetching data: ", future.exception())
    Logger.info("Fetching data terminated")

