import asyncio
from contextlib import ExitStack
from datetime import timedelta, date
import calendar
from typing import List

from tqdm.auto import tqdm

from fetch import fetch, fetch_synchrone
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


async def process_and_save_data(symbol: str, data: bytes, day: date, file: ParquetDumper):
    """
    Processes and save the data for the given symbol and date, decompressing it and appending it to the provided file.
    If an exception is encountered, the function will retry the download and continue processing the data.

    Parameters:
    symbol (str): The symbol of the financial asset.
    data (bytes): The binary data to be processed.
    day (date): The date of the data.
    file (ParquetDumper): The file to append the processed data to.
    """
    try :
        decompressed_data = await asyncio.get_event_loop().run_in_executor(None,decompress,symbol, day,data)
        file.append(day, decompressed_data)
    except Exception as e:
        print(f"Retry download for {symbol} at {day}")
        retry_response = fetch_synchrone(symbol, day)
        print(f"Download finish : continue processing")
        process_and_save_data(symbol, retry_response, day, file)

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
            responses = fetch(symbols, days(start, end), progress_bar, threads)
            with ExitStack() as stack:
                files = [stack.enter_context(ParquetDumper(symbol, start, end, folder)) for symbol in symbols]
                loop = asyncio.get_event_loop()
                tasks = [loop.create_task(process_and_save_data(symbol, tuple_data_day[0], tuple_data_day[1], file))
                         for symbol_responses, file, symbol in zip(responses, files, symbols)
                         for tuple_data_day in symbol_responses]
                loop.run_until_complete(asyncio.gather(*tasks))
    Logger.info("Fetching data terminated")

