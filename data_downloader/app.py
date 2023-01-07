import concurrent
import threading
import time
from collections import deque
from datetime import timedelta, date

from fetch import fetch_day
from parquet_dumper import ParquetDumper
from processor import decompress
from utils import is_debug_mode, TimeFrame, Logger

SATURDAY = 5
day_counter = 0


def days(start, end):
    if start > end:
        return
    end = end + timedelta(days=1)
    today = date.today()
    while start != end:
        if start.weekday() != SATURDAY and start != today:
            yield start
        start = start + timedelta(days=1)


def format_left_time(seconds):
    if seconds < 0:
        return "--:--:--"
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return "%d:%02d:%02d" % (h, m, s)


def update_progress(done, total, avg_time_per_job, threads):
    progress = 1 if total == 0 else done / total
    progress = int((1.0 if progress > 1.0 else progress) * 100)
    remainder = 100 - progress
    estimation = (avg_time_per_job * (total - done) / threads)
    if not is_debug_mode():
        print('\r[{0}] {1}%  Left : {2}  '.format('#' * progress + '-' * remainder, progress,
                                                  format_left_time(estimation)), end='')


def how_many_days(start, end):
    return sum(1 for _ in days(start, end))


def avg(fetch_times):
    if len(fetch_times) != 0:
        return sum(fetch_times) / len(fetch_times)
    else:
        return -1


def name(symbol, timeframe, start, end):
    ext = ".csv"

    for x in dir(TimeFrame):
        if getattr(TimeFrame, x) == timeframe:
            ts_str = x

    name = symbol + "_" + ts_str + "_" + str(start)

    if start != end:
        name += "_" + str(end)

    return name + ext


def app(symbols, start, end, threads, timeframe, folder, header):
    if start > end:
        return
    lock = threading.Lock()
    global day_counter
    total_days = how_many_days(start, end) * len(symbols)

    if total_days == 0:
        return

    last_fetch = deque([], maxlen=10)
    update_progress(day_counter, total_days, -1, threads)

    def do_work(symbol, day, csv):
        global day_counter
        star_time = time.time()
        Logger.info("Fetching day {0}".format(day))
        try:
            csv.append(day, decompress(symbol, day, fetch_day(symbol, day)))
        except Exception as e:
            print("ERROR for {0}, {1} Exception : {2}".format(day, symbol, str(e)))
        elapsed_time = time.time() - star_time
        last_fetch.append(elapsed_time)
        with lock:
            day_counter += 1
        Logger.info("Day {0} fetched in {1}s".format(day, elapsed_time))

    futures = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:

        files = {symbol: ParquetDumper(symbol, timeframe, start, end, folder, header) for symbol in symbols}

        for day in days(start, end):
            for symbol in symbols:
                futures.append(executor.submit(do_work, symbol, day, files[symbol]))

        for future in concurrent.futures.as_completed(futures):
            if future.exception() is None:
                update_progress(day_counter, total_days, avg(last_fetch), threads)
            else:
                Logger.error("An error happen when fetching data : ", future.exception())

        Logger.info("Fetching data terminated")
        for file in files.values():
            file.dump()

    update_progress(day_counter, total_days, avg(last_fetch), threads)
