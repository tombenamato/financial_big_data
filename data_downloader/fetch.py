import asyncio
from asyncio import Semaphore
from queue import Queue
import time
from multiprocessing import Manager
from datetime import date
from typing import Tuple, List

import aiohttp
import requests
from aiohttp import ClientSession
from tqdm import tqdm

from utils import Logger
from threading import Lock

MIN_PROXY = 50

PROXY_ULTRA_FAST_URL = "https://raw.githubusercontent.com/saschazesiger/Free-Proxies/master/proxies/ultrafast.txt"
PROXY_FAST_URL = "https://raw.githubusercontent.com/saschazesiger/Free-Proxies/master/proxies/fast.txt"
PROXY_HTTP_URL = "https://raw.githubusercontent.com/saschazesiger/Free-Proxies/master/proxies/http.txt"
PROXY_ANONYMOUS = 'https://raw.githubusercontent.com/monosans/proxy-list/master/proxies_anonymous/http.txt'
PROXY_HTTP_URL_2 = "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt"
VALIDATE_URL = "https://www.google.com"
MAX_CACHE_SIZE = 10_000
ATTEMPTS = 50
GET_MAX_WAITING_TIME = 2
GET_GENERAL_COOLDOWN = 1
MAX_THREAD_USAGE = 20
MIN_THREAD_USAGE = -5  # imlpying each proxy have MIN_THREAD_USAGE continuous chance before been removing if error
URL = "http://datafeed.dukascopy.com/datafeed/{currency}/{year}/{month:02d}/{day:02d}_ticks.bi5"

manager = Manager()
working_proxies = manager.dict()
recent_proxies = Queue(maxsize=MAX_CACHE_SIZE)
update_proxies_lock = Lock()


async def update_proxy():
    """
    Updates the pool of available proxy servers.
    If the number of active proxies is less than the minimum threshold, it performs a "cold restart"
    by waiting for other threads to finish, and then fetching a new set of proxy servers
    from multiple sources and adding them to the working_proxies dictionary.
    """
    active_proxy = sum([1 for nbr_thread in working_proxies.values() if nbr_thread >= MIN_THREAD_USAGE])
    if active_proxy > MIN_PROXY:
        for proxy, nbr_thread in working_proxies.items():
            if nbr_thread >= MIN_THREAD_USAGE and nbr_thread <= MAX_THREAD_USAGE:
                recent_proxies.put_nowait(proxy)
        for proxy in recent_proxies.queue:
            working_proxies[proxy] += 1
        if recent_proxies.qsize() == 0:
            await asyncio.sleep(0.5)  # wait for a proxy to be released
        return
    # cold restart, we wait for other thread to finish otherwise could remove
    await asyncio.sleep(GET_MAX_WAITING_TIME + GET_GENERAL_COOLDOWN)  ## problem here need asyncio so other coroutine can be run
    proxies = set()
    # fetch proxy from different source
    with requests.get(PROXY_ULTRA_FAST_URL) as response:
        ultra_fast_proxy = response.text.split('\n')
        proxies.update(ultra_fast_proxy)
    with requests.get(PROXY_FAST_URL) as response:
        fast_proxy = response.text.split('\n')
        proxies.update(fast_proxy)
    # for this source need to make sure the proxy is for http
    with requests.get(PROXY_HTTP_URL) as response:
        http_proxy = set(response.text.split('\n'))
        proxies = proxies.intersection(http_proxy)
    # with requests.get(PROXY_HTTP_URL_2) as response:
    #    http_proxy = response.text.split('\n')
    #    proxies.update(http_proxy)
    with requests.get(PROXY_ANONYMOUS) as response:
        anonymous_proxy = response.text.split("\n")
        proxies.update(anonymous_proxy)
    proxies = ['http://' + proxy for proxy in proxies]
    for proxy in proxies:
        working_proxies[proxy] = 0
    await update_proxy()

async def fetch_proxy():
    """Fetch a new proxy from the proxy list.

    Returns:
        str: A new proxy URL.
    """
    while True:
        if recent_proxies.qsize() > 0:
            return recent_proxies.get()
        else:
            if update_proxies_lock.acquire(blocking=False):
                await update_proxy()
                update_proxies_lock.release()
            else:
                await asyncio.sleep(0) #return to task manager so other work can be done

class DataFetcher(object):
    def __init__(self, threads: int = 1000) -> None:
        client_timeout = aiohttp.ClientTimeout(total=GET_MAX_WAITING_TIME)
        connect = aiohttp.TCPConnector(limit=threads)
        self.session = ClientSession(timeout=client_timeout, connector=connect)
        self.semantic = asyncio.Semaphore(threads)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.session.connector.close()
        self.session.__exit__(*args)

    def get_url(symbol: str, day: date) -> str:
        """ From the given parameter create and return the URL where the daily tick data should be.

        Args:
                symbol (str): The symbol of the financial asset.
                day (date): The date of the data.

        Returns:

        """
        url = URL.format(currency=symbol, year=day.year, month=day.month - 1, day=day.day)
        return url

    async def get(self, symbol, day) -> Tuple[bytes, date]:
        """
            This function is responsible for obtaining the data for a symbol and a date and return the data as bytes.
            It uses a proxy server from the pool of available proxy servers, to speedup the download.
            It retries the request with a different proxy if it fails and lower the fail proxy alive number.

            Args:
                symbol (str): The symbol of the financial asset.
                day (date): the date of the data

            Returns:
                Tuple: a tuple containing the response data as bytes and the date of the data
            Raises:
                Exception: If the request fails after `ATTEMPTS` attempts.
            """
        Logger.info("Fetching {0}".format(id))
        url = DataFetcher.get_url(symbol, day)
        async with self.semantic:
            for i in range(ATTEMPTS):
                proxy = await fetch_proxy()
                try:
                    async with self.session.get(url, proxy=proxy) as response:
                        if response.status == 200:
                            working_proxies[proxy] = working_proxies[proxy] - 1 if working_proxies[
                                                                                       proxy] > 0 else 0  # case for 2nd chance
                            buffer = await response.read()
                            return buffer, day
                except Exception as e:
                    Logger.warn("Request {0} failed with exception : {1}".format(id, str(e)))
                working_proxies[proxy] -= 2
        print("Request failed for {0} after {1} attempts".format(url, ATTEMPTS))
        print(f"Check if the symbol{symbol} is available for this date {day}: it will slow down a lot the download")
        return b'', day

    async def fetch_async(self, symbol: str, days: List[date], progress_bar: tqdm) -> List[
        Tuple[bytes, date]]:
        """
        This function is responsible for fetching data for a specific symbol for a given range of days.
        It creates a list of tasks, each one responsible for fetching data for a specific day and adds them to a list.
        Then, it waits for each task to complete and appends the result to a list of responses.
        It also updates the progress bar for each completed task.

        Args:
            symbol (str): the symbol to fetch data for
            days (List[date]): a range of dates to fetch data for
            progress_bar (tqdm): progress bar object to display the progress of the fetching process

        Returns:
            List[Tuple[bytes, date]]: list of tuples, each containing the data as bytes and the date of the data
        """
        tasks = []
        for day in days:
            task = self.get(symbol, day)
            tasks.append(task)
        responses = []
        for future in asyncio.as_completed(tasks):
            responses.append(await future)
            progress_bar.update()
            progress_bar.set_description(
                f'Used proxy {sum([1 for nbr_thread in working_proxies.values() if nbr_thread >= MIN_THREAD_USAGE])}')
        return responses

    def fetch(self, symbols: List[str], days: List[date], progress_bar: tqdm) -> List[
        List[Tuple[bytes, date]]]:
        """
        This function is responsible for fetching data for a list of symbols for a given range of days. It creates a
        list of futures, each one responsible for fetching data for a specific symbol and adds them to a list. Then,
        it waits for all the futures to complete and returns the result.

            Args:
                symbols (List[str]): list of symbols to fetch data for
                days (List[date]): a range of dates to fetch data for
                progress_bar (tqdm): progress bar object to display the progress of the fetching process
            Returns:
                List[List[Tuple[bytes, date]]]: list of lists, each containing the data as bytes and the date of the data
            """
        loop = asyncio.get_event_loop()
        futures = [self.fetch_async(symbol, days, progress_bar) for symbol in
                   symbols]
        future = asyncio.gather(*futures)
        result = loop.run_until_complete(future)
        return result  # List[ List[[Byte, Date]]]
