import asyncio
from queue import Queue
import time
from multiprocessing import Manager
from datetime import date


import aiohttp
import requests
from aiohttp import ClientSession

from utils import Logger
from threading import Lock

MIN_PROXY = 80

PROXY_ULTRA_FAST_URL = "https://raw.githubusercontent.com/saschazesiger/Free-Proxies/master/proxies/ultrafast.txt"
PROXY_FAST_URL = "https://raw.githubusercontent.com/saschazesiger/Free-Proxies/master/proxies/fast.txt"
PROXY_HTTP_URL = "https://raw.githubusercontent.com/saschazesiger/Free-Proxies/master/proxies/http.txt"
PROXY_ANONYMOUS = 'https://raw.githubusercontent.com/monosans/proxy-list/master/proxies_anonymous/http.txt'
PROXY_HTTP_URL_2 = "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt"
VALIDATE_URL = "https://www.google.com"
MAX_CACHE_SIZE = 10_000
ATTEMPTS = 50
GET_MAX_WAITING_TIME = 2
GET_GENERAL_COOLDOWN = 5
MAX_THREAD_USAGE = 20
MIN_THREAD_USAGE = -5 # imlpying each proxy have MIN_THREAD_USAGE continuous chance before been removing if error
URL = "http://datafeed.dukascopy.com/datafeed/{currency}/{year}/{month:02d}/{day:02d}_ticks.bi5"

manager = Manager()
working_proxies = manager.dict()
recent_proxies = Queue(maxsize=MAX_CACHE_SIZE)
update_proxies_lock = Lock()


def update_proxy():
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
            time.sleep(0.5)  # wait for a proxy to be released
        return
    # cold restart, we wait for other thread to finish otherwise could remove
    time.sleep(GET_MAX_WAITING_TIME + GET_GENERAL_COOLDOWN)
    proxies = set()
    #fetch proxy from different source
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
    update_proxy()


def fetch_proxy():
    """Fetch a new proxy from the proxy list.

    Returns:
        str: A new proxy URL.
    """
    with update_proxies_lock:
        if recent_proxies.qsize() > 0:
            return recent_proxies.get()
        update_proxy()
    return fetch_proxy()



def fetch_synchrone(symbol : str, day: date) -> bytes:
    """Make an HTTP GET request to the specified URL using the requests library.
        If the request fails or returns a non-200 status code, retry the request up to `ATTEMPTS` times. If the request is
        successful, return the response body as a bytes object.
        Args:
            symbol (str): The symbol of the financial asset.
            day (date): The date of the data.
        Returns:
            bytes: The response body as a bytes object.
        Raises:
            Exception: If the request fails after `ATTEMPTS` attempts.
        """
    url = URL.format(currency=symbol, year=day.year, month=day.month - 1, day=day.day)
    start = time.time()
    Logger.info("Fetching {0}".format(id))
    for i in range(ATTEMPTS):
        proxy = fetch_proxy()
        # print('-')
        try:
            with requests.get(url, proxies={'http': proxy, 'https': proxy}, timeout=GET_MAX_WAITING_TIME) as res:
                working_proxies[proxy] = working_proxies[proxy] -1 if working_proxies[proxy]>0 else 0 #case for 2nd chance
                if res.status_code == 200:
                    buffer = res.content
                    Logger.info("Fetched {0} completed in {1}s".format(id, time.time() - start))
                    if len(buffer) <= 0:
                        Logger.info("Buffer for {0} is empty ".format(id))
                    return buffer
                else:
                    # print("--------------------------------------------")
                    Logger.warn("Request to {0} failed with error code : {1} ".format(url, str(res.status_code)))
        except Exception as e:
            Logger.warn("Request {0} failed with exception : {1}".format(id, str(e)))
            #print("----------")
            working_proxies[proxy] -= 2
    print("too many bad proxy")
    raise Exception("Request failed for {0} after {1} attempts".format(url, ATTEMPTS))

async def get(session, url: str, day, semantic) -> (bytes, date):
    Logger.info("Fetching {0}".format(id))
    async with semantic:
        try:
            for i in range(ATTEMPTS):
                proxy = fetch_proxy()
                # print('-')
                try:
                    async with session.get(url, proxy=proxy) as response:
                        if response.status == 200:
                            working_proxies[proxy] = working_proxies[proxy] - 1 if working_proxies[
                                                                                       proxy] > 0 else 0  # case for 2nd chance
                            buffer = await response.read()
                            return (buffer, day)
                except Exception as e:
                    Logger.warn("Request {0} failed with exception : {1}".format(id, str(e)))
                working_proxies[proxy] -= 2
        except Exception as e:
            print("Request failed for {0} after {1} attempts".format(url, ATTEMPTS))
            raise Exception("Request failed for {0} after {1} attempts".format(url, ATTEMPTS))


async def fetch_async(symbol, days, threads, semantic, progress_bar):
    """

    Args:
        symbol:
        days:
        threads:
        semantic:
        progress_bar:

    Returns:
        List[Future[Byte, Date]]
    """
    tasks = []
    client_timeout = aiohttp.ClientTimeout(total=GET_MAX_WAITING_TIME)
    connect = aiohttp.TCPConnector(limit=threads)
    async with ClientSession(timeout=client_timeout, connector=connect) as session:
        for day in days:
            url = URL.format(currency=symbol, year=day.year, month=day.month - 1, day=day.day)
            task = asyncio.ensure_future(get(session, url, day, semantic))
            tasks.append(task)
        responses = []
        for future in asyncio.as_completed(tasks):
            responses.append(await future)
            progress_bar.update()
            progress_bar.set_description(
                f'Used proxy {sum([1 for nbr_thread in working_proxies.values() if nbr_thread >= MIN_THREAD_USAGE])}')
    return responses


def fetch(symbols, days, progress_bar, threads=1000):
    """
    return data of multiple symbol in order of symbols
    Args:
        symbols:
        days:
        progress_bar:

    Returns:

    """
    semantic = asyncio.Semaphore(threads)
    loop = asyncio.get_event_loop()
    futures = [fetch_async(symbol, days, threads, semantic, progress_bar) for symbol in symbols] #List[1 elem per symbol Coroutine[List[Future[Byte, Date]]]]
    future = asyncio.ensure_future(asyncio.gather(*futures)) #Future[List[ List[Future[Byte, Date]]]]
    loop.run_until_complete(future)
    return future.result()#List[ List[Future[Byte, Date]]]
