import asyncio
from queue import Queue
import datetime
import time
from multiprocessing import Manager
import requests
from aiohttp import ClientSession

from utils import Logger
from threading import Lock

PROXY_ULTRA_FAST_URL = "https://raw.githubusercontent.com/saschazesiger/Free-Proxies/master/proxies/ultrafast.txt"
PROXY_FAST_URL = "https://raw.githubusercontent.com/saschazesiger/Free-Proxies/master/proxies/fast.txt"
PROXY_HTTP_URL = "https://raw.githubusercontent.com/saschazesiger/Free-Proxies/master/proxies/http.txt"
PROXY_ANONYMOUS = 'https://raw.githubusercontent.com/monosans/proxy-list/master/proxies_anonymous/http.txt'
PROXY_HTTP_URL_2 = "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt"
VALIDATE_URL = "https://www.google.com"
MAX_CACHE_SIZE = 10_000
ATTEMPTS = 300
GET_MAX_WAITING_TIME=2
MAX_THREAD_USAGE = 20
MIN_THREAD_USAGE = -10
URL = "http://datafeed.dukascopy.com/datafeed/{currency}/{year}/{month:02d}/{day:02d}_ticks.bi5"
ATTEMPTS = 100

manager = Manager()
working_proxies = manager.dict()
recent_proxies = Queue(maxsize=MAX_CACHE_SIZE)
update_proxies_lock = Lock()



def update_proxy():
    active_proxy = sum([1 for nbr_thread in working_proxies.values() if nbr_thread>=MIN_THREAD_USAGE])
    if active_proxy > 20:
        for proxy, nbr_thread in working_proxies.items():
            if nbr_thread>=MIN_THREAD_USAGE and nbr_thread<=MAX_THREAD_USAGE:
                recent_proxies.put_nowait(proxy)
        print(active_proxy)
        print(recent_proxies.qsize())
        for proxy in recent_proxies.queue:
            working_proxies[proxy] +=1
        if recent_proxies.qsize() ==0:
            time.sleep(0.5) # wait for a proxy to be released
        return
    # cold restart, we wait for other thread to finish otherwise could remove
    time.sleep(GET_MAX_WAITING_TIME+1)
    print("update------------------------------------------------------------------------------------")
    proxies = set()
    with requests.get(PROXY_ULTRA_FAST_URL) as response:
        ultra_fast_proxy = response.text.split('\n')
        proxies.update(ultra_fast_proxy)
    with requests.get(PROXY_FAST_URL) as response:
        fast_proxy = response.text.split('\n')
        proxies.update(fast_proxy)
    with requests.get(PROXY_HTTP_URL) as response:
        http_proxy = set(response.text.split('\n'))
        proxies = proxies.intersection(http_proxy)
        #proxies.update(http_proxy)
    #with requests.get(PROXY_HTTP_URL_2) as response:
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


semaphore = asyncio.Semaphore(1000)

async def get(session, url: str) -> bytes:
    Logger.info("Fetching {0}".format(id))
    async with semaphore:
        for i in range(ATTEMPTS):
            proxy = fetch_proxy()
            # print('-')
            try:
                async with session.get(url, proxy = proxy, timeout=GET_MAX_WAITING_TIME) as response:
                    working_proxies[proxy] = working_proxies[proxy] - 1 if working_proxies[proxy] > 0 else 0  # case for 2nd chance
                    if response.status == 200:
                        buffer = await response.read()
                        return buffer
            except Exception as e:
                Logger.warn("Request {0} failed with exception : {1}".format(id, str(e)))
                #print("----------")
                working_proxies[proxy] -= 2
    print("too many bad proxy")
    raise Exception("Request failed for {0} after {1} attempts".format(url, ATTEMPTS))


async def fetch_async(symbol, days, progress_bar):
    tasks = []
    async with ClientSession() as session:
        for day in days:
            url = URL.format(currency=symbol, year=day.year, month=day.month - 1, day=day.day)
            task = asyncio.ensure_future(get(session, url))
            tasks.append(task)
        responses = []
        for future in asyncio.as_completed(tasks):
            responses.append(await future)
            progress_bar.update()
        #responses = await asyncio.gather(*tasks)
    return responses

def fetch(symbol, days,progress_bar):
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(fetch_async(symbol, days, progress_bar))
    loop.run_until_complete(future)
    responses = future.result()
    return zip(responses, days)