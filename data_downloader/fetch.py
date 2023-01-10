import datetime
import time
from io import BytesIO, DEFAULT_BUFFER_SIZE


import requests

from utils import Logger, is_dst



URL = "http://datafeed.dukascopy.com/datafeed/{currency}/{year}/{month:02d}/{day:02d}_ticks.bi5"
ATTEMPTS = 30


def get(url: str) -> BytesIO:
    """Make an HTTP GET request to the specified URL using the requests library.

        If the request fails or returns a non-200 status code, retry the request up to `ATTEMPTS` times. If the request is
        successful, return the response body as a bytes object.

        Args:
            url (str): The URL to make the request to.

        Returns:
            bytes: The response body as a bytes object.

        Raises:
            Exception: If the request fails after `ATTEMPTS` attempts.
        """
    buffer = BytesIO()
    start = time.time()
    Logger.info("Fetching {0}".format(id))
    for i in range(ATTEMPTS):
        try:
            with requests.get(url, stream=True) as res:
                if res.status_code == 200:
                    for chunk in res.iter_content(DEFAULT_BUFFER_SIZE):
                        buffer.write(chunk)
                    Logger.info("Fetched {0} completed in {1}s".format(id, time.time() - start))
                    if len(buffer.getbuffer()) <= 0:
                        Logger.info("Buffer for {0} is empty ".format(id))
                    return buffer.getbuffer()
                else:
                    time.sleep(1)
                    Logger.warn("Request to {0} failed with error code : {1} ".format(url, str(res.status_code)))
        except Exception as e:
            Logger.warn("Request {0} failed with exception : {1}".format(id, str(e)))
            time.sleep(1)
    raise Exception("Request failed for {0} after {1} attempts".format(url,ATTEMPTS ))


def fetch_day(symbol, day):
    """Fetch the byte data for the specified day and symbol and return it.
            Args:
                symbol (str): The currency symbol.
                day (datetime.datetime): The day to fetch data for.

            Returns:
                bytes: A bytes object containing the data.
            """
    url = URL.format(currency = symbol, year = day.year, month = day.month -1, day = day.day)
    return get(url)
