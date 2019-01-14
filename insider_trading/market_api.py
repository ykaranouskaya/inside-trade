import requests
import json
import os
import logging

import asyncio, aiohttp
from asyncio import Semaphore
import urllib


BASE_URL = "https://www.alphavantage.co"
ENDPOINT = "/query"

API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY", "")
RATE_LIMIT_WAIT = 30
MAX_REQUESTS_PER_SEC = 0.05

LOG = logging.getLogger(__name__)


class API:

    def __init__(self, function):
        self.url = urllib.parse.urljoin(BASE_URL, ENDPOINT)
        self.limit_wait = RATE_LIMIT_WAIT
        self.function = function
        self.API_KEY = API_KEY

    async def _async_request(self, params):
        try:
            await asyncio.sleep(self.limit_wait)
            async with aiohttp.ClientSession() as session:
                async with session.get(self.url, params=params) as url:
                    data = await url.read()
        except aiohttp.web.HTTPError as e:
            LOG.exception(f'Error while downloading form: {e}')
            return None

        return data

    async def request(self, parameters, limiter):
        async with limiter:
            data = await self._async_request(parameters)

        return data

    async def get_symbol_info(self, symbol, limiter):
        parameters = {"function": self.function,
                      "symbol": symbol,
                      "apikey": self.API_KEY}
        info = await self.request(parameters, limiter)
        return info


    def get_symbols_data(self, symbols):

        async def get_contents(symbols, limiter):
            coros = [self.get_symbol_info(sym, limiter) for sym in symbols]
            contents = await asyncio.gather(*coros)

            return contents


        loop = asyncio.get_event_loop()
        limiter = Semaphore(MAX_REQUESTS_PER_SEC, loop=loop)

        symbols_data = loop.run_until_complete(get_contents(symbols, limiter))

        return symbols_data

