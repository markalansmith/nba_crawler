from typing import Any, Dict

import aiohttp

HTTP_HEADERS = {
    "Host": "stats.nba.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true",
    "Connection": "keep-alive",
    "Referer": "https://stats.nba.com/",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
}


async def get_json(
    session: aiohttp.ClientSession, url: str, req_headers: dict = None, req_params: dict = None
) -> Dict[Any, Any]:
    return await _get(session, url, "json", req_headers, req_params)


async def get_text(
    session: aiohttp.ClientSession, url: str, req_headers: dict = None, req_params: dict = None
) -> Dict[Any, Any]:
    return await _get(session, url, "text", req_headers, req_params)


async def _get(
    session: aiohttp.ClientSession, url: str, response_type: str, req_headers: dict = None, req_params: dict = None
) -> Dict[Any, Any]:
    async with session.get(url, params=req_params, headers=req_headers) as response:
        ret = await getattr(response, response_type)()
        return ret
