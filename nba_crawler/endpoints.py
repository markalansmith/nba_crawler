import asyncio
import datetime
import itertools
from typing import Dict, Optional, Set

import aiohttp
import pandas as pd
from loguru import logger

from nba_crawler import nba_teams, utils


async def get_nba_games(
    session: aiohttp.ClientSession,
    headers: dict,
    start_day: datetime.datetime,
    end_day: datetime.datetime,
    request_delay_secs: int = 1,
) -> Dict[str, Set[str]]:
    team_ids = nba_teams.TEAM_IDS
    all_games = []
    logger.info(f"Retrieving nba games from {start_day} to {end_day} over {len(team_ids)} teams")
    for team_count, team_id in enumerate(team_ids):
        games = await utils.get_json(
            session,
            "https://stats.nba.com/stats/leaguegamefinder",
            req_headers=headers,
            req_params={"PlayerOrTeam": "T", "TeamID": team_id},
        )

        logger.info(f"Completed retrieval for team id {team_id}. {team_count+1} out of {len(team_ids)}")

        if "resultSets" not in games:
            continue

        result = games["resultSets"][0]
        result_headers = [x.lower() for x in result["headers"]]
        games = [dict(zip(result_headers, value)) for value in result["rowSet"]]
        games = filter(lambda x: start_day <= pd.to_datetime(x["game_date"]) <= end_day, games)
        all_games.extend(games)
        await asyncio.sleep(request_delay_secs)

    day_game_ids = {
        pd.to_datetime(game_day).strftime("%Y%m%d"): set(game["game_id"] for game in games_on_day)
        for game_day, games_on_day in itertools.groupby(all_games, lambda x: x["game_date"])
    }
    return day_game_ids


async def get_win_probability(
    session: aiohttp.ClientSession, headers: dict, game_id: str
) -> Optional[Dict[str, pd.DataFrame]]:
    win_probability = await utils.get_json(
        session,
        "https://stats.nba.com/stats/winprobabilitypbp",
        req_headers=headers,
        req_params={"GameID": game_id, "RunType": "each second"},
    )

    if "resultSets" not in win_probability:
        return None

    result = {}
    for result_set in win_probability["resultSets"]:
        result_headers = [x.lower() for x in result_set["headers"]]
        result_df = pd.DataFrame([dict(zip(result_headers, value)) for value in result_set["rowSet"]])
        result[result_set["name"]] = result_df

    return result
