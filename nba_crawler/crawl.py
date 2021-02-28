import asyncio
import pathlib
from datetime import date

import aiohttp
import click
import pandas as pd
from loguru import logger

import nba_crawler.endpoints as endpoints
from nba_crawler import utils


class Crawler:
    def __init__(self, destination: pathlib.Path, tcp_connection_limit: int = 10, client_timeout_seconds: float = 60):
        self.destination = destination
        self.connector = aiohttp.TCPConnector(limit=tcp_connection_limit)
        self.headers = utils.HTTP_HEADERS
        self.timeout = aiohttp.ClientTimeout(total=client_timeout_seconds)

    async def _crawl_win_probability(
        self, session: aiohttp.ClientSession, game_id: str, game_day: str, output_path: pathlib.Path
    ):
        try:
            logger.info(f"Retrieving win probability for game {game_id} on {game_day}")
            win_prob_result = await endpoints.get_win_probability(session, self.headers, game_id)
            logger.info(f"Retrieved win probability for game {game_id} on {game_day}")

            win_prob = win_prob_result.get("WinProbPBP", None)
            if win_prob is not None:
                win_prob.to_parquet(
                    self.destination / game_day / "win_probability" / f"{game_id}.parquet.gz", compression="gzip",
                )

            game_info = win_prob_result.get("GameInfo", None)
            if game_info is not None:
                game_info.to_parquet(
                    self.destination / game_day / "game_info" / f"{game_id}.parquet.gz", compression="gzip",
                )

        except Exception:
            logger.exception(f"Exception while retreiving win probability for {game_id} on {game_day}")

    async def crawl(self, start_day, end_day, loop=None):
        logger.info(f"Crawling data from {start_day} to {end_day}")
        async with aiohttp.ClientSession(connector=self.connector, timeout=self.timeout) as session:
            nba_games = await endpoints.get_nba_games(session, self.headers, start_day, end_day, request_delay_secs=1)
            win_prob_reqs = [
                self._crawl_win_probability(session, game_id, game_day, self.destination)
                for game_day, game_ids in nba_games.items()
                for game_id in game_ids
            ]
            await asyncio.gather(*win_prob_reqs)


@click.group(help="Crawl NBA Stats Page")
def crawl() -> None:
    pass


@crawl.command(help="Retrieve and Store GameDay information across a range days")
@click.option(
    "-s",
    "--start-date",
    type=click.DateTime(formats=["%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"]),
    default=str(date.today()),
    help="Start Date to Crawl",
    required=True,
)
@click.option(
    "-e",
    "--end-date",
    type=click.DateTime(formats=["%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"]),
    default=str(date.today()),
    help="End Date to Crawl",
    required=True,
)
@click.option(
    "-o",
    "--output-path",
    type=click.Path(exists=False, dir_okay=True),
    default=pathlib.Path("."),
    help="Location of output directory",
    required=True,
)
@click.option(
    "-t",
    "--timeout",
    type=int,
    default=60,
    help="Client Timeout in Seconds",
    required=True,
    show_default=True,
)
def game_day(start_date, end_date, output_path, timeout):
    logger.info(f"Crawling game-day from {start_date} to {end_date}. Results are in {output_path}")

    loop = asyncio.get_event_loop()

    output_path = pathlib.Path(output_path)
    # Ensure the the output directories exist
    logger.info(f"Creating target directories in {output_path}")
    for day in pd.date_range(start_date, end_date).strftime("%Y%m%d").to_list():
        for endpoint in ["win_probability", "game_info"]:
            day_path = output_path / day / endpoint
            day_path.mkdir(exist_ok=True, parents=True)

    crawler = Crawler(output_path, client_timeout_seconds=timeout)
    loop.run_until_complete(crawler.crawl(start_date, end_date, loop=loop))
    loop.close()

    logger.info(f"Crawling complete. Results in {output_path}")
