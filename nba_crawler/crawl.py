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
    def __init__(self, destination: pathlib.Path):
        self.destination = destination
        self.connector = aiohttp.TCPConnector(limit=10)
        self.headers = utils.HTTP_HEADERS
        self.timeout = aiohttp.ClientTimeout(total=60)

    async def crawl(self, start_day, end_day, loop=None):
        logger.info(f"Crawling data from {start_day} to {end_day}")
        async with aiohttp.ClientSession(connector=self.connector, timeout=self.timeout) as session:
            game_ids = await endpoints.get_nba_games(session, self.headers, start_day, end_day, request_delay_secs=1)
            for game_day, game_ids in game_ids.items():
                game_infos = []
                for game_id in game_ids:
                    try:
                        logger.info(f"Retrieving win probability for game {game_id} on {game_day}")
                        win_prob_result = await endpoints.get_win_probability(session, self.headers, game_id)
                        if not win_prob_result:
                            logger.error(f'Unable to retrieve win probability data for {game_id} on {game_day}')
                            continue
                            
                        win_prob = win_prob_result.get("WinProbPBP", None)
                        if win_prob is not None:
                            win_prob.to_parquet(
                                self.destination / game_day / "win_probability" / f"{game_id}.parquet.gz",
                                compression="gzip",
                            )

                        game_info = win_prob_result.get("GameInfo", None)
                        if game_info is not None:
                            game_infos.append(game_info)
                    except Exception:
                        logger.exception(f"Exception while retreiving win probability for {game_id} on {game_day}")

                if game_infos:
                    game_infos = pd.concat(game_infos, ignore_index=True)
                    game_infos.to_parquet(self.destination / game_day / "game_infos.parquet.gz", compression="gzip")


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
def game_day(start_date, end_date, output_path):
    logger.info(f"Crawling game-day from {start_date} to {end_date}. Results are in {output_path}")

    loop = asyncio.get_event_loop()

    output_path = pathlib.Path(output_path)
    # Ensure the the output directories exist
    logger.info(f"Creating target directories in {output_path}")
    for day in pd.date_range(start_date, end_date).strftime("%Y%m%d").to_list():
        win_probability_path = output_path / day / "win_probability"
        win_probability_path.mkdir(exist_ok=True, parents=True)

    crawler = Crawler(output_path)
    loop.run_until_complete(crawler.crawl(start_date, end_date, loop=loop))
    loop.close()

    logger.info(f"Crawling complete. Results in {output_path}")
