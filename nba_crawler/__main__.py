import click

import nba_crawler

APP_NAME = "nba_crawler"


@click.group(help="NBA Stats Crawler", name="nba_crawler")
@click.version_option(prog_name=APP_NAME, version=nba_crawler.__version__)
def _nba_crawler() -> None:
    pass


_nba_crawler.add_command(nba_crawler.crawl.crawl)


if __name__ == "__main__":
    nba_crawler()
