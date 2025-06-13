#!/usr/bin/env python3
"""
Reddit Story Scraper
Scrapes text-based stories from specified subreddits and stores them in a database.
"""

import argparse
from dotenv import load_dotenv
from shorts_creator.scraper import run_scraper

load_dotenv()


def main():
    parser = argparse.ArgumentParser(
        description="Scrape Reddit stories and store in database"
    )
    parser.add_argument(
        "--hours", type=int, required=True, help="Number of hours back to scrape from"
    )
    parser.add_argument(
        "--config",
        type=str,
        default="./config.json",
        help="Path to config file (default: ./config.json)",
    )

    args = parser.parse_args()

    run_scraper(args.config, args.hours)


if __name__ == "__main__":
    main()
