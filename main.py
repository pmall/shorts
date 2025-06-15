#!/usr/bin/env python3
"""
Reddit Story Scraper and Evaluator
Main CLI interface for scraping Reddit stories and evaluating them for viral potential.
"""

import argparse

from dotenv import load_dotenv

from shorts_creator.evaluate import run_evaluator
from shorts_creator.scraper import run_scraper

load_dotenv()


def main() -> None:
    """Main entry point for the Reddit story scraper and evaluator."""
    parser = argparse.ArgumentParser(
        description="Reddit story scraper and evaluator for viral short video potential"
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Scraper subcommand
    scraper_parser = subparsers.add_parser(
        "scrape", help="Scrape Reddit stories and store in database"
    )
    scraper_parser.add_argument(
        "--config",
        type=str,
        default="./config.json",
        help="Path to config file (default: ./config.json)",
    )

    # Evaluator subcommand
    evaluator_parser = subparsers.add_parser(
        "evaluate", help="Evaluate Reddit stories for viral short video potential"
    )
    evaluator_parser.add_argument(
        "--max-stories",
        type=int,
        default=1000,
        help="Maximum number of stories to evaluate (default: 1000)",
    )

    args = parser.parse_args()

    if args.command == "scrape":
        run_scraper(args.config)
    elif args.command == "evaluate":
        run_evaluator(args.max_stories)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
