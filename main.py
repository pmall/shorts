#!/usr/bin/env python3
"""
Reddit Story Scraper
Scrapes text-based stories from specified subreddits and stores them in a database.
"""

import json
import argparse
from dotenv import load_dotenv
from shorts_creator.scraper import RedditScraper
from shorts_creator.database import create_database_manager

load_dotenv()


def load_config(config_path: str) -> dict:
    """Load configuration from JSON file"""
    try:
        with open(config_path, "r") as f:
            config = json.load(f)
        print(f"[INFO] Loaded config from {config_path}")
        return config
    except FileNotFoundError:
        print(f"[ERROR] Config file not found: {config_path}")
        raise
    except json.JSONDecodeError as e:
        print(f"[ERROR] Invalid JSON in config file: {e}")
        raise


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

    # Load configuration
    config = load_config(args.config)
    subreddits = config.get("subreddits", [])
    min_length = config.get("min_content_length", 100)

    if not subreddits:
        print("[ERROR] No subreddits specified in config")
        return

    print(
        f"[INFO] Starting scrape: {len(subreddits)} subreddits, {args.hours} hours back"
    )

    # Initialize components
    db = create_database_manager()
    scraper = RedditScraper(min_content_length=min_length)

    try:
        # Setup database
        db.connect()
        db.create_table()

        total_new_stories = 0
        total_duplicates = 0

        # Scrape each subreddit
        for subreddit_name in subreddits:
            stories = scraper.get_stories_from_subreddit(subreddit_name, args.hours)

            # Store stories in database
            for story in stories:
                inserted = db.insert_story(
                    story["reddit_id"],
                    story["subreddit"],
                    story["content"],
                    story["created_utc"],
                    story["flair"],
                )

                if inserted:
                    total_new_stories += 1
                    print(f"[INFO] Stored new story: {story['reddit_id']}")
                else:
                    total_duplicates += 1
                    print(f"[DEBUG] Duplicate story skipped: {story['reddit_id']}")

        print(
            f"[INFO] Scraping complete: {total_new_stories} new stories, {total_duplicates} duplicates"
        )

    except Exception as e:
        print(f"[ERROR] Fatal error: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
