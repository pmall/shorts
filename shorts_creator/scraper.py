"""
Reddit scraper for extracting text-based stories from specified subreddits.
"""

import os
from datetime import UTC, datetime, timedelta
from typing import TypedDict

import praw
from praw.models import Submission

from shorts_creator.database import create_database_manager
from shorts_creator.utils import load_config


class StoryData(TypedDict):
    reddit_id: str
    subreddit: str
    content: str
    created_utc: int
    flair: str | None


class RedditScraper:
    """Handles Reddit API interactions and story extraction."""

    def __init__(self, min_content_length: int = 100) -> None:
        """Initialize the Reddit scraper."""
        self.min_content_length = min_content_length
        self.reddit = praw.Reddit(
            client_id=os.getenv("REDDIT_CLIENT_ID"),
            client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
            user_agent=os.getenv("REDDIT_USER_AGENT"),
        )
        print("[INFO] Connected to Reddit API as read-only")

    def format_content(self, submission: Submission) -> str:
        """Format submission title and content as markdown."""
        title = submission.title.strip()
        selftext = submission.selftext.strip() if submission.selftext else ""

        if selftext:
            # Title + content format
            return f"# {title}\n\n{selftext}"
        else:
            # Title only
            return title

    def is_valid_story(self, submission: Submission) -> bool:
        """Check if submission is a valid text-based story."""
        # Skip if it's a link post (has URL but no selftext)
        if submission.url and not submission.is_self:
            return False

        # Get formatted content and check minimum length
        content = self.format_content(submission)
        if len(content) < self.min_content_length:
            return False

        return True

    def get_stories_from_subreddit(self, subreddit_name: str) -> list[StoryData]:
        """Scrape stories from a subreddit from the last 24 hours."""
        print(f"[INFO] Scraping r/{subreddit_name} for stories from last 24 hours")

        subreddit = self.reddit.subreddit(subreddit_name)
        cutoff_time = datetime.now(UTC) - timedelta(hours=24)
        cutoff_timestamp = cutoff_time.timestamp()

        stories: list[StoryData] = []
        processed = 0

        try:
            # Get posts from multiple sources
            # top() supports time filters, others don't
            post_sources = [
                ("hot", subreddit.hot(limit=100)),
                ("rising", subreddit.rising(limit=100)),
                ("top_day", subreddit.top(time_filter="day")),
            ]

            for source_name, posts in post_sources:
                print(f"[INFO] Processing {source_name} posts from r/{subreddit_name}")

                for submission in posts:
                    processed += 1

                    # Skip if older than 24 hours
                    if submission.created_utc < cutoff_timestamp:
                        continue

                    # Skip if not a valid story
                    if not self.is_valid_story(submission):
                        print(f"[DEBUG] Skipped: {submission.id} - not a valid story")
                        continue

                    content = self.format_content(submission)
                    flair: str | None = (
                        submission.link_flair_text
                        if submission.link_flair_text
                        else None
                    )
                    stories.append(
                        StoryData(
                            reddit_id=submission.id,
                            subreddit=subreddit_name,
                            content=content,
                            created_utc=int(submission.created_utc),
                            flair=flair,
                        )
                    )

                    flair_info = f" [Flair: {flair}]" if flair else ""
                    print(
                        f"[DEBUG] Found story: {submission.id} ({len(content)} chars){flair_info} from {source_name}"
                    )

        except Exception as e:
            print(f"[ERROR] Error scraping r/{subreddit_name}: {e}")

        print(
            f"[INFO] Found {len(stories)} valid stories from r/{subreddit_name} "
            f"(processed {processed} posts)"
        )
        return stories

    def run(self, config_file: str) -> None:
        """Main execution function."""
        # Load configuration
        config = load_config(config_file)
        subreddits = config.get("subreddits", [])
        min_length = config.get("min_content_length", 100)

        if not subreddits:
            print("[ERROR] No subreddits specified in config")
            return

        print(f"[INFO] Starting scrape: {len(subreddits)} subreddits, last 24 hours")

        # Initialize database
        db = create_database_manager()

        try:
            # Setup database
            db.connect()
            db.create_tables()

            # Update minimum content length
            self.min_content_length = min_length

            total_new_stories = 0
            total_duplicates = 0

            # Scrape each subreddit
            for subreddit_name in subreddits:
                stories = self.get_stories_from_subreddit(subreddit_name)

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
                f"[INFO] Scraping complete: {total_new_stories} new stories, "
                f"{total_duplicates} duplicates"
            )

        except Exception as e:
            print(f"[ERROR] Fatal error: {e}")
            raise
        finally:
            db.close()


def run_scraper(config_file: str) -> None:
    """Run the Reddit scraper with the specified configuration."""
    scraper = RedditScraper()
    scraper.run(config_file)
