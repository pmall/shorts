import os
import praw
from typing import Optional
from datetime import datetime, timedelta, UTC
from praw.models import Submission
from shorts_creator.utils import load_config
from shorts_creator.database import create_database_manager


class RedditScraper:
    """Handles Reddit API interactions and story extraction"""

    def __init__(self, min_content_length: int = 100):
        self.min_content_length = min_content_length
        self.reddit = praw.Reddit(
            client_id=os.getenv("REDDIT_CLIENT_ID"),
            client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
            user_agent=os.getenv("REDDIT_USER_AGENT"),
        )
        print(f"[INFO] Connected to Reddit API as read-only")

    def format_content(self, submission: Submission) -> str:
        """Format submission title and content as markdown"""
        title = submission.title.strip()
        selftext = submission.selftext.strip() if submission.selftext else ""

        if selftext:
            # Title + content format
            return f"# {title}\n\n{selftext}"
        else:
            # Title only
            return title

    def is_valid_story(self, submission: Submission) -> bool:
        """Check if submission is a valid text-based story"""
        # Skip if it's a link post (has URL but no selftext)
        if submission.url and not submission.is_self:
            return False

        # Get formatted content and check minimum length
        content = self.format_content(submission)
        if len(content) < self.min_content_length:
            return False

        return True

    def get_stories_from_subreddit(
        self, subreddit_name: str, hours_back: int
    ) -> list[dict]:
        """Scrape stories from a subreddit within the specified time range"""
        print(
            f"[INFO] Scraping r/{subreddit_name} for stories from last {hours_back} hours"
        )

        subreddit = self.reddit.subreddit(subreddit_name)
        cutoff_time = datetime.now(UTC) - timedelta(hours=hours_back)
        cutoff_timestamp = cutoff_time.timestamp()

        stories = []
        processed = 0

        try:
            # Get recent posts (new posts are more likely to be within our time range)
            for submission in subreddit.new(limit=1000):  # Reasonable limit
                processed += 1

                # Skip if older than our cutoff
                if submission.created_utc < cutoff_timestamp:
                    continue

                # Skip if not a valid story
                if not self.is_valid_story(submission):
                    print(f"[DEBUG] Skipped: {submission.id} - not a valid story")
                    continue

                content = self.format_content(submission)
                flair: Optional[str] = (
                    submission.link_flair_text if submission.link_flair_text else None
                )
                stories.append(
                    {
                        "reddit_id": submission.id,
                        "subreddit": subreddit_name,
                        "content": content,
                        "created_utc": int(submission.created_utc),
                        "flair": flair,
                    }
                )

                flair_info = f" [Flair: {flair}]" if flair else ""
                print(
                    f"[DEBUG] Found story: {submission.id} ({len(content)} chars){flair_info}"
                )

        except Exception as e:
            print(f"[ERROR] Error scraping r/{subreddit_name}: {e}")

        print(
            f"[INFO] Found {len(stories)} valid stories from r/{subreddit_name} (processed {processed} posts)"
        )
        return stories


def run_scraper(config_file: str, hours: int):
    # Load configuration
    config = load_config(config_file)
    subreddits = config.get("subreddits", [])
    min_length = config.get("min_content_length", 100)

    if not subreddits:
        print("[ERROR] No subreddits specified in config")
        return

    print(f"[INFO] Starting scrape: {len(subreddits)} subreddits, {hours} hours back")

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
            stories = scraper.get_stories_from_subreddit(subreddit_name, hours)

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
