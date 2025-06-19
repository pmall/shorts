#!/usr/bin/env python3
"""
Story Evaluator
Uses Gemini AI to evaluate Reddit stories for viral short video potential.
"""

import json
import time
from typing import TypedDict, Any

from google import genai
from google.genai import types

from shorts_creator.database import create_database_manager
from shorts_creator.prompts import EVALUATION_PROMPT_TEMPLATE

# Constants
CATEGORIES = [
    "relationship",
    "workplace",
    "family",
    "revenge",
    "confession",
    "humor",
    "drama",
    "mystery",
    "lifestyle",
    "uncategorized",
]

TARGET_AUDIENCES = ["general", "young_adult", "mature", "teens"]

MAX_TOKENS_PER_BATCH = 50000
MAX_STORIES_PER_BATCH = 20
BATCH_DELAY_SECONDS = 4
MAX_RETRIES = 3
GEMINI_MODEL = "gemini-2.0-flash-lite"

RESPONSE_SCHEMA = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "reddit_id": {"type": "string", "pattern": "^[a-z0-9]{6,10}$"},
            "score": {"type": "integer", "minimum": 0, "maximum": 100},
            "category": {
                "type": "string",
                "enum": [
                    "relationship",
                    "workplace",
                    "family",
                    "revenge",
                    "confession",
                    "humor",
                    "drama",
                    "mystery",
                    "lifestyle",
                    "uncategorized",
                ],
            },
            "target_audience": {
                "type": "string",
                "enum": ["general", "young_adult", "mature", "teens"],
            },
        },
        "required": ["reddit_id", "score", "category", "target_audience"],
    },
}


class StoryData(TypedDict):
    reddit_id: str
    subreddit: str
    content: str
    created_utc: int
    flair: str | None


class EvaluationData(TypedDict):
    reddit_id: str
    score: int
    category: str
    target_audience: str


class StoryEvaluator:
    """Evaluates Reddit stories for viral potential using Gemini AI."""

    def __init__(self) -> None:
        """Initialize the story evaluator."""
        self.db_manager = create_database_manager()
        self.client = genai.Client()

    def connect_and_setup(self) -> None:
        """Connect to database and create tables."""
        self.db_manager.connect()
        self.db_manager.create_tables()

    def get_unevaluated_stories(self, limit: int | None = None) -> list[StoryData]:
        """Get stories that haven't been evaluated yet."""
        raw_stories = self.db_manager.get_unevaluated_stories(limit)
        return [
            StoryData(
                reddit_id=str(story["reddit_id"]),
                subreddit=str(story["subreddit"]),
                content=str(story["content"]),
                created_utc=(
                    int(story["created_utc"]) if story["created_utc"] is not None else 0
                ),
                flair=story["flair"] if story["flair"] is None else str(story["flair"]),
            )
            for story in raw_stories
        ]

    def estimate_tokens(self, text: str) -> int:
        """Rough token estimation (1 token ≈ 0.75 words)."""
        word_count = len(text.split())
        return int(word_count / 0.75)

    def format_story_for_prompt(self, story: StoryData) -> str:
        return f"""
Story ID: {story['reddit_id']}
Subreddit: r/{story['subreddit']}
Flair: {story['flair'] or 'None'}
Content: {story['content']}
""".strip()

    def create_batches(self, stories: list[StoryData]) -> list[list[StoryData]]:
        """Create batches of stories respecting both token and story count limits."""
        # Sort stories by content length
        stories_by_length = sorted(stories, key=lambda x: len(x["content"]))

        batches: list[list[StoryData]] = []
        current_batch: list[StoryData] = []
        current_tokens = 0

        for story in stories_by_length:
            # Format story for prompt
            story_text = self.format_story_for_prompt(story)

            story_tokens = self.estimate_tokens(story_text)

            # Check if adding this story would exceed either limit
            would_exceed_tokens = current_tokens + story_tokens > MAX_TOKENS_PER_BATCH
            would_exceed_count = len(current_batch) >= MAX_STORIES_PER_BATCH

            if (would_exceed_tokens or would_exceed_count) and current_batch:
                batches.append(current_batch)
                current_batch = [story]
                current_tokens = story_tokens
            else:
                current_batch.append(story)
                current_tokens += story_tokens

        # Add the last batch
        if current_batch:
            batches.append(current_batch)

        print(f"[INFO] Created {len(batches)} batches for processing")
        return batches

    def build_prompt(self, stories: list[StoryData]) -> str:
        """Build the evaluation prompt for a batch of stories."""
        stories_content = [self.format_story_for_prompt(s) for s in stories]

        return EVALUATION_PROMPT_TEMPLATE(
            stories_content=stories_content,
            categories=CATEGORIES,
            target_audiences=TARGET_AUDIENCES,
        )

    def call_gemini(self, prompt: str) -> list[EvaluationData]:
        """Call Gemini API with the prompt."""
        try:
            response = self.client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=RESPONSE_SCHEMA,
                    temperature=0.3,
                ),
            )

            return json.loads(response.text or "[]")
        except Exception as e:
            print(f"[ERROR] Gemini API call failed: {str(e)}")
            raise

    def validate_evaluation(self, evaluation: dict[str, Any]) -> bool:
        """Validate a single evaluation."""
        required_fields = ["reddit_id", "score", "category", "target_audience"]

        # Check required fields
        for field in required_fields:
            if field not in evaluation:
                print(f"[ERROR] Missing field '{field}' in evaluation")
                return False

        # Validate reddit_id format
        reddit_id = evaluation["reddit_id"]
        if not isinstance(reddit_id, str) or len(reddit_id) < 6 or len(reddit_id) > 10:
            print(f"[ERROR] Invalid reddit_id format: {reddit_id}")
            return False

        # Validate score
        if not isinstance(evaluation["score"], int) or not (
            0 <= evaluation["score"] <= 100
        ):
            print(f"[ERROR] Invalid score: {evaluation['score']}")
            return False

        # Validate category
        if evaluation["category"] not in CATEGORIES:
            print(f"[ERROR] Invalid category: {evaluation['category']}")
            return False

        # Validate target audience
        if evaluation["target_audience"] not in TARGET_AUDIENCES:
            print(f"[ERROR] Invalid target audience: {evaluation['target_audience']}")
            return False

        return True

    def process_batch(self, batch: list[StoryData]) -> bool:
        """Process a single batch of stories, returns True if successful."""
        print(f"[INFO] Processing batch of {len(batch)} stories")

        # Build prompt
        prompt = self.build_prompt(batch)

        # Call Gemini
        try:
            evaluations = self.call_gemini(prompt)
        except Exception as e:
            print(f"[ERROR] Failed to get evaluations from Gemini: {str(e)}")
            return False

        # Validate response structure
        if not isinstance(evaluations, list):
            print("[ERROR] Invalid response structure: not a list")
            return False

        if len(evaluations) == 0:
            print("[WARNING] Received empty evaluations list")
            return False

        # Validate each evaluation and ensure all batch stories are evaluated
        valid_evaluations: list[dict[str, Any]] = []
        batch_reddit_ids = {story["reddit_id"] for story in batch}
        evaluated_reddit_ids = set()

        for evaluation in evaluations:
            if self.validate_evaluation(dict(evaluation)):
                eval_dict = dict(evaluation)
                reddit_id = eval_dict["reddit_id"]

                # Check if this reddit_id was in our batch
                if reddit_id in batch_reddit_ids:
                    valid_evaluations.append(eval_dict)
                    evaluated_reddit_ids.add(reddit_id)
                else:
                    print(f"[WARNING] Evaluation for unexpected reddit_id: {reddit_id}")
            else:
                print(f"[WARNING] Skipping invalid evaluation: {evaluation}")

        # Check if we got evaluations for all stories in the batch
        missing_ids = batch_reddit_ids - evaluated_reddit_ids
        if missing_ids:
            print(f"[WARNING] Missing evaluations for reddit_ids: {missing_ids}")

        # Insert valid evaluations
        if valid_evaluations:
            try:
                inserted_count = self.db_manager.insert_evaluations(valid_evaluations)
                print(
                    f"[INFO] Successfully processed {inserted_count}/{len(batch)} evaluations"
                )
                return inserted_count > 0
            except Exception as e:
                print(f"[ERROR] Database insertion failed: {str(e)}")
                return False
        else:
            print("[ERROR] No valid evaluations to insert")
            return False

    def run(self, max_stories: int) -> None:
        """Main execution function."""
        print(f"[INFO] Starting story evaluation with max_stories={max_stories}")

        # Connect and setup
        try:
            self.connect_and_setup()
        except Exception as e:
            print(f"[ERROR] Database connection failed: {str(e)}")
            return

        # Get unevaluated stories
        try:
            stories = self.get_unevaluated_stories(max_stories)
        except Exception as e:
            print(f"[ERROR] Failed to fetch stories: {str(e)}")
            return

        if not stories:
            print("[INFO] No stories to evaluate")
            return

        # Create batches
        try:
            batches = self.create_batches(stories)
        except Exception as e:
            print(f"[ERROR] Failed to create batches: {str(e)}")
            return

        # Process batches
        failures = 0
        total_processed = 0

        for i, batch in enumerate(batches, 1):
            print(f"[INFO] Processing batch {i}/{len(batches)}")

            try:
                success = self.process_batch(batch)
            except Exception as e:
                print(f"[ERROR] Batch processing failed: {str(e)}")
                success = False

            if success:
                total_processed += len(batch)
                failures = 0  # Reset failure count on success
            else:
                failures += 1
                print(f"[ERROR] Batch {i} failed (failure count: {failures})")

                if failures >= MAX_RETRIES:
                    print(
                        f"[ERROR] Maximum failures ({MAX_RETRIES}) reached. Stopping."
                    )
                    break

            # Rate limiting delay
            if i < len(batches):  # Don't delay after the last batch
                print(
                    f"[INFO] Waiting {BATCH_DELAY_SECONDS} seconds before next batch..."
                )
                time.sleep(BATCH_DELAY_SECONDS)

        print(f"[INFO] Evaluation complete. Processed {total_processed} stories")

        # Close database connection
        try:
            self.db_manager.close()
        except Exception as e:
            print(f"[WARNING] Error closing database connection: {str(e)}")


def run_evaluator(max_stories: int) -> None:
    """Run the story evaluator."""
    try:
        evaluator = StoryEvaluator()
        evaluator.run(max_stories)
    except Exception as e:
        print(f"[ERROR] Evaluator failed to start: {str(e)}")
