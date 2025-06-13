#!/usr/bin/env python3
"""
Story Evaluator
Uses Gemini AI to evaluate Reddit stories for viral short video potential.
"""

import json
import time
from string import Template
from typing import TypedDict, Any

from google import genai
from google.genai import types

from shorts_creator.database import create_database_manager

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
BATCH_DELAY_SECONDS = 4
MAX_RETRIES = 3
GEMINI_MODEL = "gemini-2.0-flash-lite"

# Prompt template
EVALUATION_PROMPT = Template(
    """
You are an expert content creator who specializes in viral short-form video content. Your task is to evaluate Reddit stories for their potential to become captivating, viral short videos.

For each story, consider these factors:
- Emotional hooks and engagement potential
- Clear conflict/resolution or narrative arc
- Relatability and universal appeal
- Surprising elements or plot twists
- Visual storytelling potential
- Shareability and discussion-worthy content

Rate each story on a scale of 0-1000 where:
- 0-200: Poor viral potential, boring or unsuitable
- 201-400: Below average, some elements but lacking
- 401-600: Average potential, decent story but common
- 601-800: Good potential, engaging with viral elements
- 801-1000: Excellent potential, highly engaging and viral-worthy

Also categorize each story and identify the target audience.

Stories to evaluate:
$stories_content

Provide your evaluation in the following JSON format for each story:
{
  "evaluations": [
    {
      "reddit_id": "story_reddit_id",
      "score": 750,
      "category": "one of: ${categories}",
      "target_audience": "one of: ${audiences}"
    }
  ]
}
"""
)


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

    def create_batches(self, stories: list[StoryData]) -> list[list[StoryData]]:
        """Create batches of stories respecting token limits and grouping by length."""
        # Sort stories by content length
        stories_by_length = sorted(stories, key=lambda x: len(x["content"]))

        batches: list[list[StoryData]] = []
        current_batch: list[StoryData] = []
        current_tokens = 0

        # Estimate base prompt tokens
        base_prompt = EVALUATION_PROMPT.safe_substitute(
            stories_content="",
            categories=", ".join(CATEGORIES),
            audiences=", ".join(TARGET_AUDIENCES),
        )
        base_tokens = self.estimate_tokens(base_prompt)

        for story in stories_by_length:
            # Format story for prompt
            story_text = f"""
Story ID: {story['reddit_id']}
Subreddit: r/{story['subreddit']}
Flair: {story['flair'] or 'None'}
Content: {story['content']}
---
"""
            story_tokens = self.estimate_tokens(story_text)

            # Check if adding this story would exceed token limit
            if (
                current_tokens + story_tokens + base_tokens > MAX_TOKENS_PER_BATCH
                and current_batch
            ):
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
        stories_content = ""
        for story in stories:
            stories_content += f"""
Story ID: {story['reddit_id']}
Subreddit: r/{story['subreddit']}
Flair: {story['flair'] or 'None'}
Content: {story['content']}


---


"""

        return EVALUATION_PROMPT.substitute(
            stories_content=stories_content.strip(),
            categories=", ".join(CATEGORIES),
            audiences=", ".join(TARGET_AUDIENCES),
        )

    def call_gemini(self, prompt: str) -> dict[str, list[EvaluationData]]:
        """Call Gemini API with the prompt."""
        try:
            response = self.client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json", temperature=0.3
                ),
            )

            return json.loads(response.text or "{}")
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

        # Validate score
        if not isinstance(evaluation["score"], int) or not (
            0 <= evaluation["score"] <= 1000
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
            response = self.call_gemini(prompt)
        except Exception:
            return False

        # Validate response structure
        if "evaluations" not in response:
            print("[ERROR] Invalid response structure: missing 'evaluations' key")
            return False

        evaluations = response["evaluations"]

        # Validate each evaluation
        valid_evaluations: list[dict[str, Any]] = []
        for evaluation in evaluations:
            if self.validate_evaluation(dict(evaluation)):
                valid_evaluations.append(dict(evaluation))
            else:
                print(f"[WARNING] Skipping invalid evaluation: {evaluation}")

        # Insert valid evaluations
        if valid_evaluations:
            inserted_count = self.db_manager.insert_evaluations(valid_evaluations)
            print(f"[INFO] Successfully inserted {inserted_count} evaluations")
            return inserted_count > 0
        else:
            print("[ERROR] No valid evaluations to insert")
            return False

    def run(self, max_stories: int) -> None:
        """Main execution function."""
        print(f"[INFO] Starting story evaluation with max_stories={max_stories}")

        # Connect and setup
        self.connect_and_setup()

        # Get unevaluated stories
        stories = self.get_unevaluated_stories(max_stories)

        if not stories:
            print("[INFO] No stories to evaluate")
            return

        # Create batches
        batches = self.create_batches(stories)

        # Process batches
        failures = 0
        total_processed = 0

        for i, batch in enumerate(batches, 1):
            print(f"[INFO] Processing batch {i}/{len(batches)}")

            success = self.process_batch(batch)

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
        self.db_manager.close()


def run_evaluator(max_stories: int) -> None:
    """Run the story evaluator."""
    evaluator = StoryEvaluator()
    evaluator.run(max_stories)
