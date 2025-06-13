#!/usr/bin/env python3
"""
Story Evaluator
Uses Gemini AI to evaluate Reddit stories for viral short video potential.
"""

import json
import time
import argparse
from string import Template
from typing import Any, Optional
from google import genai
from google.genai import types

from database import create_database_manager

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


class StoryEvaluator:
    def __init__(self):
        self.db_manager = create_database_manager()
        self.client = genai.Client()

    def connect_and_setup(self):
        """Connect to database and create tables"""
        self.db_manager.connect()
        self.db_manager.create_table()

    def get_unevaluated_stories(
        self, limit: Optional[int] = None
    ) -> list[dict[str, Any]]:
        """Get stories that haven't been evaluated yet"""
        return self.db_manager.get_unevaluated_stories(limit)

    def estimate_tokens(self, text: str) -> int:
        """Rough token estimation (1 token ≈ 0.75 words)"""
        word_count = len(text.split())
        return int(word_count / 0.75)

    def create_batches(
        self, stories: list[dict[str, Any]]
    ) -> list[list[dict[str, Any]]]:
        """Create batches of stories respecting token limits and grouping by length"""
        # Sort stories by content length
        stories_by_length = sorted(stories, key=lambda x: len(x["content"]))

        batches = []
        current_batch = []
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

    def build_prompt(self, stories: list[dict[str, Any]]) -> str:
        """Build the evaluation prompt for a batch of stories"""
        stories_content = ""
        for story in stories:
            stories_content += f"""
Story ID: {story['reddit_id']}\n
Subreddit: r/{story['subreddit']}\n
Flair: {story['flair'] or 'None'}\n
Content: {story['content']}\n
\n\n
---
\n\n
"""

        return EVALUATION_PROMPT.substitute(
            stories_content=stories_content.strip(),
            categories=", ".join(CATEGORIES),
            audiences=", ".join(TARGET_AUDIENCES),
        )

    def call_gemini(self, prompt: str) -> dict[str, Any]:
        """Call Gemini API with the prompt"""
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

    def insert_evaluations(self, evaluations: list[dict[str, Any]]) -> int:
        """Insert evaluations into database, returns number of successful insertions"""
        return self.db_manager.insert_evaluations(evaluations)

    def validate_evaluation(self, evaluation: dict[str, Any]) -> bool:
        """Validate a single evaluation"""
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

    def process_batch(self, batch: list[dict[str, Any]]) -> bool:
        """Process a single batch of stories, returns True if successful"""
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
        valid_evaluations = []
        for evaluation in evaluations:
            if self.validate_evaluation(evaluation):
                valid_evaluations.append(evaluation)
            else:
                print(f"[WARNING] Skipping invalid evaluation: {evaluation}")

        # Insert valid evaluations
        if valid_evaluations:
            inserted_count = self.insert_evaluations(valid_evaluations)
            print(f"[INFO] Successfully inserted {inserted_count} evaluations")
            return inserted_count > 0
        else:
            print("[ERROR] No valid evaluations to insert")
            return False

    def run(self, max_stories: int):
        """Main execution function"""
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

        # Close database connections
        self.db_manager.close()
        self.db_manager.close()


def run_evaluator(max_stories: int):
    evaluator = StoryEvaluator()
    evaluator.run(max_stories)


def main():
    parser = argparse.ArgumentParser(
        description="Evaluate Reddit stories for viral short video potential"
    )
    parser.add_argument(
        "--max-stories",
        type=int,
        default=1000,
        help="Maximum number of stories to evaluate (default: 1000)",
    )

    args = parser.parse_args()

    run_evaluator(args.max_stories)


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    main()
