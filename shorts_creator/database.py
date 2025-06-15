"""
Database operations for Reddit story scraper and evaluator.
Supports SQLite and PostgreSQL databases using SQLAlchemy Core.
"""

import os
from abc import ABC, abstractmethod
from typing import Dict, List

from sqlalchemy import (
    Column,
    Engine,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    insert,
    text,
)
from sqlalchemy.dialects.postgresql import BIGINT as PostgreSQLBigint
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import INTEGER as SQLiteInteger
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import IntegrityError


class BaseDatabaseManager(ABC):
    """Abstract base class for database operations using SQLAlchemy Core."""

    def __init__(self) -> None:
        """Initialize the database manager."""
        self.engine: Engine | None = None
        self.metadata = MetaData()
        self.stories_table: Table | None = None
        self.evaluations_table: Table | None = None
        self._create_table_schema()

    @abstractmethod
    def _get_connection_string(self) -> str:
        """Return database-specific connection string."""
        pass

    @abstractmethod
    def _get_created_utc_column(self) -> type:
        """Return database-specific column type for created_utc."""
        pass

    @abstractmethod
    def _insert_evaluations_batch(
        self, conn, evaluations: List[Dict[str, str | int]]
    ) -> int:
        """Database-specific batch insert implementation."""
        pass

    def _create_table_schema(self) -> None:
        """Create the table schemas."""
        self.stories_table = Table(
            "stories",
            self.metadata,
            Column("reddit_id", String(255), primary_key=True),
            Column("subreddit", String(255), nullable=False),
            Column("content", Text, nullable=False),
            Column("created_utc", self._get_created_utc_column(), nullable=False),
            Column("flair", String(255), nullable=True),
        )

        self.evaluations_table = Table(
            "stories_evaluations",
            self.metadata,
            Column(
                "reddit_id",
                String(255),
                ForeignKey("stories.reddit_id"),
                primary_key=True,
            ),
            Column("score", Integer, nullable=False),
            Column("category", String(255), nullable=False),
            Column("target_audience", String(255), nullable=False),
        )

    def connect(self) -> None:
        """Establish database connection."""
        connection_string = self._get_connection_string()
        # Hide credentials in log output
        log_string = (
            connection_string.split("@")[-1]
            if "@" in connection_string
            else connection_string
        )
        print(f"[INFO] Connecting to database: {log_string}")
        self.engine = create_engine(connection_string)

    def create_tables(self) -> None:
        """Create all tables and views if they don't exist."""
        if self.engine is None:
            raise RuntimeError("Database not connected. Call connect() first.")

        self.metadata.create_all(self.engine)
        print("[INFO] Database tables created/verified")

        # Create the summary view
        self._create_summary_view()
        print("[INFO] Summary view created/verified")

    def _create_summary_view(self) -> None:
        """Create the summary view that joins stories and evaluations."""
        if self.engine is None:
            raise RuntimeError("Database not connected. Call connect() first.")

        # Drop view if it exists (for recreation with updated schema)
        drop_view_sql = "DROP VIEW IF EXISTS summary"

        # Create view with INNER JOIN using WHERE clause
        create_view_sql = """
        CREATE VIEW summary AS
        SELECT 
            s.reddit_id,
            s.subreddit,
            s.content,
            s.created_utc,
            s.flair,
            se.score,
            se.category,
            se.target_audience
        FROM stories s, stories_evaluations se
        WHERE s.reddit_id = se.reddit_id
        """

        with self.engine.connect() as conn:
            # Drop existing view
            conn.execute(text(drop_view_sql))
            # Create new view
            conn.execute(text(create_view_sql))
            conn.commit()

    def insert_story(
        self,
        reddit_id: str,
        subreddit: str,
        content: str,
        created_utc: int,
        flair: str | None = None,
    ) -> bool:
        """Insert a story, ignoring duplicates. Returns True if inserted, False if duplicate."""
        if self.engine is None:
            raise RuntimeError("Database not connected. Call connect() first.")

        if self.stories_table is None:
            raise RuntimeError("Stories table not initialized.")

        stmt = insert(self.stories_table).values(
            reddit_id=reddit_id,
            subreddit=subreddit,
            content=content,
            created_utc=created_utc,
            flair=flair,
        )

        try:
            with self.engine.connect() as conn:
                result = conn.execute(stmt)
                conn.commit()
                return result.rowcount > 0
        except IntegrityError:
            # Duplicate key - story already exists
            return False

    def get_unevaluated_stories(
        self, limit: int | None = None
    ) -> List[Dict[str, str | int | None]]:
        """Get stories that haven't been evaluated yet."""
        if self.engine is None:
            raise RuntimeError("Database not connected. Call connect() first.")

        query = """
        SELECT s.reddit_id, s.subreddit, s.content, s.created_utc, s.flair
        FROM stories s
        LEFT JOIN stories_evaluations se ON s.reddit_id = se.reddit_id
        WHERE se.reddit_id IS NULL
        ORDER BY s.created_utc DESC
        """

        if limit:
            query += f" LIMIT {limit}"

        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            stories: List[Dict[str, str | int | None]] = []
            for row in result:
                stories.append(
                    {
                        "reddit_id": row[0],
                        "subreddit": row[1],
                        "content": row[2],
                        "created_utc": row[3],
                        "flair": row[4],
                    }
                )

        print(f"[INFO] Found {len(stories)} unevaluated stories")
        return stories

    def insert_evaluations(self, evaluations: List[Dict[str, str | int]]) -> int:
        """Insert evaluations into database, returns number of successful insertions."""
        if self.engine is None:
            raise RuntimeError("Database not connected. Call connect() first.")

        if self.evaluations_table is None:
            raise RuntimeError("Evaluations table not initialized.")

        if not evaluations:
            return 0

        try:
            with self.engine.connect() as conn:
                successful_insertions = self._insert_evaluations_batch(
                    conn, evaluations
                )
                conn.commit()
                return successful_insertions
        except Exception as e:
            print(f"[ERROR] Batch insert failed: {str(e)}")
            return 0

    def close(self) -> None:
        """Close database connection."""
        if self.engine:
            self.engine.dispose()


class SQLiteDatabaseManager(BaseDatabaseManager):
    """SQLite-specific database manager."""

    def _get_connection_string(self) -> str:
        """Return SQLite connection string."""
        db_path = os.getenv("DB_PATH", "./stories.db")
        return f"sqlite:///{db_path}"

    def _get_created_utc_column(self) -> type:
        """Return SQLite-specific column type for created_utc."""
        return SQLiteInteger

    def _insert_evaluations_batch(
        self, conn, evaluations: List[Dict[str, str | int]]
    ) -> int:
        """SQLite-specific batch insert implementation."""
        if self.evaluations_table is None:
            raise RuntimeError("Evaluations table not initialized.")

        successful_insertions = 0

        try:
            # Try bulk insert first
            stmt = sqlite_insert(self.evaluations_table).values(evaluations)
            stmt = stmt.on_conflict_do_nothing()
            result = conn.execute(stmt)
            successful_insertions = result.rowcount
        except Exception as e:
            # Fallback to individual inserts for SQLite if bulk fails
            print(f"[WARNING] Bulk insert failed, trying individual inserts: {str(e)}")
            conn.rollback()
            for eval_data in evaluations:
                try:
                    stmt = sqlite_insert(self.evaluations_table).values(
                        reddit_id=eval_data["reddit_id"],
                        score=eval_data["score"],
                        category=eval_data["category"],
                        target_audience=eval_data["target_audience"],
                    )
                    stmt = stmt.on_conflict_do_nothing()
                    result = conn.execute(stmt)
                    if result.rowcount > 0:
                        successful_insertions += 1
                except Exception as inner_e:
                    print(
                        f"[ERROR] Failed to insert evaluation for {eval_data['reddit_id']}: {str(inner_e)}"
                    )

        print(f"[INFO] Successfully inserted {successful_insertions} evaluations")
        return successful_insertions


class PostgreSQLDatabaseManager(BaseDatabaseManager):
    """PostgreSQL-specific database manager."""

    def _get_connection_string(self) -> str:
        """Return PostgreSQL connection string."""
        db_string = os.getenv("DB_STRING")

        if not db_string:
            raise ValueError("PostgreSQL requires DB_STRING environment variables")

        return db_string

    def _get_created_utc_column(self) -> type:
        """Return PostgreSQL-specific column type for created_utc."""
        return PostgreSQLBigint

    def _insert_evaluations_batch(
        self, conn, evaluations: List[Dict[str, str | int]]
    ) -> int:
        """PostgreSQL-specific batch insert implementation."""
        if self.evaluations_table is None:
            raise RuntimeError("Evaluations table not initialized.")

        stmt = pg_insert(self.evaluations_table).values(evaluations)
        stmt = stmt.on_conflict_do_nothing(index_elements=["reddit_id"])
        result = conn.execute(stmt)
        successful_insertions = result.rowcount

        print(f"[INFO] Successfully inserted {successful_insertions} evaluations")
        return successful_insertions


def create_database_manager() -> BaseDatabaseManager:
    """Factory function to create appropriate database manager."""
    db_type = os.getenv("DB_TYPE", "sqlite").lower()

    if db_type == "sqlite":
        return SQLiteDatabaseManager()
    elif db_type == "postgresql":
        return PostgreSQLDatabaseManager()
    else:
        raise ValueError(f"Unsupported database type: {db_type}")
