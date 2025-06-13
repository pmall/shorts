import os
from abc import ABC, abstractmethod
from typing import Any, Optional
from sqlalchemy import (
    create_engine,
    Table,
    Column,
    String,
    Text,
    MetaData,
    insert,
    Engine,
    Integer,
    ForeignKey,
    text,
)
from sqlalchemy.dialects.sqlite import INTEGER as SQLiteInteger
from sqlalchemy.dialects.postgresql import BIGINT as PostgreSQLBigint
from sqlalchemy.exc import IntegrityError


class BaseDatabaseManager(ABC):
    """Abstract base class for database operations using SQLAlchemy Core"""

    def __init__(self):
        self.engine: Optional[Engine] = None
        self.metadata = MetaData()
        self.stories_table: Optional[Table] = None
        self.evaluations_table: Optional[Table] = None
        self._create_table_schema()

    @abstractmethod
    def _get_connection_string(self) -> str:
        """Return database-specific connection string"""
        pass

    @abstractmethod
    def _get_created_utc_column(self):
        """Return database-specific column type for created_utc"""
        pass

    def _create_table_schema(self) -> None:
        """Create the table schemas"""
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
            "story_evaluations",
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
        """Establish database connection"""
        connection_string = self._get_connection_string()
        print(
            f"[INFO] Connecting to database: {connection_string.split('@')[-1] if '@' in connection_string else connection_string}"
        )
        self.engine = create_engine(connection_string)

    def create_table(self) -> None:
        """Create all tables if they don't exist"""
        if self.engine is None:
            raise RuntimeError("Database not connected. Call connect() first.")

        self.metadata.create_all(self.engine)
        print("[INFO] Database tables created/verified")

    def insert_story(
        self,
        reddit_id: str,
        subreddit: str,
        content: str,
        created_utc: int,
        flair: Optional[str] = None,
    ) -> bool:
        """Insert a story, ignoring duplicates. Returns True if inserted, False if duplicate"""
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
        self, limit: Optional[int] = None
    ) -> list[dict[str, Any]]:
        """Get stories that haven't been evaluated yet"""
        if self.engine is None:
            raise RuntimeError("Database not connected. Call connect() first.")

        query = """
        SELECT s.reddit_id, s.subreddit, s.content, s.created_utc, s.flair
        FROM stories s
        LEFT JOIN story_evaluations se ON s.reddit_id = se.reddit_id
        WHERE se.reddit_id IS NULL
        ORDER BY s.created_utc DESC
        """

        if limit:
            query += f" LIMIT {limit}"

        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            stories = []
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

    def insert_evaluations(self, evaluations: list[dict[str, Any]]) -> int:
        """Insert evaluations into database, returns number of successful insertions"""
        if self.engine is None:
            raise RuntimeError("Database not connected. Call connect() first.")

        if self.evaluations_table is None:
            raise RuntimeError("Evaluations table not initialized.")

        successful_insertions = 0

        with self.engine.connect() as conn:
            for eval_data in evaluations:
                try:
                    stmt = insert(self.evaluations_table).values(
                        reddit_id=eval_data["reddit_id"],
                        score=eval_data["score"],
                        category=eval_data["category"],
                        target_audience=eval_data["target_audience"],
                    )
                    conn.execute(stmt)
                    successful_insertions += 1
                except IntegrityError:
                    print(
                        f"[WARNING] Duplicate evaluation for story {eval_data['reddit_id']}"
                    )
                except Exception as e:
                    print(
                        f"[ERROR] Failed to insert evaluation for {eval_data['reddit_id']}: {str(e)}"
                    )

            conn.commit()

        return successful_insertions

    def close(self) -> None:
        """Close database connection"""
        if self.engine:
            self.engine.dispose()


class SQLiteDatabaseManager(BaseDatabaseManager):
    """SQLite-specific database manager"""

    def _get_connection_string(self) -> str:
        db_path = os.getenv("DB_PATH", "./stories.db")
        return f"sqlite:///{db_path}"

    def _get_created_utc_column(self):
        return SQLiteInteger


class PostgreSQLDatabaseManager(BaseDatabaseManager):
    """PostgreSQL-specific database manager"""

    def _get_connection_string(self) -> str:
        host = os.getenv("DB_HOST", "localhost")
        port = os.getenv("DB_PORT", "5432")
        database = os.getenv("DB_NAME")
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")

        return f"postgresql://{user}:{password}@{host}:{port}/{database}"

    def _get_created_utc_column(self):
        return PostgreSQLBigint


def create_database_manager() -> BaseDatabaseManager:
    """Factory function to create appropriate database manager"""
    db_type = os.getenv("DB_TYPE", "sqlite").lower()

    if db_type == "sqlite":
        return SQLiteDatabaseManager()
    elif db_type == "postgresql":
        return PostgreSQLDatabaseManager()
    else:
        raise ValueError(f"Unsupported database type: {db_type}")
