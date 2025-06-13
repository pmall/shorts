import os
from abc import ABC, abstractmethod
from typing import Optional
from sqlalchemy import (
    create_engine,
    Table,
    Column,
    String,
    Text,
    MetaData,
    insert,
    Engine,
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
        """Create the table schema"""
        self.stories_table = Table(
            "stories",
            self.metadata,
            Column("reddit_id", String(255), primary_key=True),
            Column("subreddit", String(255), nullable=False),
            Column("content", Text, nullable=False),
            Column("created_utc", self._get_created_utc_column(), nullable=False),
            Column("flair", String(255), nullable=True),
        )

    def connect(self) -> None:
        """Establish database connection"""
        connection_string = self._get_connection_string()
        print(
            f"[INFO] Connecting to database: {connection_string.split('@')[-1] if '@' in connection_string else connection_string}"
        )
        self.engine = create_engine(connection_string)

    def create_table(self) -> None:
        """Create the stories table if it doesn't exist"""
        if self.engine is None:
            raise RuntimeError("Database not connected. Call connect() first.")

        self.metadata.create_all(self.engine)
        print("[INFO] Database table created/verified")

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
