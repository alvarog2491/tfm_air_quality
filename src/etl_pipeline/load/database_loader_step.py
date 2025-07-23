"""
Database loading step for persisting data to databases.
"""

import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any
from etl_pipeline import ETLStep


class DatabaseLoaderStep(ETLStep):
    """Step to load data into databases."""

    def __init__(
        self,
        connection_string: str,
        table_name: str,
        load_method: str = "replace",
        chunk_size: int = 10000,
    ):
        super().__init__("Database Loading")
        self.connection_string = connection_string
        self.table_name = table_name
        self.load_method = load_method  # 'replace', 'append', 'fail'
        self.chunk_size = chunk_size

    def execute(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Load dataset into database."""
        self.log_start()

        try:
            import sqlalchemy as sa

            # Create database engine
            engine = sa.create_engine(self.connection_string)

            # Load data in chunks for large datasets
            total_rows = len(df)
            rows_loaded = 0

            for chunk_start in range(0, total_rows, self.chunk_size):
                chunk_end = min(chunk_start + self.chunk_size, total_rows)
                chunk_df = df.iloc[chunk_start:chunk_end]

                # Load chunk to database
                chunk_df.to_sql(
                    name=self.table_name,
                    con=engine,
                    if_exists=self.load_method if chunk_start == 0 else "append",
                    index=False,
                    method="multi",  # Use multi-row inserts for better performance
                )

                rows_loaded += len(chunk_df)
                self.logger.info(f"Loaded {rows_loaded}/{total_rows} rows to database")

            # Verify data was loaded
            with engine.connect() as conn:
                result = conn.execute(
                    sa.text(f"SELECT COUNT(*) FROM {self.table_name}")
                )
                db_row_count = result.scalar()

            load_result = {
                "table_name": self.table_name,
                "rows_loaded": rows_loaded,
                "db_row_count": db_row_count,
                "load_method": self.load_method,
                "success": db_row_count >= rows_loaded,
            }

            engine.dispose()

            self.log_success(f"Loaded {rows_loaded} rows to table '{self.table_name}'")
            return load_result

        except ImportError:
            raise ImportError(
                "SQLAlchemy is required for database loading. Install with: pip install sqlalchemy"
            )
        except Exception as e:
            self.logger.error(f"Database loading failed: {str(e)}")
            raise
