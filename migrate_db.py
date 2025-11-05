#!/usr/bin/env python3
"""
Database migration script to add query_number and map_name columns to queries table
"""

import sqlite3
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'properties.db')


def migrate():
    """Add query_number and map_name columns to queries table"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # Check if query_number column exists
        cursor.execute("PRAGMA table_info(queries)")
        columns = [col[1] for col in cursor.fetchall()]

        if 'query_number' not in columns:
            logger.info("Adding query_number column to queries table...")
            cursor.execute('ALTER TABLE queries ADD COLUMN query_number INTEGER')
            logger.info("✓ Added query_number column")
        else:
            logger.info("query_number column already exists")

        if 'map_name' not in columns:
            logger.info("Adding map_name column to queries table...")
            cursor.execute('ALTER TABLE queries ADD COLUMN map_name TEXT')
            logger.info("✓ Added map_name column")
        else:
            logger.info("map_name column already exists")

        # Create unique index on query_number if it doesn't exist
        cursor.execute("PRAGMA index_list(queries)")
        indexes = cursor.fetchall()
        has_query_number_index = any('query_number' in str(idx) for idx in indexes)

        if not has_query_number_index:
            logger.info("Creating unique index on query_number...")
            cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_query_number ON queries(query_number)')
            logger.info("✓ Created unique index on query_number")

        conn.commit()
        logger.info("✓ Migration completed successfully!")

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    logger.info(f"Running migration on database: {DB_PATH}")
    migrate()
