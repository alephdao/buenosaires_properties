#!/usr/bin/env python3
"""
Script to update existing queries with query_number and map_name from queries.yaml
"""

import sqlite3
import os
import yaml
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'properties.db')
QUERIES_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'queries.yaml')


def update_queries():
    """Update existing queries with query_number and map_name from yaml"""
    # Load queries from yaml
    with open(QUERIES_PATH, 'r') as f:
        config = yaml.safe_load(f)
    yaml_queries = config.get('queries', [])

    if not yaml_queries:
        logger.error("No queries found in queries.yaml")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        for yaml_query in yaml_queries:
            query_name = yaml_query['name']
            query_number = yaml_query.get('query_number')
            map_name = yaml_query.get('map_name')

            # Check if query exists in database
            cursor.execute('SELECT id, query_number, map_name FROM queries WHERE name = ?', (query_name,))
            result = cursor.fetchone()

            if result:
                query_id, current_query_number, current_map_name = result

                # Update if values have changed
                if current_query_number != query_number or current_map_name != map_name:
                    logger.info(f"Updating query '{query_name}' (ID: {query_id})")
                    logger.info(f"  query_number: {current_query_number} -> {query_number}")
                    logger.info(f"  map_name: {current_map_name} -> {map_name}")

                    cursor.execute('''
                        UPDATE queries
                        SET query_number = ?, map_name = ?
                        WHERE id = ?
                    ''', (query_number, map_name, query_id))
                    logger.info(f"✓ Updated query '{query_name}'")
                else:
                    logger.info(f"Query '{query_name}' already up to date")
            else:
                logger.warning(f"Query '{query_name}' not found in database")

        conn.commit()
        logger.info("✓ All queries updated successfully!")

        # Show current state
        logger.info("\nCurrent queries in database:")
        cursor.execute('SELECT id, query_number, name, map_name FROM queries ORDER BY query_number')
        for row in cursor.fetchall():
            logger.info(f"  ID={row[0]}, query_number={row[1]}, name='{row[2]}', map_name='{row[3]}'")

    except Exception as e:
        logger.error(f"Update failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    logger.info(f"Updating queries from {QUERIES_PATH}")
    update_queries()
