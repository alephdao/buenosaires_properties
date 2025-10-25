#!/usr/bin/env python3
"""
Backfill script to import existing CSV data into the SQLite database.
"""

import csv
import db
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def backfill_from_csv(csv_file_path, query_name="Historical Data - 3-4br"):
    """Backfill database from existing CSV file."""

    if not os.path.exists(csv_file_path):
        logger.error(f"CSV file not found: {csv_file_path}")
        return

    # Initialize database
    db.init_database()

    # Create a query for the historical data
    query_record = db.get_query_by_name(query_name)
    if query_record:
        query_id = query_record['id']
        logger.info(f"Using existing query: {query_name} (ID: {query_id})")
    else:
        query_id = db.add_query(
            name=query_name,
            url="https://www.argenprop.com/inmuebles/alquiler/belgrano-o-br-norte-o-palermo/3-dormitorios-o-4-dormitorios-o-5-o-mas-dormitorios",
            neighborhoods="belgrano-o-br-norte-o-palermo",
            bedrooms="3-dormitorios-o-4-dormitorios-o-5-o-mas-dormitorios"
        )
        logger.info(f"Created new query: {query_name} (ID: {query_id})")

    # Read CSV and import
    with open(csv_file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        new_count = 0
        updated_count = 0
        error_count = 0

        for row in reader:
            try:
                # Convert data types
                property_data = {
                    'address': row['address'],
                    'currency': row['currency'],
                    'price': float(row['price']) if row['price'] and row['price'] != '' else None,
                    'expenses': float(row['expenses']) if row['expenses'] and row['expenses'] != 'N/A' else 0,
                    'size': float(row['size']) if row['size'] and row['size'] != 'N/A' else None,
                    'bedrooms': int(float(row['bedrooms'])) if row['bedrooms'] and row['bedrooms'] != 'N/A' else None,
                    'bathrooms': int(float(row['bathrooms'])) if row['bathrooms'] and row['bathrooms'] != 'N/A' else None,
                    'listing_url': row['listing_url'],
                    'website': row.get('website', 'argenprop'),
                    'url': row.get('url', ''),
                    'description': row.get('description', ''),
                    'timestamp': row['timestamp']
                }

                # Upsert property
                is_new, prop_id = db.upsert_property(property_data, query_id)

                if is_new:
                    new_count += 1
                else:
                    updated_count += 1

                if (new_count + updated_count) % 100 == 0:
                    logger.info(f"Processed {new_count + updated_count} rows...")

            except Exception as e:
                error_count += 1
                logger.error(f"Error importing row: {e}")
                logger.debug(f"Row data: {row}")
                continue

    logger.info(f"\n{'='*60}")
    logger.info(f"BACKFILL COMPLETE")
    logger.info(f"{'='*60}")
    logger.info(f"New properties: {new_count}")
    logger.info(f"Updated properties: {updated_count}")
    logger.info(f"Errors: {error_count}")
    logger.info(f"Total processed: {new_count + updated_count}")
    logger.info(f"{'='*60}\n")

    # Show database stats
    stats = db.get_stats()
    logger.info(f"Database now contains {stats['total_properties']} total properties")


def main():
    # Backfill from argenprop CSV
    csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'argenprop', 'argenprop_listings.csv')

    if os.path.exists(csv_path):
        logger.info(f"Starting backfill from: {csv_path}")
        backfill_from_csv(csv_path)
    else:
        logger.warning(f"No CSV file found at: {csv_path}")
        logger.info("Skipping backfill. Run the scraper to populate the database.")

    # Backfill from listings_clean CSV (filtered historical data)
    clean_csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'listings_clean.csv')

    if os.path.exists(clean_csv_path):
        logger.info(f"Starting backfill from: {clean_csv_path}")
        backfill_from_csv(clean_csv_path, query_name="Historical Data - Cleaned")
    else:
        logger.info("No listings_clean.csv found, skipping.")


if __name__ == '__main__':
    main()
