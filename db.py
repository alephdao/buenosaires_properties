"""
Database module for Buenos Aires Properties scraper.
Handles SQLite operations with upsert and query tracking.
"""

import sqlite3
import os
from datetime import datetime
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)

# Database path
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'properties.db')

# Exchange rate for price normalization
PESO_TO_USD = 1500


def get_connection():
    """Get a database connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # Return rows as dictionaries
    return conn


def init_database():
    """Initialize database tables if they don't exist."""
    conn = get_connection()
    cursor = conn.cursor()

    # Create queries table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS queries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            url TEXT NOT NULL,
            neighborhoods TEXT,
            bedrooms TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            last_run DATETIME
        )
    ''')

    # Create properties table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS properties (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT UNIQUE NOT NULL,
            currency TEXT,
            price REAL,
            price_dollars INTEGER,
            expenses REAL,
            size REAL,
            bedrooms INTEGER,
            bathrooms INTEGER,
            listing_url TEXT,
            website TEXT,
            url TEXT,
            description TEXT,
            timestamp DATETIME NOT NULL,
            last_updated DATETIME NOT NULL,
            query_id INTEGER,
            price_total_usd REAL,
            is_filtered BOOLEAN DEFAULT 0,
            filtered_at DATETIME,
            FOREIGN KEY (query_id) REFERENCES queries(id)
        )
    ''')

    # Create index on address for faster lookups
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_address ON properties(address)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_is_filtered ON properties(is_filtered)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_filtered_at ON properties(filtered_at)')

    conn.commit()
    conn.close()
    logger.info("Database initialized successfully")


def add_query(name: str, url: str, neighborhoods: str = None, bedrooms: str = None) -> int:
    """Add a new query to the database."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('''
        INSERT INTO queries (name, url, neighborhoods, bedrooms)
        VALUES (?, ?, ?, ?)
    ''', (name, url, neighborhoods, bedrooms))

    query_id = cursor.lastrowid
    conn.commit()
    conn.close()

    logger.info(f"Added query '{name}' with ID {query_id}")
    return query_id


def get_query_by_name(name: str) -> Optional[Dict]:
    """Get a query by name."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM queries WHERE name = ?', (name,))
    row = cursor.fetchone()
    conn.close()

    if row:
        return dict(row)
    return None


def update_query_last_run(query_id: int):
    """Update the last_run timestamp for a query."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('''
        UPDATE queries SET last_run = ? WHERE id = ?
    ''', (datetime.now(), query_id))

    conn.commit()
    conn.close()


def calculate_price_dollars(currency: str, price: float) -> Optional[int]:
    """Calculate normalized price in USD, rounded to nearest dollar."""
    if not price:
        return None

    if currency == '$':  # Pesos
        return round(price / PESO_TO_USD)
    else:  # USD
        return round(price)


def upsert_property(property_data: Dict, query_id: int) -> tuple[bool, int]:
    """
    Insert or update a property. Uses address as unique key.
    Returns (is_new, property_id).
    """
    conn = get_connection()
    cursor = conn.cursor()

    address = property_data.get('address')
    if not address:
        raise ValueError("Property must have an address")

    # Check if property exists
    cursor.execute('SELECT id, timestamp FROM properties WHERE address = ?', (address,))
    existing = cursor.fetchone()

    if existing:
        # Update existing property, keep original timestamp
        property_id = existing['id']
        original_timestamp = existing['timestamp']

        # Calculate price_dollars
        price_dollars = calculate_price_dollars(
            property_data.get('currency'),
            property_data.get('price')
        )

        cursor.execute('''
            UPDATE properties SET
                currency = ?,
                price = ?,
                price_dollars = ?,
                expenses = ?,
                size = ?,
                bedrooms = ?,
                bathrooms = ?,
                listing_url = ?,
                website = ?,
                url = ?,
                description = ?,
                last_updated = ?,
                query_id = ?
            WHERE address = ?
        ''', (
            property_data.get('currency'),
            property_data.get('price'),
            price_dollars,
            property_data.get('expenses'),
            property_data.get('size'),
            property_data.get('bedrooms'),
            property_data.get('bathrooms'),
            property_data.get('listing_url'),
            property_data.get('website'),
            property_data.get('url'),
            property_data.get('description'),
            datetime.now(),
            query_id,
            address
        ))

        is_new = False
    else:
        # Insert new property
        timestamp = property_data.get('timestamp', datetime.now())
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp)

        # Calculate price_dollars
        price_dollars = calculate_price_dollars(
            property_data.get('currency'),
            property_data.get('price')
        )

        cursor.execute('''
            INSERT INTO properties (
                address, currency, price, price_dollars, expenses, size, bedrooms, bathrooms,
                listing_url, website, url, description, timestamp, last_updated, query_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            address,
            property_data.get('currency'),
            property_data.get('price'),
            price_dollars,
            property_data.get('expenses'),
            property_data.get('size'),
            property_data.get('bedrooms'),
            property_data.get('bathrooms'),
            property_data.get('listing_url'),
            property_data.get('website'),
            property_data.get('url'),
            property_data.get('description'),
            timestamp,
            datetime.now(),
            query_id
        ))

        property_id = cursor.lastrowid
        is_new = True

    conn.commit()
    conn.close()

    return is_new, property_id


def get_all_properties(filtered_only: bool = False) -> List[Dict]:
    """Get all properties from the database."""
    conn = get_connection()
    cursor = conn.cursor()

    if filtered_only:
        cursor.execute('SELECT * FROM properties WHERE is_filtered = 1 ORDER BY timestamp DESC')
    else:
        cursor.execute('SELECT * FROM properties ORDER BY timestamp DESC')

    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]


def get_properties_for_today_filtered() -> List[Dict]:
    """Get properties that were filtered today."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT * FROM properties
        WHERE is_filtered = 1
        AND DATE(filtered_at) = DATE('now')
        ORDER BY timestamp DESC
    ''')

    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]


def update_property_filtered_status(property_id: int, is_filtered: bool, price_total_usd: float = None):
    """Update the filtered status of a property."""
    conn = get_connection()
    cursor = conn.cursor()

    if is_filtered:
        cursor.execute('''
            UPDATE properties
            SET is_filtered = ?, filtered_at = ?, price_total_usd = ?
            WHERE id = ?
        ''', (1, datetime.now(), price_total_usd, property_id))
    else:
        cursor.execute('''
            UPDATE properties
            SET is_filtered = ?, price_total_usd = ?
            WHERE id = ?
        ''', (0, price_total_usd, property_id))

    conn.commit()
    conn.close()


def reset_filtered_status():
    """Reset all filtered statuses to False."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('UPDATE properties SET is_filtered = 0, filtered_at = NULL')

    conn.commit()
    conn.close()
    logger.info("Reset all filtered statuses")


def get_property_by_address(address: str) -> Optional[Dict]:
    """Get a property by its address."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM properties WHERE address = ?', (address,))
    row = cursor.fetchone()
    conn.close()

    if row:
        return dict(row)
    return None


def get_stats() -> Dict:
    """Get database statistics."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('SELECT COUNT(*) as total FROM properties')
    total = cursor.fetchone()['total']

    cursor.execute('SELECT COUNT(*) as filtered FROM properties WHERE is_filtered = 1')
    filtered = cursor.fetchone()['filtered']

    cursor.execute('SELECT COUNT(*) as today_filtered FROM properties WHERE is_filtered = 1 AND DATE(filtered_at) = DATE("now")')
    today_filtered = cursor.fetchone()['today_filtered']

    cursor.execute('SELECT COUNT(*) as queries FROM queries')
    queries = cursor.fetchone()['queries']

    conn.close()

    return {
        'total_properties': total,
        'filtered_properties': filtered,
        'today_filtered': today_filtered,
        'total_queries': queries
    }


if __name__ == '__main__':
    # Initialize database when run directly
    logging.basicConfig(level=logging.INFO)
    init_database()
    print("Database initialized successfully!")
    print(f"Database location: {DB_PATH}")
