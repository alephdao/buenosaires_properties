#!/usr/bin/env python3

import db
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

dollarblue = 1200  # PJG update to the current rate.

def calculate_price_total_usd(currency, price, expenses):
    """Calculate total USD price from currency, price, and expenses."""
    if not price:
        return None

    if currency == '$':
        # ARS - convert to USD
        return (price / dollarblue) + (expenses / dollarblue)
    else:
        # USD - just add expenses converted from ARS
        return price + (expenses / dollarblue)

def apply_filters():
    """Apply filtering rules to all properties in the database."""
    # Initialize database
    db.init_database()

    # Reset all filtered statuses
    db.reset_filtered_status()

    # Get all properties
    properties = db.get_all_properties()
    logger.info(f"Processing {len(properties)} total properties")

    # Count today's properties
    today = datetime.now().date()
    today_properties = [p for p in properties if datetime.fromisoformat(p['timestamp']).date() == today]
    logger.info(f"Found {len(today_properties)} properties scraped today")

    filtered_count = 0

    for prop in properties:
        # Calculate price_total_usd
        price_total_usd = calculate_price_total_usd(
            prop['currency'],
            prop['price'],
            prop['expenses'] or 0
        )

        # Apply filters
        passes_filters = True

        # Size filter: >= 90m² or N/A
        if prop['size'] is not None and prop['size'] < 90:
            passes_filters = False

        # Price filter: $300-1500 USD
        if price_total_usd is None or price_total_usd < 300 or price_total_usd > 1500:
            passes_filters = False

        # Date filter: only today
        prop_date = datetime.fromisoformat(prop['timestamp']).date()
        if prop_date != today:
            passes_filters = False

        # Update property with filtered status
        if passes_filters:
            db.update_property_filtered_status(prop['id'], True, price_total_usd)
            filtered_count += 1
        else:
            db.update_property_filtered_status(prop['id'], False, price_total_usd)

    logger.info(f"Filtered {filtered_count} properties matching criteria")

    # Get and display filtered properties
    filtered_properties = db.get_properties_for_today_filtered()
    logger.info(f"\n{'='*60}")
    logger.info(f"FILTERED PROPERTIES ({len(filtered_properties)} total)")
    logger.info(f"{'='*60}")

    for prop in filtered_properties:
        logger.info(f"\n📍 {prop['address']}")
        logger.info(f"   💵 ${prop['price_total_usd']:.0f}/month")
        logger.info(f"   📏 {prop['size']}m² | 🛏️ {prop['bedrooms']} bed")
        logger.info(f"   🔗 {prop['listing_url']}")

    return filtered_count

def main():
    filtered_count = apply_filters()
    stats = db.get_stats()

    print(f"\n{'='*60}")
    print(f"DATABASE STATISTICS")
    print(f"{'='*60}")
    print(f"Total properties in database: {stats['total_properties']}")
    print(f"Filtered properties (all time): {stats['filtered_properties']}")
    print(f"Filtered properties (today): {stats['today_filtered']}")
    print(f"Total queries: {stats['total_queries']}")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()


# geolocator = Nominatim(user_agent="geoapiExercises")

# def get_long_lat(address):
#     try:
#         time.sleep(1)  # Delay to prevent rate limiting
#         location = geolocator.geocode(address)
#         if location:
#             return location.longitude, location.latitude
#         else:
#             logger.warning(f"Could not geocode address: {address}")
#             return None, None
#     except (GeocoderTimedOut, GeocoderServiceError) as e:
#         logger.error(f"Geocoding error for address {address}: {e}")
#         return None, None

# Apply the function to create longitude and latitude fields
# df['longitude'], df['latitude'] = zip(*df['address'].map(get_long_lat))