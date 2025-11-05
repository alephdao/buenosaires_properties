#!/usr/bin/env python3
"""
Fix failed geocoding by cleaning up address formats
"""

import json
import re
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import time

CACHE_FILE = '/Users/philipgalebach/coding-projects/buenosaires_properties/analysis/geocache.json'

def clean_address(address):
    """Clean address by removing floor info and other confusing elements"""
    # Remove floor information: ", Piso X" or "Piso PB" or similar
    address = re.sub(r',?\s*[Pp]iso\s+\w+', '', address)

    # Remove patterns like "al 2500" (means "at street number 2500")
    address = re.sub(r'\s+al\s+(\d+)', r' \1', address)

    # Remove patterns like "8°" or "7°" (floor indicators)
    address = re.sub(r'\s+\d+°', '', address)

    # Remove patterns like "e/" (between streets) - just take the first street
    if 'e/' in address.lower():
        address = address.split('e/')[0].strip()

    # Remove "Y" (and) when between street names
    if ' Y ' in address.upper():
        # Keep just the first street
        parts = re.split(r'\s+[Yy]\s+', address)
        if len(parts) > 1:
            address = parts[0].strip()

    # Normalize "Av" and "AV" to "Avenida"
    address = re.sub(r'\b[Aa][Vv]\.?\s+', 'Avenida ', address)

    # Clean up multiple spaces
    address = re.sub(r'\s+', ' ', address).strip()

    return address

def load_geocache():
    """Load geocoding cache"""
    with open(CACHE_FILE, 'r') as f:
        return json.load(f)

def save_geocache(cache):
    """Save geocoding cache"""
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=2)

def main():
    print("=== Fixing Failed Geocoding ===\n")

    # Load cache
    cache = load_geocache()
    print(f"Loaded {len(cache)} cached addresses")

    # Find failed addresses
    failed = [(addr, loc) for addr, loc in cache.items() if loc.get('latitude') is None]
    print(f"Found {len(failed)} failed addresses to retry\n")

    if not failed:
        print("No failed addresses to fix!")
        return

    # Setup geocoder
    geolocator = Nominatim(user_agent="buenosaires_properties_analyzer", timeout=10)
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1.5)

    success_count = 0
    still_failed_count = 0

    for i, (original_addr, _) in enumerate(failed):
        cleaned_addr = clean_address(original_addr)

        # Skip if cleaning didn't change anything
        if cleaned_addr == original_addr:
            print(f"{i+1}/{len(failed)}: Skipping (no change): {original_addr[:60]}")
            still_failed_count += 1
            continue

        try:
            full_address = f"{cleaned_addr}, Buenos Aires, Argentina"
            location = geocode(full_address)

            if location:
                # Update cache with new successful location
                cache[original_addr] = {
                    'latitude': location.latitude,
                    'longitude': location.longitude
                }
                success_count += 1
                print(f"✓ {i+1}/{len(failed)}: {original_addr[:50]}")
                print(f"   Cleaned to: {cleaned_addr[:50]}")

                # Save every 10 successes
                if success_count % 10 == 0:
                    save_geocache(cache)
                    print(f"   Saved progress ({success_count} fixed so far)")
            else:
                still_failed_count += 1
                print(f"✗ {i+1}/{len(failed)}: Still failed: {original_addr[:60]}")

        except Exception as e:
            still_failed_count += 1
            print(f"✗ {i+1}/{len(failed)}: Error: {str(e)[:50]}")

    # Final save
    save_geocache(cache)

    print(f"\n=== Summary ===")
    print(f"Successfully fixed: {success_count}")
    print(f"Still failed: {still_failed_count}")
    print(f"Total processed: {len(failed)}")
    print(f"\nCache updated with {len(cache)} total addresses")

if __name__ == "__main__":
    main()
