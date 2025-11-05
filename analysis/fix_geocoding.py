#!/usr/bin/env python3
"""
Fix failed geocoding by cleaning up address formats
"""

import json
import re
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

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

def geocode_single_address(args):
    """Geocode a single address (for parallel processing)"""
    i, total, original_addr, geolocator = args

    cleaned_addr = clean_address(original_addr)

    # Skip if cleaning didn't change anything
    if cleaned_addr == original_addr:
        return (original_addr, None, f"{i+1}/{total}: Skipping (no change): {original_addr[:60]}")

    try:
        full_address = f"{cleaned_addr}, Buenos Aires, Argentina"
        location = geolocator.geocode(full_address)

        if location:
            result = {
                'latitude': location.latitude,
                'longitude': location.longitude
            }
            msg = f"✓ {i+1}/{total}: {original_addr[:50]}\n   Cleaned to: {cleaned_addr[:50]}"
            return (original_addr, result, msg)
        else:
            return (original_addr, None, f"✗ {i+1}/{total}: Still failed: {original_addr[:60]}")

    except Exception as e:
        return (original_addr, None, f"✗ {i+1}/{total}: Error: {str(e)[:50]}")

def main(workers=8):
    print("=== Fixing Failed Geocoding ===\n")

    # Load cache
    cache = load_geocache()
    print(f"Loaded {len(cache)} cached addresses")

    # Find failed addresses
    failed = [(addr, loc) for addr, loc in cache.items() if loc.get('latitude') is None]
    print(f"Found {len(failed)} failed addresses to retry")
    print(f"Using {workers} parallel workers\n")

    if not failed:
        print("No failed addresses to fix!")
        return

    # Setup geocoders (one per worker to avoid conflicts)
    geolocators = [
        Nominatim(user_agent=f"buenosaires_properties_analyzer_worker{i}", timeout=10)
        for i in range(workers)
    ]

    success_count = 0
    still_failed_count = 0
    cache_lock = threading.Lock()

    # Prepare tasks
    tasks = [
        (i, len(failed), addr, geolocators[i % workers])
        for i, (addr, _) in enumerate(failed)
    ]

    # Process in parallel with rate limiting per worker
    with ThreadPoolExecutor(max_workers=workers) as executor:
        # Submit all tasks
        future_to_addr = {
            executor.submit(geocode_single_address, task): task[1]
            for task in tasks
        }

        # Process results as they complete
        for future in as_completed(future_to_addr):
            original_addr, result, msg = future.result()
            print(msg)

            # Update cache thread-safely
            with cache_lock:
                if result:
                    cache[original_addr] = result
                    success_count += 1

                    # Save every 10 successes
                    if success_count % 10 == 0:
                        save_geocache(cache)
                        print(f"   Saved progress ({success_count} fixed so far)")
                else:
                    still_failed_count += 1

            # Rate limit: small delay between requests
            time.sleep(0.2)  # With 8 workers, this gives ~1.6 second spacing per worker

    # Final save
    save_geocache(cache)

    print(f"\n=== Summary ===")
    print(f"Successfully fixed: {success_count}")
    print(f"Still failed: {still_failed_count}")
    print(f"Total processed: {len(failed)}")
    print(f"\nCache updated with {len(cache)} total addresses")

if __name__ == "__main__":
    main()
