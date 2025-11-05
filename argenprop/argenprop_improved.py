#!/usr/local/bin/python3

import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException, TimeoutException
from bs4 import BeautifulSoup
import re
import csv
import time
import os
from datetime import datetime
import json
import logging
import sys
import argparse
import yaml

# Add parent directory to path for db import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import db
import archive

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define file paths relative to script location
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
progress_file_path = os.path.join(script_dir, 'argenprop_progress.json')
queries_file_path = os.path.join(parent_dir, 'queries.yaml')

website = 'argenprop'
base_url = 'https://www.argenprop.com'

# Define patterns to search for the needed information
patterns = {
    'expenses': r'\+\s*\$\s*([\d\.]+)\s*expensas',
    'address': r'class="card__address"[^>]*>\s*([^<]+)',
    'size': r'(\d+)\s*m²\s*cubie',
    'bedrooms': r'(\d+)\s*dorm',
    'bathrooms': r'(\d+)\s*baños'
}

# Configure Chrome options
chrome_options = Options()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--disable-blink-features=AutomationControlled')
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option('useAutomationExtension', False)

def load_queries():
    """Load queries from YAML config file"""
    with open(queries_file_path, 'r') as f:
        config = yaml.safe_load(f)
    return config.get('queries', [])

def build_query_url(neighborhoods, bedrooms):
    """Build the search URL from neighborhoods and bedrooms"""
    return f"{base_url}/inmuebles/alquiler/{neighborhoods}/{bedrooms}?pagina-1"

def save_progress(page_num, url, query_id):
    """Save current progress to a JSON file"""
    progress = {
        'last_page': page_num,
        'last_url': url,
        'query_id': query_id,
        'timestamp': datetime.now().isoformat()
    }
    with open(progress_file_path, 'w') as f:
        json.dump(progress, f)
    logger.info(f"Progress saved: page {page_num}")

def load_progress():
    """Load progress from JSON file if it exists"""
    if os.path.exists(progress_file_path):
        try:
            with open(progress_file_path, 'r') as f:
                progress = json.load(f)
            logger.info(f"Resuming from page {progress['last_page']}")
            return progress
        except Exception as e:
            logger.error(f"Error loading progress: {e}")
    return None

def getdata_with_retry(url, max_retries=3, retry_delay=5):
    """Get page data with retry logic for network errors"""
    for attempt in range(max_retries):
        driver = None
        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.set_page_load_timeout(30)  # 30 second timeout
            driver.get(url)
            time.sleep(2)  # Increased wait time
            page_source = driver.page_source
            driver.quit()
            soup = BeautifulSoup(page_source, 'html.parser')
            return soup
        except (WebDriverException, TimeoutException) as e:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
            
            if attempt < max_retries - 1:
                logger.warning(f"Attempt {attempt + 1} failed: {str(e)[:100]}...")
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logger.error(f"All {max_retries} attempts failed for URL: {url}")
                raise
    
    return None

def getnextpage(soup):
    siguiente_tag = soup.find('a', {'aria-label': 'Siguiente'})
    if siguiente_tag and 'href' in siguiente_tag.attrs:
        return base_url + siguiente_tag['href']
    return None

def parse_listing(listing, url):
    """Parse a single listing"""
    listing_html = str(listing)
    listing_data = {}
    
    listing_data['website'] = website 
    
    # Extracting the listing URL
    link_tag = listing.find('a', href=True)
    if link_tag and 'href' in link_tag.attrs:
        listing_data['listing_url'] = base_url + link_tag['href']
    else:
        listing_data['listing_url'] = 'N/A'
    
    for key, pattern in patterns.items():
        match = re.search(pattern, listing_html)
        listing_data[key] = match.group(1).replace('.', '') if match else 'N/A'
    
    # currency_span is null when consult price
    currency_span = listing.find('span', class_='card__currency')
    price_span = currency_span.find_next_sibling(string=True) if currency_span else None
    
    if currency_span is not None:
        listing_data['currency'] = currency_span.text.strip()
    else:
        listing_data['currency'] = ''
    
    if price_span is not None:
        listing_data['price'] = int(re.sub(r'[^\d]+', '', price_span.strip()))
    else:
        listing_data['price'] = ''
    
    listing_data['url'] = url
    listing_data['description'] = listing.find('p', class_='card__info').text.strip() if listing.find('p', class_='card__info') else None
    listing_data['timestamp'] = datetime.now().isoformat()
    
    return listing_data

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='ArgentProp scraper with SQLite storage')
    parser.add_argument('--query', type=str, default=None, help='Query name from queries.yaml')
    parser.add_argument('--max-pages', type=int, default=None, help='Maximum pages to scrape (for testing)')
    args = parser.parse_args()

    # Initialize database
    db.init_database()

    # Load queries from config
    queries = load_queries()
    if not queries:
        logger.error("No queries found in queries.yaml")
        return

    # Select query
    if args.query:
        selected_query = next((q for q in queries if q['name'] == args.query), None)
        if not selected_query:
            logger.error(f"Query '{args.query}' not found in queries.yaml")
            logger.info(f"Available queries: {[q['name'] for q in queries]}")
            return
    else:
        # Use first query as default
        selected_query = queries[0]

    logger.info(f"Using query: {selected_query['name']}")

    # Get or create query in database
    query_record = db.get_query_by_name(selected_query['name'])
    if query_record:
        query_id = query_record['id']
        query_number = query_record.get('query_number')
        map_name = query_record.get('map_name')
        logger.info(f"Found existing query with ID {query_id} (query_number: {query_number})")
    else:
        query_url = build_query_url(selected_query['neighborhoods'], selected_query['bedrooms'])
        query_number = selected_query.get('query_number')
        map_name = selected_query.get('map_name')
        query_id = db.add_query(
            name=selected_query['name'],
            url=query_url,
            neighborhoods=selected_query['neighborhoods'],
            bedrooms=selected_query['bedrooms'],
            query_number=query_number,
            map_name=map_name
        )

    # Load progress or start fresh
    progress = load_progress()

    if progress and progress.get('query_id') == query_id:
        # Resume from last URL
        url = progress['last_url']
        page_num = progress['last_page']
        logger.info(f"Resuming from page {page_num}: {url}")
    else:
        # Start fresh
        url = build_query_url(selected_query['neighborhoods'], selected_query['bedrooms'])
        page_num = 1
        logger.info("Starting fresh scraping session")

        # Clear progress file if starting fresh
        if os.path.exists(progress_file_path):
            os.remove(progress_file_path)
    
    total_listings = 0
    
    try:
        while True:
            logger.info(f"Fetching page {page_num}: {url}")
            
            try:
                data = getdata_with_retry(url)
                
                apartment_listings = []
                listings = data.find_all('div', class_='listing__item')
                
                logger.info(f"Found {len(listings)} listings on page {page_num}")
                
                new_count = 0
                updated_count = 0

                for listing in listings:
                    try:
                        listing_data = parse_listing(listing, url)

                        # Convert string numbers to proper types
                        if listing_data.get('expenses') != 'N/A':
                            listing_data['expenses'] = float(listing_data['expenses'])
                        else:
                            listing_data['expenses'] = 0

                        if listing_data.get('size') != 'N/A':
                            listing_data['size'] = float(listing_data['size'])
                        else:
                            listing_data['size'] = None

                        if listing_data.get('bedrooms') != 'N/A':
                            listing_data['bedrooms'] = int(listing_data['bedrooms'])
                        else:
                            listing_data['bedrooms'] = None

                        if listing_data.get('bathrooms') != 'N/A':
                            listing_data['bathrooms'] = int(listing_data['bathrooms'])
                        else:
                            listing_data['bathrooms'] = None

                        # Upsert to database
                        is_new, prop_id = db.upsert_property(listing_data, query_id)
                        if is_new:
                            new_count += 1
                            # Archive new properties only
                            logger.info(f"Archiving new property {prop_id}: {listing_data['address']}")
                            archived_path = archive.archive_property_page(
                                property_id=prop_id,
                                address=listing_data['address'],
                                listing_url=listing_data['listing_url'],
                                date_scraped=listing_data.get('timestamp')
                            )
                            if archived_path:
                                db.update_archived_path(prop_id, archived_path)
                                logger.info(f"✓ Archived to {archived_path}")
                            else:
                                logger.warning(f"✗ Failed to archive property {prop_id}")
                        else:
                            updated_count += 1

                    except Exception as e:
                        logger.error(f"Error processing listing: {e}")
                        continue

                total_listings += new_count
                logger.info(f"Page {page_num} complete. New: {new_count}, Updated: {updated_count}, Total new so far: {total_listings}")

                # Save progress after each successful page
                save_progress(page_num, url, query_id)

                # Update query last_run
                db.update_query_last_run(query_id)
                
                # Random delay between pages
                sleep_duration = random.uniform(1, 3)
                time.sleep(sleep_duration)
                
                # Check if we've reached max pages (for testing)
                if args.max_pages and page_num >= args.max_pages:
                    logger.info(f"Reached max pages limit ({args.max_pages}). Stopping.")
                    break

                # Get next page
                next_page_url = getnextpage(data)
                if not next_page_url:
                    logger.info("No more pages found. Scraping complete!")
                    break

                url = next_page_url
                page_num += 1
                
            except Exception as e:
                logger.error(f"Error processing page {page_num}: {e}")
                logger.info("Saving progress before exiting...")
                save_progress(page_num, url, query_id)
                raise
    
    finally:
        # Clean up progress file on successful completion
        if os.path.exists(progress_file_path):
            os.remove(progress_file_path)
            logger.info("Progress file removed after successful completion")

        logger.info(f"Scraping finished. Total listings collected: {total_listings}")

        # Fix failed geocoding and generate updated map
        try:
            import subprocess

            # First, fix failed geocoding
            logger.info("\n=== Fixing geocoding for failed addresses ===")
            fix_script = os.path.join(parent_dir, 'analysis', 'fix_geocoding.py')
            subprocess.run([sys.executable, fix_script], check=True)
            logger.info("✓ Geocoding fixes complete")

            # Determine which bedroom count to map based on query
            # Query 1 (3-4br) -> map 3+ bedrooms
            # Query 2 (2+br) -> map 2 bedrooms
            bedroom_count = 2  # default
            if selected_query['name'] == "3-4br Belgrano/Palermo/Barrio Norte":
                bedroom_count = 3
            elif selected_query['name'] == "2+br or 3+amb Belgrano/Palermo/Barrio Norte":
                bedroom_count = 2

            # Then generate updated map
            # Only use cache if we didn't scrape any new properties
            cache_flag = "--cache-only" if total_listings == 0 else ""
            logger.info(f"\n=== Generating property map for {bedroom_count}-bedroom properties ===")
            logger.info(f"Scraped {total_listings} new properties - will geocode new addresses" if total_listings > 0 else "No new properties - using cache only")
            map_script = os.path.join(parent_dir, 'analysis', 'map_properties.py')

            # Pass query_id and map_name to the map script
            map_name_arg = f"--map-name={map_name}" if map_name else ""
            cmd = [sys.executable, map_script, str(bedroom_count), f"--query-id={query_id}"]
            if map_name_arg:
                cmd.append(map_name_arg)
            if cache_flag:
                cmd.append(cache_flag)

            subprocess.run(cmd, check=True)

            logger.info("✓ Map generation complete")
        except Exception as e:
            logger.error(f"Post-scraping analysis failed: {e}")

if __name__ == "__main__":
    main()