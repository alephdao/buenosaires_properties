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

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define file paths relative to script location
script_dir = os.path.dirname(os.path.abspath(__file__))
csv_file_path = os.path.join(script_dir, 'argenprop_listings.csv')
progress_file_path = os.path.join(script_dir, 'argenprop_progress.json')

website = 'argenprop'
base_url = 'https://www.argenprop.com'

# Define the fieldnames for the CSV
fieldnames = ['address','currency', 'price', 'expenses', 'size', 'bedrooms', 'bathrooms', 'listing_url', 'website', 'url', 'description', 'timestamp']

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

def save_progress(page_num, url):
    """Save current progress to a JSON file"""
    progress = {
        'last_page': page_num,
        'last_url': url,
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
    # Load progress or start fresh
    progress = load_progress()
    
    if progress and os.path.exists(csv_file_path):
        # Resume from last URL
        url = progress['last_url']
        page_num = progress['last_page']
        logger.info(f"Resuming from page {page_num}: {url}")
    else:
        # Start fresh
        url = base_url + '/inmuebles/alquiler/belgrano-o-br-norte-o-palermo/3-dormitorios-o-4-dormitorios-o-5-o-mas-dormitorios?pagina-1'
        page_num = 1
        logger.info("Starting fresh scraping session")
        
        # Clear existing files if starting fresh
        if os.path.exists(csv_file_path):
            os.remove(csv_file_path)
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
                
                for listing in listings:
                    try:
                        listing_data = parse_listing(listing, url)
                        apartment_listings.append(listing_data)
                    except Exception as e:
                        logger.error(f"Error parsing listing: {e}")
                        continue
                
                # Write to CSV
                with open(csv_file_path, mode='a', newline='', encoding='utf-8') as file:
                    writer = csv.DictWriter(file, fieldnames=fieldnames)
                    if file.tell() == 0:
                        writer.writeheader()
                    for listing in apartment_listings:
                        writer.writerow(listing)
                
                total_listings += len(apartment_listings)
                logger.info(f"Page {page_num} complete. Total listings so far: {total_listings}")
                
                # Save progress after each successful page
                save_progress(page_num, url)
                
                # Random delay between pages
                sleep_duration = random.uniform(1, 3)
                time.sleep(sleep_duration)
                
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
                save_progress(page_num, url)
                raise
    
    finally:
        # Clean up progress file on successful completion
        if os.path.exists(progress_file_path):
            os.remove(progress_file_path)
            logger.info("Progress file removed after successful completion")
        
        logger.info(f"Scraping finished. Total listings collected: {total_listings}")

if __name__ == "__main__":
    main()