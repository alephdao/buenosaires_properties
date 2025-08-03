#!/usr/local/bin/python3

#PJG: Core notes on code
    #the code often misses bathrooms because those aren't in the home page. doesn't matter imo
    #data is processed in a subsequent cleaning code

import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import re
import csv
import time  
import os
from datetime import datetime

# Define the CSV file path relative to script location
script_dir = os.path.dirname(os.path.abspath(__file__))
csv_file_path = os.path.join(script_dir, 'argenprop_listings.csv')
# if os.path.exists(csv_file_path):
#     os.remove(csv_file_path)

website = 'argenprop'
base_url = 'https://www.argenprop.com'
# url = base_url + '/inmuebles/alquiler/belgrano-o-br-norte-o-palermo/3-dormitorios?'
url = base_url + '/inmuebles/alquiler/belgrano-o-br-norte-o-palermo/3-dormitorios-o-4-dormitorios-o-5-o-mas-dormitorios?pagina-1'

# Define the fieldnames for the CSV
fieldnames = [ 'address','currency', 'price', 'expenses',  'size', 'bedrooms', 'bathrooms', 'listing_url', 'website', 'url',  'description', 'timestamp']

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

def getdata(url):
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)
    time.sleep(1)  # Wait for page to load
    page_source = driver.page_source
    driver.quit()
    soup = BeautifulSoup(page_source, 'html.parser')
    return soup

def getnextpage(soup):
    siguiente_tag = soup.find('a', {'aria-label': 'Siguiente'})
    if siguiente_tag and 'href' in siguiente_tag.attrs:
        return base_url + siguiente_tag['href']
    return None

while True:

    print("Fetching data from URL:", url)
    data = getdata(url)

    apartment_listings = []

    listings = data.find_all('div', class_='listing__item')  # Adjusted class to match your HTML structure

    for listing in listings:
        listing_html = str(listing)
        listing_data = {}

        listing_data['website'] = website 
        # Extracting the listing URL
        link_tag = listing.find('a', href=True)  # Find the <a> tag with an href attribute
        if link_tag and 'href' in link_tag.attrs:
            listing_data['listing_url'] = base_url + link_tag['href']  # Concatenate base URL with the relative URL
        else:
            listing_data['listing_url'] = 'N/A'  # In case there's no link

        for key, pattern in patterns.items():
            match = re.search(pattern, listing_html)
            listing_data[key] = match.group(1).replace('.', '') if match else 'N/A'

        # currency_span is null when consult price
        currency_span = listing.find('span', class_='card__currency')
        price_span = currency_span.find_next_sibling(string=True) if currency_span else None

        if currency_span is not None:
            listing_data['currency'] = currency_span.text.strip()
        else:
            listing_data['currency'] = ''  # Leave blank if currency_span does not exist

        if price_span is not None:
            # Ensure that price_span is a string and strip it before applying regex
            listing_data['price'] = int(re.sub(r'[^\d]+', '', price_span.strip()))
        else:
            listing_data['price'] = ''  # Leave blank if price_span does not exist

        listing_data['url'] = url  # Added 'url' to the data being captured

        listing_data['description'] = listing.find('p', class_='card__info').text.strip() if listing.find('p', class_='card__info') else None

        listing_data['timestamp'] = datetime.now().isoformat()

        apartment_listings.append(listing_data)

    with open(csv_file_path, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        if file.tell() == 0:
            writer.writeheader()
        for listing in apartment_listings:
            writer.writerow(listing)
    print("Data written to CSV")
    sleep_duration = random.uniform(0.1, 1)
    time.sleep(sleep_duration)

    next_page_url = getnextpage(data)
    if not next_page_url:
        break
    url = next_page_url
