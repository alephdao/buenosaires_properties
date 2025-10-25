"""
This code scrapes zonaprop. It works even though zonaprop requires
using selenium to create a browser to get the actual html and
even though selenium also has cloudflare implemented. 

to update this just change the initial url and it will pull every page from there.

double check how many pages it has before running. 

"""


from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import undetected_chromedriver as uc
# from selenium import webdriver
#from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import os
import csv
import random
import time
from requests_html import HTMLSession


base_url = 'https://www.zonaprop.com.ar'
url = base_url + '/departamentos-alquiler-capital-federal-mas-de-3-habitaciones-a-estrenar.html'

headers = ['website', 'price', 'expenses', 'address', 'size', 'bedrooms', 'bathrooms', 'url', 'listing_url']
csv_file_path = "zonaprop_listings.csv"  

def setup_driver():
    options = Options()
    # options.add_experimental_option("detach", True)
    driver = uc.Chrome()
    # driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    return driver

def get_page_source(driver, url):
    try:
        driver.get(url)
        time.sleep(1) 
        return driver.page_source
    except Exception as e:
        print(f"An error occurred while fetching the page: {e}")
        return None

def parse_page(page_source):
    if page_source:
        return BeautifulSoup(page_source, 'html.parser')
    return None


def getnextpage(soup):
    siguiente_tag = soup.find('a', {'data-qa': 'PAGING_NEXT'})
    if siguiente_tag and 'href' in siguiente_tag.attrs:
        return base_url + siguiente_tag['href']
    return None

driver = setup_driver()
with open(csv_file_path, 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(headers)

while True:
# for i in range(1, 3):
   
    apartment_listings = []

    page_source = get_page_source(driver, url)
    data = parse_page(page_source)

    print("Fetching data from URL:", url)
    
    # data = getdata(url)

    listings = data.find_all('div', attrs={'data-qa': 'posting PROPERTY'})

    for listing in listings:
        listing_data = []
        
        # Append data to listing_data list instead of a dictionary
        listing_data.append(base_url)

        price_div = listing.find('div', attrs={"data-qa": "POSTING_CARD_PRICE"})
        listing_data.append(price_div.text.strip() if price_div else "Consultar Precio")

        expenses_div = listing.find('div', attrs={"data-qa": "expensas"})
        listing_data.append(expenses_div.text.replace('Expensas', '').strip() if expenses_div else "")

        address_div = listing.find('div', class_="sc-ge2uzh-0 eXwAuU")
        listing_data.append(address_div.text.strip() if address_div else "not found")

        size_span = [span.get_text(strip=True).split()[0] for span in listing.find_all('span') if 'm²' in span.get_text()]
        listing_data.append(size_span[0].strip() if size_span else "not found")

        bedrooms_span = [span.get_text(strip=True).split()[0] for span in listing.find_all('span') if 'dorm.' in span.get_text()]
        listing_data.append(bedrooms_span[0].strip() if bedrooms_span else "not found")

        bathrooms_span = [span.get_text(strip=True).split()[0] for span in listing.find_all('span') if 'baños' in span.get_text()]
        listing_data.append(bathrooms_span[0].strip() if bathrooms_span else "not found")

        listing_data.append(url)

        # Append the complete URL to the listing_data list
        listing_data.append("https://www.zonaprop.com.ar" + listing.get('data-to-posting', '') if listing else "")

        # Append the listing_data list to the apartment_listings list
        apartment_listings.append(listing_data)

    with open(csv_file_path, 'a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        for apartment_listing in apartment_listings:
            writer.writerow(apartment_listing)

    print(f"Data successfully written to {csv_file_path}")

    sleep_duration = random.uniform(.5, 1)
    time.sleep(sleep_duration)

    next_page_url = getnextpage(data)
    if not next_page_url:
        break
    url = next_page_url

driver.quit()
