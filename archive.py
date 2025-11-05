#!/usr/bin/env python3
"""
Archive module for Buenos Aires Properties scraper.
Downloads and saves full HTML pages with all assets for offline viewing.
"""

import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from pathlib import Path
from datetime import datetime
import logging
import re

logger = logging.getLogger(__name__)

# Base directory for archives
ARCHIVE_BASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'buckets', 'links')


def sanitize_filename(name):
    """Sanitize a string to be used as a filename."""
    # Remove or replace invalid characters
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    # Remove leading/trailing spaces and dots
    name = name.strip('. ')
    # Limit length
    return name[:100]


def download_asset(url, session):
    """Download an asset (image, CSS, JS) and return its content."""
    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
        return response.content
    except Exception as e:
        logger.warning(f"Failed to download asset {url}: {e}")
        return None


def archive_property_page(property_id, address, listing_url, date_scraped=None):
    """
    Archive a property listing page with all its assets.

    Args:
        property_id: Database ID of the property
        address: Property address
        listing_url: URL of the listing to archive
        date_scraped: Date the property was scraped (defaults to today)

    Returns:
        str: Relative path to archived HTML file, or None if archiving failed
    """
    if not listing_url:
        logger.warning(f"No listing URL provided for property {property_id}")
        return None

    # Use provided date or default to today
    if date_scraped is None:
        date_scraped = datetime.now()
    elif isinstance(date_scraped, str):
        date_scraped = datetime.fromisoformat(date_scraped)

    date_str = date_scraped.strftime('%Y%m%d')

    # Create archive directory name
    safe_address = sanitize_filename(address)
    archive_dir_name = f"{property_id}_{safe_address}_{date_str}"
    archive_dir = os.path.join(ARCHIVE_BASE, archive_dir_name)

    # Create directory if it doesn't exist
    os.makedirs(archive_dir, exist_ok=True)

    logger.info(f"Archiving property {property_id} ({address}) to {archive_dir}")

    try:
        # Create a session for efficient multiple requests
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

        # Download main HTML page
        response = session.get(listing_url, timeout=15)
        response.raise_for_status()
        html_content = response.text

        # Parse HTML
        soup = BeautifulSoup(html_content, 'html.parser')

        # Create assets directory
        assets_dir = os.path.join(archive_dir, 'assets')
        os.makedirs(assets_dir, exist_ok=True)

        # Download and update image sources
        for img in soup.find_all('img'):
            src = img.get('src') or img.get('data-src')
            if src:
                img_url = urljoin(listing_url, src)
                img_filename = sanitize_filename(os.path.basename(urlparse(img_url).path))

                # Ensure filename has an extension
                if '.' not in img_filename:
                    img_filename += '.jpg'

                img_path = os.path.join(assets_dir, img_filename)

                # Download image
                img_content = download_asset(img_url, session)
                if img_content:
                    with open(img_path, 'wb') as f:
                        f.write(img_content)
                    # Update src to relative path
                    img['src'] = f'assets/{img_filename}'
                    if img.get('data-src'):
                        img['data-src'] = f'assets/{img_filename}'

        # Download and update CSS files
        for link in soup.find_all('link', rel='stylesheet'):
            href = link.get('href')
            if href:
                css_url = urljoin(listing_url, href)
                css_filename = sanitize_filename(os.path.basename(urlparse(css_url).path))

                if not css_filename.endswith('.css'):
                    css_filename += '.css'

                css_path = os.path.join(assets_dir, css_filename)

                css_content = download_asset(css_url, session)
                if css_content:
                    with open(css_path, 'wb') as f:
                        f.write(css_content)
                    link['href'] = f'assets/{css_filename}'

        # Add metadata comment at the top
        metadata_comment = soup.new_tag('comment')
        metadata_comment.string = f"""
        Archived Property Listing
        Property ID: {property_id}
        Address: {address}
        Original URL: {listing_url}
        Archived Date: {date_scraped.strftime('%Y-%m-%d %H:%M:%S')}
        """
        if soup.head:
            soup.head.insert(0, metadata_comment)

        # Save modified HTML
        html_filename = 'index.html'
        html_path = os.path.join(archive_dir, html_filename)

        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(str(soup))

        # Return relative path from project root
        relative_path = os.path.relpath(html_path, os.path.dirname(ARCHIVE_BASE))
        logger.info(f"✓ Successfully archived to {relative_path}")

        return relative_path

    except Exception as e:
        logger.error(f"Failed to archive property {property_id}: {e}")
        return None


def get_archive_path(property_id, address, date_scraped):
    """
    Get the expected archive path for a property without actually archiving it.
    Useful for checking if an archive already exists.
    """
    if isinstance(date_scraped, str):
        date_scraped = datetime.fromisoformat(date_scraped)

    date_str = date_scraped.strftime('%Y%m%d')
    safe_address = sanitize_filename(address)
    archive_dir_name = f"{property_id}_{safe_address}_{date_str}"

    html_path = os.path.join(ARCHIVE_BASE, archive_dir_name, 'index.html')

    if os.path.exists(html_path):
        return os.path.relpath(html_path, os.path.dirname(ARCHIVE_BASE))

    return None


if __name__ == '__main__':
    # Test archiving with a sample property
    logging.basicConfig(level=logging.INFO)

    test_url = "https://www.argenprop.com/departamento-en-alquiler-en-barrio-norte-3-ambientes--18357476"
    result = archive_property_page(
        property_id=999,
        address="Aguero al 1700",
        listing_url=test_url
    )

    if result:
        print(f"\n✅ Test archive created at: {result}")
    else:
        print("\n❌ Test archive failed")
