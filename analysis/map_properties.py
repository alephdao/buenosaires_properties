#!/usr/bin/env python3
"""
Real estate property mapper for Buenos Aires
Creates an interactive map with properties color-coded by total price
"""

import sqlite3
import pandas as pd
import folium
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import time
import json
import os
from pathlib import Path

# Configuration
DB_PATH = '/Users/philipgalebach/coding-projects/buenosaires_properties/properties.db'
OUTPUT_DIR = '/Users/philipgalebach/coding-projects/buenosaires_properties/analysis'
CACHE_FILE = f'{OUTPUT_DIR}/geocache.json'

def load_geocache():
    """Load geocoding cache"""
    if Path(CACHE_FILE).exists():
        with open(CACHE_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_geocache(cache):
    """Save geocoding cache"""
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=2)

def geocode_addresses(df, use_cache_only=False):
    """Geocode addresses to get latitude/longitude with caching"""
    cache = load_geocache()
    print(f"Loaded {len(cache)} cached addresses")

    if use_cache_only:
        print("Using cached addresses only (skipping new geocoding)")
        locations = []
        for idx, row in df.iterrows():
            address = row['address'].strip()
            if address in cache:
                locations.append(cache[address])
            else:
                locations.append({'latitude': None, 'longitude': None})

        df['latitude'] = [loc['latitude'] for loc in locations]
        df['longitude'] = [loc['longitude'] for loc in locations]
        return df

    print(f"Geocoding {len(df)} addresses...")

    geolocator = Nominatim(user_agent="buenosaires_properties_analyzer", timeout=10)
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1.5)

    locations = []
    cache_updated = False

    for idx, row in df.iterrows():
        address = row['address'].strip()

        # Check cache first
        if address in cache:
            locations.append(cache[address])
            if idx % 50 == 0:
                print(f"✓ {idx+1}/{len(df)}: {address[:40]} (cached)")
            continue

        try:
            # Add "Buenos Aires, Argentina" to improve accuracy
            full_address = f"{address}, Buenos Aires, Argentina"
            location = geocode(full_address)

            if location:
                result = {
                    'latitude': location.latitude,
                    'longitude': location.longitude
                }
                locations.append(result)
                cache[address] = result
                cache_updated = True
                print(f"✓ {idx+1}/{len(df)}: {address[:40]}")
            else:
                result = {'latitude': None, 'longitude': None}
                locations.append(result)
                cache[address] = result
                cache_updated = True
                print(f"✗ {idx+1}/{len(df)}: {address[:40]} - Not found")

            # Save cache every 10 successful geocodes
            if cache_updated and len(cache) % 10 == 0:
                save_geocache(cache)

        except Exception as e:
            print(f"✗ {idx+1}/{len(df)}: {address[:40]} - Error: {str(e)[:50]}")
            result = {'latitude': None, 'longitude': None}
            locations.append(result)
            cache[address] = result
            cache_updated = True

    # Final save
    if cache_updated:
        save_geocache(cache)
        print(f"Saved {len(cache)} addresses to cache")

    df['latitude'] = [loc['latitude'] for loc in locations]
    df['longitude'] = [loc['longitude'] for loc in locations]

    return df

def get_price_color(price, p33, p67):
    """Get color based on price percentile"""
    if price < p33:
        return '#28a745'  # Green (cheap)
    elif price < p67:
        return '#ffc107'  # Yellow/Orange (medium)
    else:
        return '#dc3545'  # Red (expensive)

def format_price(price):
    """Format price for display (e.g., 1500 -> $1.5k, 1500000 -> $1.5m)"""
    if price >= 1_000_000:
        return f"${price/1_000_000:.1f}m"
    elif price >= 1_000:
        return f"${price/1_000:.1f}k"
    else:
        return f"${price:.0f}"

def create_map(df, bedrooms):
    """Create folium map with properties"""
    # Filter out properties without coordinates
    df_mapped = df.dropna(subset=['latitude', 'longitude', 'price_dollars'])
    df_mapped = df_mapped[df_mapped['price_dollars'] > 0]

    print(f"\nMapping {len(df_mapped)} {bedrooms}-bedroom properties")
    print(f"Price range: ${df_mapped['price_dollars'].min():,.0f} - ${df_mapped['price_dollars'].max():,.0f}")

    # Create base map centered on Buenos Aires
    center_lat = df_mapped['latitude'].mean()
    center_lon = df_mapped['longitude'].mean()
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=13,
        tiles='OpenStreetMap'
    )

    # Define color thresholds based on price percentiles
    p33 = df_mapped['price_dollars'].quantile(0.33)
    p67 = df_mapped['price_dollars'].quantile(0.67)

    print(f"Color thresholds: Green < ${p33:,.0f} | Yellow < ${p67:,.0f} | Red >= ${p67:,.0f}")

    # Add markers
    for idx, row in df_mapped.iterrows():
        color = get_price_color(row['price_dollars'], p33, p67)

        # Format popup text with both live and archived links
        links_html = ''
        if pd.notna(row['listing_url']):
            links_html += f'<p style="margin: 5px 0;"><a href="{row["listing_url"]}" target="_blank">🔗 Live Link</a></p>'

        if pd.notna(row.get('archived_path')):
            # Convert relative path to absolute file:// URL
            archived_full_path = os.path.abspath(os.path.join(OUTPUT_DIR, '..', row['archived_path']))
            links_html += f'<p style="margin: 5px 0;"><a href="file://{archived_full_path}" target="_blank">📁 Archived Link</a></p>'

        popup_html = f"""
        <div style="font-family: Arial; min-width: 200px;">
            <h4 style="margin: 0 0 10px 0;">${row['price_dollars']:,.0f}</h4>
            <p style="margin: 5px 0;"><b>{row['address']}</b></p>
            <p style="margin: 5px 0;">{int(row['size']) if pd.notna(row['size']) else 'N/A'} m² cubie.</p>
            <p style="margin: 5px 0;">{int(row['bedrooms'])} dorm.</p>
            {links_html}
        </div>
        """

        # Create marker with custom icon
        folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=folium.Popup(popup_html, max_width=300),
            icon=folium.DivIcon(html=f"""
                <div style="
                    background-color: {color};
                    color: white;
                    padding: 5px 10px;
                    border-radius: 4px;
                    font-weight: bold;
                    font-size: 12px;
                    white-space: nowrap;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.3);
                    font-family: Arial;
                ">
                    {format_price(row['price_dollars'])}
                </div>
            """)
        ).add_to(m)

    # Add legend
    legend_html = f'''
    <div style="position: fixed;
                bottom: 50px; right: 50px; width: 220px; height: 150px;
                background-color: white; border:2px solid grey; z-index:9999;
                font-size:14px; padding: 15px; border-radius: 5px;">
    <p style="margin: 0 0 10px 0;"><b>{bedrooms}-Bedroom Properties</b></p>
    <p style="margin: 5px 0;"><span style="color: #28a745; font-size: 20px;">●</span> Low: < ${p33:,.0f}</p>
    <p style="margin: 5px 0;"><span style="color: #ffc107; font-size: 20px;">●</span> Mid: ${p33:,.0f} - ${p67:,.0f}</p>
    <p style="margin: 5px 0;"><span style="color: #dc3545; font-size: 20px;">●</span> High: > ${p67:,.0f}</p>
    </div>
    '''
    m.get_root().html.add_child(folium.Element(legend_html))

    # Save map
    output_file = f'{OUTPUT_DIR}/buenos_aires_{bedrooms}bed_map.html'
    m.save(output_file)
    print(f"\n✓ Map saved to: {output_file}")

    return df_mapped, output_file

def main(bedrooms=2, use_cache_only=False):
    """Main function to create property map"""
    # Load data from database
    print(f"Loading {bedrooms}-bedroom properties from database...")
    conn = sqlite3.connect(DB_PATH)

    query = """
    SELECT address, price_dollars, size, bedrooms, bathrooms, listing_url, archived_path
    FROM properties
    WHERE bedrooms = ? AND price_dollars IS NOT NULL
    """

    df = pd.read_sql_query(query, conn, params=(bedrooms,))
    conn.close()

    print(f"Found {len(df)} properties")

    if len(df) == 0:
        print(f"No {bedrooms}-bedroom properties found!")
        return

    # Geocode addresses
    df = geocode_addresses(df, use_cache_only=use_cache_only)

    # Create map
    df_mapped, output_file = create_map(df, bedrooms)

    # Print summary statistics
    print("\n=== Summary Statistics ===")
    print(f"Total properties: {len(df)}")
    print(f"Successfully mapped: {len(df_mapped)}")
    print(f"Average price: ${df_mapped['price_dollars'].mean():,.0f}")
    print(f"Median price: ${df_mapped['price_dollars'].median():,.0f}")

    return output_file

if __name__ == "__main__":
    import sys

    # Accept bedroom count as command line argument
    bedrooms = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    use_cache_only = '--cache-only' in sys.argv

    print(f"=== Buenos Aires Property Mapper ===")
    print(f"Analyzing {bedrooms}-bedroom properties\n")

    output = main(bedrooms, use_cache_only=use_cache_only)

    print(f"\n✅ Done! Open the map at: {output}")
