import pandas as pd
# from geopy.geocoders import Nominatim
# from geopy.exc import GeocoderTimedOut, GeocoderServiceError
from datetime import datetime
import logging
import time

dollarblue = 1200  #PJG update to the current rate. 

output_file_path = '/Users/philipgalebach/coding-projects/buenosaires_properties/listings_clean.csv'

df = pd.read_csv('/Users/philipgalebach/coding-projects/buenosaires_properties/argenprop/argenprop_listings.csv')

df['address'] = df['address'].str.split(',', n=1).str[0] + ', CABA, Argentina'
df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

# keep first stance of each address
df.sort_values('timestamp', inplace=True)
df.drop_duplicates('address', keep='first', inplace=True)

df['price_total_usd'] = df.apply(lambda row: (row['price'] / dollarblue + row['expenses'] / dollarblue) if row['currency'] == '$' 
                             else (row['price'] + row['expenses']/ dollarblue), axis=1)

filtered_df = df[
    (df['size'].isna() | (df['size'] >= 90))  &
    (df['price_total_usd'] <= 1500) & 
    (df['price_total_usd'] >= 300) & 
    df['price_total_usd'].notna() & 
    (df['timestamp'].dt.date == datetime.now().date())
]

print(f"Number of rows with today's date: {len(df[df['timestamp'].dt.date == datetime.now().date()])}")
print(f"Number of rows that passed all the filters: {len(filtered_df)}")

filtered_df.to_csv(output_file_path, index=False)

print(f"File saved as {output_file_path}")


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