#!/usr/bin/env python3
"""Quick map from cached data"""
import sys
sys.path.insert(0, '/Users/philipgalebach/coding-projects/buenosaires_properties/analysis')
from map_properties import main

# Run with cached data only
output = main(bedrooms=2)
print(f"\n✅ Map created with available data!")
