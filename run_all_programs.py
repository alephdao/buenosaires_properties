#!/Users/philip.galebach/coding-projects/webscraping/buenosaires_properties/myenv/bin/python3

import subprocess
import os

def run_all_programs():
    # Run argenprop.py
    argenprop_program_path = '/Users/philip.galebach/coding-projects/webscraping/buenosaires_properties/argenprop/argenprop.py'
    subprocess.run(["python", argenprop_program_path], check=True)

    cleaning_program_path = '/Users/philip.galebach/coding-projects/webscraping/buenosaires_properties/cleaning.py'
    subprocess.run(["python", cleaning_program_path], check=True)

    alerts_program_path = '/Users/philip.galebach/coding-projects/webscraping/buenosaires_properties/alerts.py'
    subprocess.run(["python", alerts_program_path], check=True)

# Run all the programs
run_all_programs()