#!/usr/bin/env python3

import subprocess
import os

def run_all_programs():
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Run argenprop_improved.py (with better error handling and resume capability)
    argenprop_program_path = os.path.join(script_dir, 'argenprop', 'argenprop_improved.py')
    subprocess.run(["python", argenprop_program_path], check=True)

    cleaning_program_path = os.path.join(script_dir, 'cleaning.py')
    subprocess.run(["python", cleaning_program_path], check=True)

    alerts_program_path = os.path.join(script_dir, 'alerts.py')
    subprocess.run(["python", alerts_program_path], check=True)

# Run all the programs
run_all_programs()