#!/bin/bash
# run_gui.sh
# Helper script to launch Costco Scraper GUI using the configured virtual environment.

cd "$(dirname "$0")"

if [ ! -d "venv" ]; then
    echo "Virtual environment not found. Please run the setup steps."
    exit 1
fi

echo "Launching Costco GUI..."
./venv/bin/python costco_gui.py
