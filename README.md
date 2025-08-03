# Buenos Aires Properties Web Scraper

A Python web scraping project for collecting rental property data from Buenos Aires real estate websites.

## Features

- Scrapes property listings from ArgentProp and ZonaProp
- Cleans and processes data with price conversions
- Filters properties based on size, price, and date criteria
- Sends Telegram alerts for new listings
- Automated daily execution via cron jobs

## Setup

1. **Clone the repository**:
   ```bash
   git clone git@github.com:alephdao/buenosaires_properties.git
   cd buenosaires_properties
   ```

2. **Create virtual environment**:
   ```bash
   python3 -m venv myenv
   source myenv/bin/activate  # On Windows: myenv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables**:
   ```bash
   cp .env.example .env
   # Edit .env with your actual Telegram bot credentials
   ```

## Usage

### Individual Scripts

- **ArgentProp scraper**: `python argenprop/argenprop.py`
- **ZonaProp scraper**: `python zonaprop/zonaprop.py`
- **Data cleaning**: `python cleaning.py`
- **Send alerts**: `python alerts.py`

### Run All Scripts

Execute the complete pipeline:
```bash
python run_all_programs.py
```

## Configuration

### Filtering Criteria (in `cleaning.py`)

- **Size**: ≥ 90m² (or any size if not specified)
- **Price**: USD $300-1500 total (including expenses)
- **Date**: Current day only
- **Currency conversion**: Uses `dollarblue` rate for ARS to USD

### Telegram Setup

1. Create a Telegram bot via [@BotFather](https://t.me/botfather)
2. Get your chat ID by messaging [@userinfobot](https://t.me/userinfobot)
3. Add credentials to `.env` file

## Files Structure

```
├── argenprop/
│   ├── argenprop.py          # ArgentProp scraper
│   └── argenprop_listings.csv # Raw data output
├── zonaprop/
│   ├── zonaprop.py           # ZonaProp scraper
│   └── zonaprop_listings.csv # Raw data output
├── alerts.py                 # Telegram notification system
├── cleaning.py               # Data processing and filtering
├── run_all_programs.py       # Execute complete pipeline
├── listings_clean.csv        # Final filtered results
├── requirements.txt          # Python dependencies
└── cron.txt                  # Cron job configuration
```

## Automation

Set up daily execution with cron:
```bash
# Add to crontab (crontab -e)
0 9 * * * /path/to/your/project/run_all_programs.py >> /path/to/your/project/cron.log 2>&1
```

## Dependencies

- `requests-html`: Web scraping with JavaScript support
- `beautifulsoup4`: HTML parsing
- `selenium`: Browser automation for ZonaProp
- `undetected-chromedriver`: Bypass anti-bot measures
- `pandas`: Data processing
- `python-telegram-bot`: Telegram integration

## Notes

- Update `dollarblue` rate in `cleaning.py` for current USD/ARS conversion
- Adjust filtering criteria as needed for your preferences
- Ensure Chrome browser is installed for Selenium-based scraping
