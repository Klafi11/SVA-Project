# Tankerkönig Diesel Price Aufgabe 3
This project fetches fuel station and price data from Tankerkönig and determines the minimum diesel price available two days before the current date.
It downloads the official CSV datasets, merges station and price data, filters valid diesel prices, and prints the stations offering the cheapest diesel.

## Project structure 
```yaml
Aufgabe_3
├── main.py
├── .python-version 3.12
├── pyproject.toml
├── uv.lock
├── .env           
├── .gitignore
└── README.md
```

## What the script does
- Uses Tankerkönig’s official CSV data
- Always looks at today − 2 days (vorgestern)
- Downloads:
- station metadata (name, brand, city, postcode)
- prices data (timestamp, fuel_price, price_changes)
- find lowest Diesel price
- Merges both datasets
- Filters invalid prices (diesel > 0)
- Outputs the station(s) with the lowest diesel price

## Requirements
- Python 3.12+
- uv
- A Tankerkönig account (username & password)

## Setup

### 1. Clone the repository
```bash
git clone <your-repo-url>
cd <your-repo-name>
```

### 2. Create a .env file
The script expects credentials via environment variables.
Create a file named .env in the project root:

```env
USERNAME=your_tankerkoenig_username
PASSWORD=your_tankerkoenig_password
```

### 3. Install dependencies (with uv) & Run the project
```bash
uv sync
uv run main.py
```
