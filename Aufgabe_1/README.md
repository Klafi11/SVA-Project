# Tankerkönig Southernmost Station Analysis Aufgabe 1
This project fetches historical fuel station data from **Tankerkönig** and determines the **southernmost petrol station** based on latitude.
It downloads the official daily CSV datasets for the **last 30 days**, merges them into a single dataset, cleans invalid coordinates, and identifies the stations with the lowest (most southern) latitude values.

## Project structure
```yaml
Aufgabe_1
├── main.py
├── .python-version 3.12
├── pyproject.toml
├── uv.lock
├── .env
├── .gitignore
└── README.md
```

## what the script does 

- Uses Tankerkönig’s official CSV station data
- Fetches station datasets for the last 30 days
- Downloads data concurrently using a thread pool
- Merges all daily CSV files into one large DataFrame
- Cleans latitude and longitude values:
- Determines the southernmost petrol stations by latitude
- Reports any errors encountered during data fetching

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
