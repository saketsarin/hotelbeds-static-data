# Hotelbeds Data Fetcher

## Overview

This Python script is designed to fetch and update static data from the Hotelbeds API. It retrieves information about various travel-related entities such as countries, destinations, room types, and more. The script stores this data in a PostgreSQL database, allowing for efficient management and updates of the information.

## Features

- Fetches data from multiple Hotelbeds API endpoints
- Stores data in a PostgreSQL database
- Handles incremental updates based on last update time
- Implements rate limiting to respect API constraints
- Provides detailed logging for monitoring and debugging

## Prerequisites

- Python 3.6+
- PostgreSQL database
- Hotelbeds API credentials

## Installation

1. Install PostgreSQL:
   - For Ubuntu:
     ```
     sudo apt update
     sudo apt install postgresql postgresql-contrib
     ```
   - For macOS (using Homebrew):
     ```
     brew install postgresql
     ```
   - For Windows, download and install from the [official PostgreSQL website](https://www.postgresql.org/download/windows/).

2. Start the PostgreSQL service:
   - For Ubuntu:
     ```
     sudo systemctl start postgresql
     sudo systemctl enable postgresql
     ```
   - For macOS:
     ```
     brew services start postgresql
     ```
   - For Windows, the service should start automatically after installation.

3. Create a new PostgreSQL database:
   ```
   sudo -u postgres psql
   CREATE DATABASE <your-database-name>;
   \q
   ```

4. Clone the repository or download the script.

5. Create a virtual environment:
   ```
   python3 -m venv venv
   ```

6. Activate the virtual environment:
   ```
   source venv/bin/activate
   ```

7. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

## Configuration

1. Create a `.env` file in the same directory as the script with the following content:

   ```
   API_KEY=your_api_key
   SECRET=your_secret
   
   DB_URL=postgresql://username:password@host:port/<your-database-name>
   ```

2. Replace the placeholder values with your actual Hotelbeds API credentials and database connection details.

## Usage

To run the script, use the following command:

```
python get_static_data.py
```

The script will:
1. Connect to the Hotelbeds API
2. Fetch data from various endpoints
3. Store or update the data in the PostgreSQL database
4. Log the progress and any errors encountered

## Data Tables

The script creates and updates the following tables in the database:

- hotelbeds_countries
- hotelbeds_destinations
- hotelbeds_rooms
- hotelbeds_boards
- hotelbeds_boardgroups
- hotelbeds_accommodations
- hotelbeds_categories
- hotelbeds_chains
- hotelbeds_classifications
- hotelbeds_facilities
- hotelbeds_facilitygroups
- hotelbeds_facilitytypologies
- hotelbeds_groupcategories
- hotelbeds_issues
- hotelbeds_languages
- hotelbeds_promotions
- hotelbeds_segments
- hotelbeds_imagetypes
- hotelbeds_currencies
- hotelbeds_terminals
- hotelbeds_ratecomments
- hotelbeds_hotels

Additionally, it maintains a `hotelbeds_last_updated_time` table to track the last update time for each entity type.