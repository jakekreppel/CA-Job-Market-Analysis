import requests
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
import time
import matplotlib.pyplot as plt
import seaborn as sns

# --- 1. CONFIGURATION ---
BASE_URL = "https://api.adzuna.com/v1/api/jobs/us/search/1"

# API Parameters
params = {
    "app_id": "76b10b3e",
    "app_key": "5117f9dccd74e73a20b833e8e232b0c4",
    "results_per_page": 50,
    "what": "data analyst",
    "where": "california",
    "content-type": "application/json"
}

# PostgreSQL Database Details
DB_USER = "postgres"
DB_PASSWORD = "analytics123"  
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "postgres"          # Using the default 'postgres' database for now
TABLE_NAME = "job_listings_ca"

# Connection string for PostgreSQL
DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- 2. CORE FUNCTIONS ---

def fetch_jobs_api(url, params):
    """Extracts job data from the Adzuna API."""
    print("Step 1: Fetching data from API...")
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json().get('results', [])
        extracted_data = []
        for job in data:
            extracted_data.append({
                'Title': job.get('title'),
                'Location': job.get('location', {}).get('display_name'),
                'Salary': job.get('salary_max'),
                'Description': job.get('description')
            })
        return extracted_data
    else:
        print(f"API Error: {response.status_code}")
        return []

def clean_data(df):
    """Transforms raw data into a clean format."""
    print("Step 2: Cleaning and transforming data...")
    # Clean Salary
    df['Salary'] = pd.to_numeric(df['Salary'], errors='coerce').fillna(0)
    # Parse City from Location
    df['City'] = df['Location'].str.split(',').str[0].str.strip()
    # Filter out zero-salary jobs
    return df[df['Salary'] > 0].copy()

def load_to_postgres(df, db_url, table_name):
    """Loads the cleaned DataFrame into PostgreSQL."""
    print(f"Step 3: Loading data into PostgreSQL table '{table_name}'...")
    try:
        engine = create_engine('postgresql://postgres:analytics123@127.0.0.1:5432/postgres')
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print("Success! Data is now in pgAdmin.")
        plt.figure(figsize=(10, 6))
        # Get top 10 cities by average salary
        top_cities = df.groupby('City')['Salary'].mean().sort_values(ascending=False).head(10)
        
        sns.barplot(x=top_cities.values, y=top_cities.index, palette='viridis')
        
        plt.title('Top 10 Highest Paying Cities for Data Analysts (CA)')
        plt.xlabel('Average Salary ($)')
        plt.ylabel('City')
        plt.tight_layout()
        
        # Save the chart as a PNG file 
        plt.savefig('salary_analysis_chart.png')
        print("Chart generated: 'salary_analysis_chart.png' saved to folder.")
        return True
    except Exception as e:
        print(f"Load Error: {e}")
        return False

# --- 3. EXECUTION ---

if __name__ == "__main__":
    raw_list = fetch_jobs_api(BASE_URL, params)
    df_raw = pd.DataFrame(raw_list)
    
    if not df_raw.empty:
        df_clean = clean_data(df_raw)
        
        load_to_postgres(df_clean, DB_URL, TABLE_NAME)
        
        print("\n--- Summary ---")
        print(df_clean[['Title', 'City', 'Salary']].head())
    else:
        print("No jobs found to process.")

