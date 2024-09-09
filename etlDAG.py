import csv
import json
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'weather_etl_dag',
    default_args=default_args,
    description='ETL DAG to fetch weather data and save it as a CSV file',
    schedule_interval=timedelta(days=1),  # Runs daily
    start_date=days_ago(1),
    tags=['weather', 'etl'],
)

# 1. Extract: Function to get weather data from OpenWeather API
def extract_weather_data():
    api_key = 'your_api_key_here'  # Replace with your actual API key from OpenWeather
    city = 'London'
    url = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}'

    logging.info(f"Fetching weather data for {city}")
    
    response = requests.get(url)
    data = response.json()

    # Save raw JSON data
    with open('/tmp/weather_data_raw.json', 'w') as f:
        json.dump(data, f)

    logging.info("Weather data extracted successfully")

# 2. Transform: Function to transform data into a CSV-friendly format
def transform_weather_data():
    logging.info("Transforming weather data...")

    # Load the raw JSON data
    with open('/tmp/weather_data_raw.json', 'r') as f:
        data = json.load(f)
    
    # Extract key fields for transformation
    weather_info = {
        'city': data['name'],
        'temperature': data['main']['temp'],
        'humidity': data['main']['humidity'],
        'pressure': data['main']['pressure'],
        'weather': data['weather'][0]['description']
    }

    # Save the transformed data as CSV
    with open('/tmp/transformed_weather_data.csv', 'w', newline='') as csvfile:
        fieldnames = ['city', 'temperature', 'humidity', 'pressure', 'weather']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(weather_info)

    logging.info("Weather data transformed and saved as CSV")

# 3. Load: Function to "load" data (for example, log the final CSV)
def load_weather_data():
    logging.info("Loading transformed weather data...")
    
    # Just log the CSV content (in a real scenario, this could be uploading to a DB, etc.)
    with open('/tmp/transformed_weather_data.csv', 'r') as f:
        logging.info(f.read())

# Define the tasks
t1 = PythonOperator(
    task_id='extract',
    python_callable=extract_weather_data,
    dag=dag,
)

t2 = PythonOperator(
    task_id='transform',
    python_callable=transform_weather_data,
    dag=dag,
)

t3 = PythonOperator(
    task_id='load',
    python_callable=load_weather_data,
    dag=dag,
)

# Set up task dependencies (ETL flow)
t1 >> t2 >> t3
