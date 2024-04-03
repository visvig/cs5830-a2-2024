from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import re
import random
import zipfile
import os

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 28), 
    'email': ['v.vishal1102@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
dag = DAG(
    'ncei_datafetch_pipeline_stable_me20b204',
    default_args=default_args,
    description='Pipeline to fetch, select, and archive NCEI data files',
    schedule_interval=timedelta(days=1), 
    catchup=False,
)

# Task to fetch the page containing dataset links
fetch_page_task = BashOperator(
    task_id='fetch_page',
    bash_command=(
        'mkdir -p /tmp/me20b204_a2; '  # Create a directory to store temporary files
        'curl -o /tmp/me20b204_a2/dataset_page.html https://www.ncei.noaa.gov/data/local-climatological-data/access/2023/'  # Fetch the dataset page and save it as HTML file
    ),
    dag=dag,
)

def select_files(**kwargs):
    base_url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2023/"
    with open('/tmp/me20b204_a2/dataset_page.html', 'r') as file:
        content = file.read()
    links = re.findall(r'href="(.*?\.csv)"', content)  # Extract links to CSV files from the HTML content
    absolute_links = [base_url + link for link in links]  # Create absolute links by appending base URL
    selected_links = random.sample(absolute_links, min(len(absolute_links), 20))  # Select a random subset of links (maximum 20)
    
    # Save selected links to a text file
    with open('/tmp/me20b204_a2/selected_links.txt', 'w') as file:
        for link in selected_links:
            file.write(link + '\n')

select_files_task = PythonOperator(
    task_id='select_files',
    python_callable=select_files,
    provide_context=True,
    dag=dag,
)

fetch_files_task = BashOperator(
    task_id='fetch_files',
    bash_command="""
    while IFS='' read -r line || [[ -n "$line" ]]; do
      curl -L "$line" -o "/tmp/me20b204_a2/$(basename $line)"  # Fetch each selected file and save it with the same name
    done < /tmp/me20b204_a2/selected_links.txt
    """,
    dag=dag,
)

# Task to zip the fetched files
def zip_files(**kwargs):
    zip_filename = '/tmp/me20b204_a2/dataset_archive.zip'  # Define the name of the zip file
    with zipfile.ZipFile(zip_filename, 'w') as zipf:
        for root, dirs, files in os.walk('/tmp'):
            for file in files:
                if file.endswith('.csv'):  # Only include CSV files in the zip archive
                    zipf.write(os.path.join(root, file), file)  # Add each CSV file to the zip archive
    return zip_filename

zip_files_task = PythonOperator(
    task_id='zip_files',
    python_callable=zip_files,
    provide_context=True,
    dag=dag,
)

# Task to place the archive in a desired location
place_archive_task = BashOperator(
    task_id='place_archive',
    bash_command='mv /tmp/me20b204_a2/dataset_archive.zip /tmp/me20b204_cs5830_a2_data_files.zip',  # Move the zip archive to a desired location
    dag=dag,
)

# Set task dependencies
fetch_page_task >> select_files_task >> fetch_files_task >> zip_files_task >> place_archive_task
