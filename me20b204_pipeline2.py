from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
import apache_beam as beam

# Define default arguments for your DAG
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

dag = DAG(
    'ncei_data_analytics_pipeline_stable_me20b204',
    default_args=default_args,
    description='Pipeline for data analytics using Apache Beam',
    schedule_interval=timedelta(days=1), 
    catchup=False,
)

# Task 1: Wait for the archive to be available
wait_for_archive = FileSensor(
    task_id='wait_for_archive',
    fs_conn_id='fs_default',
    filepath='/tmp/me20b204_cs5830_a2_data_files.zip',
    timeout=5,
    poke_interval=1,
    soft_fail=True,
    dag=dag,
)

# Task 2: Check if the file is a valid archive and unzip it
unzip_file = BashOperator(
    task_id='unzip_file',
    bash_command='unzip -o /tmp/me20b204_cs5830_a2_data_files.zip -d /tmp/me20b204_cs5830_a2_data_files_unzipped',
    dag=dag,
)

from apache_beam.options.pipeline_options import PipelineOptions
import os
import geopandas as gpd

# Define Apache Beam pipeline for extracting and filtering data
def beam_filter_data():
    input_directory = '/tmp/me20b204_cs5830_a2_data_files_unzipped'
    output_directory = '/tmp/me20b204_cs5830_a2_data_files_filtered'
    os.makedirs(output_directory, exist_ok=True)

    # Define the Apache Beam pipeline options
    options = PipelineOptions(
        runner='DirectRunner',
        temp_location=os.path.join(output_directory, 'temp'),
        save_main_session=True
    )

    # Function to parse each line of the CSV into the desired format
    def parse_csv(line):
        fields = line.split(',')
        # Extract the fields, stripping whitespace and removing quotes if present
        date_time = fields[1].strip('"').strip()  # Extract and clean up date time
        latitude = fields[2].strip('"').strip()
        longitude = fields[3].strip('"').strip()
        windspeed = fields[23].strip('"').strip()  # Update with the correct index if necessary
        bulb_temperature = fields[10].strip('"').strip()  # Update with the correct index if necessary
        # Return the structured data, including date_time
        return [(latitude, longitude, date_time, windspeed, bulb_temperature)]

    # Define the pipeline
    with beam.Pipeline(options=options) as p:
        (
            p
            | 'ReadFiles' >> beam.io.ReadFromText(os.path.join(input_directory, '*.csv'), skip_header_lines=1)
            | 'ParseAndFilter' >> beam.FlatMap(parse_csv)
            | 'FormatForCSV' >> beam.Map(lambda fields: ','.join(fields))  # Format the fields as CSV
            | 'WriteToCSV' >> beam.io.WriteToText(os.path.join(output_directory, 'filtered'), file_name_suffix='.csv', header='LATITUDE,LONGITUDE,DATE_TIME,WINDSPEED,BULB_TEMPERATURE')
        )

# Define the task for Apache Airflow to filter data
filter_data_task = PythonOperator(
    task_id='filter_data',
    python_callable=beam_filter_data,
    dag=dag,
)


# Define Apache Beam pipeline for computing monthly averages
def beam_compute_averages():
    input_directory = '/tmp/me20b204_cs5830_a2_data_files_filtered'
    output_directory = '/tmp/me20b204_cs5830_a2_data_files_filtered_averages'
    os.makedirs(output_directory, exist_ok=True)  # Ensure output directory exists

    # Define the Apache Beam pipeline options
    options = PipelineOptions(
        runner='DirectRunner',
        temp_location=os.path.join(output_directory, 'temp'),
        save_main_session=True
    )

    # Function to extract year-month, latitude, longitude and map data accordingly
    def to_monthly_record(line):
        fields = line.split(',')
        try:
            # Update field indices according to the new data format
            latitude, longitude = fields[0], fields[1]
            date_time = fields[2]  # New date_time field
            windspeed = float(fields[3]) if fields[3] != '' else 0.0  # Update for windspeed
            dry_bulb_temperature = float(fields[4]) if fields[4] != '' else 0.0  # Update for temperature
            year_month = datetime.strptime(date_time.split('T')[0], '%Y-%m-%d').strftime('%Y-%m')  # Extract year-month
            # Return formatted record
            return [(f"{latitude},{longitude},{year_month}", [windspeed, dry_bulb_temperature])]
        except (ValueError, IndexError):
            return []

    # Define the pipeline
    with beam.Pipeline(options=options) as p:
        (
            p
            | 'ReadFilteredFiles' >> beam.io.ReadFromText(os.path.join(input_directory, '*.csv'), skip_header_lines=1)
            | 'MapToMonthlyRecord' >> beam.FlatMap(to_monthly_record)
            | 'GroupByLocationAndMonth' >> beam.GroupByKey()
            | 'ComputeAverages' >> beam.Map(lambda x: (x[0], [sum(values) / len(values) for values in zip(*x[1])]))
            | 'FormatForOutput' >> beam.Map(lambda x: f"{x[0]},{','.join(map(str, x[1]))}")
            | 'WriteAveragesToCSV' >> beam.io.WriteToText(os.path.join(output_directory, 'monthly_averages'), file_name_suffix='.csv', header='LATITUDE,LONGITUDE,YEAR_MONTH,WINDSPEED_AVG,DRYBULB_TEMPERATURE_AVG')
        )


# Define the task for Apache Beam to compute monthly and location-wise averages
compute_averages_task = PythonOperator(
    task_id='compute_averages',
    python_callable=beam_compute_averages,
    dag=dag,
)

import os
import pandas as pd
import geopandas as gpd

import matplotlib
matplotlib.use('Agg') # Anti-Grain Geometry engine for rendering

import matplotlib.pyplot as plt
from airflow.operators.python import PythonOperator
from datetime import datetime
import cartopy.crs as ccrs
import cartopy.feature

def visualize_data():
    input_directory = '/tmp/me20b204_cs5830_a2_data_files_filtered_averages'
    output_directory = '/tmp/me20b204_cs5830_a2_heatmaps'
    os.makedirs(output_directory, exist_ok=True)

    options = PipelineOptions(
        runner='DirectRunner',
        temp_location=os.path.join(output_directory, 'temp'),
        save_main_session=True
    )

     # Function to read data, convert to GeoDataFrame, and create heatmap for each month
    def create_heatmap(element):
        file_path, df = element
        print(f"Starting to process file: {file_path}")
        print("Data read successfully:")
        print(df.head())

        unique_months = df['YEAR_MONTH'].unique()
        print(f"Unique months in the data: {unique_months}")

        for month in unique_months:
            print(f"Processing data for month: {month}")
            month_df = df[df['YEAR_MONTH'] == month]
            gdf = gpd.GeoDataFrame(month_df, geometry=gpd.points_from_xy(month_df['LONGITUDE'], month_df['LATITUDE']))
            print(gdf.head())

            for field in ['WINDSPEED_AVG', 'DRYBULB_TEMPERATURE_AVG']:
                try:
                    print(f"Creating heatmap for {field} in {month}")
                    fig, ax = plt.subplots(figsize=(10, 6))
                    gdf.plot(column=field, ax=ax, legend=True, cmap='coolwarm',
                             legend_kwds={'label': f'{field} by Location', 'orientation': 'horizontal'})
                    ax.set_xlabel('Longitude')
                    ax.set_ylabel('Latitude')
                    ax.set_title(f'{field} Heatmap for {month}')
                    output_path = os.path.join(output_directory, f'{field}_heatmap_{month}.png')
                    plt.savefig(output_path)
                    plt.close(fig)
                    print(f"Successfully created heatmap for {field} in {month}")
                except Exception as e:
                    print(f"Error creating heatmap for {field} in {month}: {e}")

    def read_csv(file_path):
        df = pd.read_csv(file_path)
        return file_path, df

    with beam.Pipeline(options=options) as p:
        (p 
            | 'Create File Paths' >> beam.Create([os.path.join(input_directory, filename) for filename in os.listdir(input_directory) if filename.endswith('.csv')])
            | 'Read CSV Files' >> beam.Map(read_csv)
            | 'Create Heatmaps' >> beam.Map(create_heatmap)
        )

# Define the task for Apache Airflow to visualize data
visualize_data_task = PythonOperator(
    task_id='visualize_data',
    python_callable=visualize_data,
    dag=dag,
)

# Task 6: Create a GIF animation from the PNG images (optional)
create_gif = BashOperator(
    task_id='create_gif',
    bash_command=(
        'mkdir -p /tmp/me20b204_a2_animations; '  # This ensures the animations directory exists
        'convert -delay 50 -loop 0 /tmp/me20b204_cs5830_a2_heatmaps/WINDSPEED_AVG_heatmap_*.png '
        '/tmp/me20b204_a2_animations/windspeed_heatmap_animation.gif; '
        'convert -delay 50 -loop 0 /tmp/me20b204_cs5830_a2_heatmaps/DRYBULB_TEMPERATURE_AVG_heatmap_*.png '
        '/tmp/me20b204_a2_animations/drybulb_heatmap_animation.gif'
    ),
    dag=dag,
)

# Task 7: Delete CSV files from the destined location
cleanup_files = BashOperator(
    task_id='cleanup_files',
    bash_command='rm -rf /tmp/me20b204_a2 /tmp/me20b204_cs5830_a2_data_files.zip /tmp/me20b204_cs5830_a2_data_files_unzipped /tmp/me20b204_cs5830_a2_data_files_filtered /tmp/me20b204_cs5830_a2_data_files_filtered_averages',
    dag=dag,
)


# Set task dependencies
wait_for_archive >> unzip_file >> filter_data_task >> compute_averages_task >> visualize_data_task >> create_gif >> cleanup_files
