from google.cloud import bigquery
import os

# Set up the BigQuery client
client = bigquery.Client()

# Directory containing your DDL files
ddl_directory = './local_csv_data/tabel_definitions_DDL'

# GCS bucket and path where the CSV files are stored
bucket_name = os.environ.get("MY_GCS_BUCKET")
gcs_path = os.environ.get("MY_GCS_BUCKET_URL") # = "gs://rox_landing" ? need to confirm

# List all DDL files in the directory (assuming they have a .sql extension)
ddl_files = [f for f in os.listdir(ddl_directory) if f.endswith('.sql')]

for ddl_file in ddl_files:
    # Extract the table's name part after the underscore from the DDL filename
    table_name_suffix = ddl_file.split('_')[1].replace('.sql', '')

    #gcs_uri = f"gs://{bucket_name}/{gcs_path}table_{table_name_suffix}.csv" #to match each table corresponding to its schema
    #testar o seguinte: 
    #gcs_uri = f"gs://{bucket_name}/table_{table_name_suffix}.csv"
    gcs_uri = f"gs://rox_landing/table_{table_name_suffix}.csv"
    
    # Assume dataset and table names in BigQuery
    dataset_name = 'testRoxDB'
    table_name = ddl_file.replace('.sql', '')
    table_ref = client.dataset(dataset_name).table(table_name)

    # Fetch the table's schema /
    #if this doesnt works, try to dump as json the dataframe schemas or (probably this will work because "TIPAGEM É DIFERENTE DOS DDL DO GCP")
    #try to force a schema to a dataframe with the same types as big query
    table = client.get_table(table_ref)
    
    # Setting up the load job configuration
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1  # Assumes first row is header
    job_config.field_delimiter = ","  # Set the delimiter to ";"
    job_config.schema = table.schema  # Explicitly set the schema

    # Execute the load job
    load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    load_job.result()  # Wait for the job to finish

    print(f"Data loaded to {table_name} from {gcs_uri}!")

