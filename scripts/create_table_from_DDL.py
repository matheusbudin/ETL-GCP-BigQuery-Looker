from google.cloud import bigquery
import os

# Set up the BigQuery client
client = bigquery.Client()

# Directory containing your DDL files
ddl_directory = './local_csv_data/tabel_definitions_DDL'

# List all DDL files in the directory (assuming they have a .sql extension)
ddl_files = [f for f in os.listdir(ddl_directory) if f.endswith('.sql')]

for ddl_file in ddl_files:
    ddl_file_path = os.path.join(ddl_directory, ddl_file)
    
    # Read the DDL content
    with open(ddl_file_path, 'r') as file:
        ddl_content = file.read()

    # Execute the DDL content to create the table
    job = client.query(ddl_content)
    job.result()  # Wait for the query to finish

    print(f"Table created successfully from {ddl_file}!")