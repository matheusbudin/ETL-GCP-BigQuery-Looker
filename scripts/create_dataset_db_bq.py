from google.cloud import bigquery
from google.api_core.exceptions import NotFound

client = bigquery.Client()
dataset_name = 'testRoxDB'
dataset_ref = client.dataset(dataset_name)

# Check if the dataset exists
try:
    client.get_dataset(dataset_ref)
    print(f"Dataset {dataset_name} already exists.")
except NotFound:
    dataset = bigquery.Dataset(dataset_ref)
    dataset = client.create_dataset(dataset)  # API request
    print(f"Dataset {dataset_name} created.")
