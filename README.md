# rox-gcp-test
important : create and manage your credentials.json and then pass as an variable on git bash

export GOOGLE_APPLICATION_CREDENTIALS="path_to_your_service_account_file.json"

you need to have a .env file that stores all your local enviroments to protect your data for example:

export  MY_GCS_BUCKET = "<bucket_name>"
export  MY_GCS_BUCKET_URL = "<bucket_URL>"
export  GOOGLE_APPLICATION_CREDENTIALS="<your_credentials_path>"