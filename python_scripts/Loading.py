import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from dotenv import load_dotenv
import os


# Load environment variables from .env file
load_dotenv()

def loading():
    data = pd.read_csv(r'home/oris/airflow/zipco_dag/clean_data.csv')
    products = pd.read_csv(r'home/oris/airflow/zipco_dag/products.csv')
    customers = pd.read_csv(r'home/oris/airflow/zipco_dag/customers.csv')
    staff = pd.read_csv(r'home/oris/airflow/zipco_dag/staff.csv')
    fact = pd.read_csv(r'home/oris/airflow/zipco_dag/fact.csv')
    
    # Create a BlobServiceClient object
    connect_str= os.getenv('AZURE_CONNECTION_STRING')
    container_name= os.getenv('CONTAINER_NAME')
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)

    # Load data to Azure Blob Storage
    # List of tuples (DataFrame, Blob Name)
    files= [
    (data, 'rawdata/cleansed_data.csv'),
    (products, 'cleaneddata/products.csv'),
    (customers, 'cleaneddata/customers.csv'),
    (staff, 'cleaneddata/staff.csv'),
    (fact, 'cleaneddata/fact.csv'),
    ]

    # Load data to Azure Blob Storage
    for file, blob_name in files:
        blob_client = container_client.get_blob_client(blob_name)
        output = file.to_csv(index=False)
        blob_client.upload_blob(output, overwrite=True)
        print(f"{blob_name} loaded into Azure Blob Storage.")