import pandas as pd
import time
import calendar
import os, uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os.path

time_interval_between_sending = 5 * 10

data_size_in_mb = 1000


def get_data_to_send():

    file_to_upload=input('please specify file to upload path: ')

    return (file_to_upload) #retourne toujours le même fichier.


def send_data(file_to_upload):

    try:
        blob_service_client = BlobServiceClient.from_connection_string('********************')
        
        local_path=file_to_upload
        file_timestamp = str(calendar.timegm(time.gmtime()))+'.csv'
        
        print("new file name", file_timestamp)
        
        blob_client = blob_service_client.get_blob_client(container='inputdata', blob=file_timestamp)

        with open(local_path, "rb") as data:
            blob_client.upload_blob(data)
        
        print("\nUploading to Azure Storage as blob:\n\t" + local_path)

    except Exception as ex:
        print('Exception:')
        print(ex)

    pass


while True:
    t1 = time.time()
    df = get_data_to_send()

    print('Sending data to the infrastructure')
    send_data(df)
    print('Done. Waiting before sending next data...')
    t2 = time.time()

    time_spent_sending_data = t2 - t1

    if time_spent_sending_data < time_interval_between_sending:

        time.sleep(time_interval_between_sending - time_spent_sending_data)
    

