from pyspark.sql.functions import *
from pyspark.conf import SparkConf
from pyspark.context import *
from pyspark.sql import SQLContext, SparkSession
import time
import os, uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os.path
from datetime import datetime, timedelta


def get_new_job ():

    blob_service_client = BlobServiceClient.from_connection_string('*************************')
        
    container_client=blob_service_client.get_container_client("inputdata")
        
    blob_list = container_client.list_blobs()

    list_new = []

    for blob in blob_list:
        try:
            blob_name=blob.name
            print('verifying if following blob is new: ', blob_name)
            
            blob_int=blob_name.split('.')[0]
            blobi=int(blob_int)
            bob_date = datetime.fromtimestamp(blobi)
            limit=datetime.now() - timedelta(minutes=5)

            #if bob_date < limit:
            #je verifie que le blob a ete ajoute plus recemment qu'il y a 5min
            if bob_date > limit: 
                print(blobi)
                list_new.append(blobi)
        except:
            print('exception', blob.name, type(blob.name))

    return(list_new)




def data_to_res(list_job):
    #blob connection
    storage_account_name = "*******"
    storage_account_access_key = "**********"
    file_location = "**********"
    file_type = "csv"
    conf = SparkConf()
    conf.set("fs.azure.account.key."+storage_account_name+".blob.core.windows.net",storage_account_access_key)
    print('connected to blob, following files will checked for processing: ', list_job)   
    
    #spark initialization
    sc = SparkContext(conf=conf)
    sql_sc = SQLContext(sc)

    for new in list_job:
        try:
            print('dealing with {}'.format(new))        
            
            spark = SparkSession.builder.master('local').appName('test').getOrCreate()
            df_name='*********/{}.csv'.format(new)
            df = spark.read.csv(df_name, header='true')
            df.createOrReplaceTempView("table1")
            
            mean_amount=df.select(avg("amount"))
            mean_name="********".format(new)
            mean_amount.coalesce(1).write.option('header', 'true').csv(mean_name)
            print('mean calculated')


            country_mean=df.groupBy('country_sender').agg({'amount': 'mean'})
            country_m_name="*********".format(new)
            country_mean.coalesce(1).write.option('header', 'true').csv(country_m_name)
            print('mean by country calculated')

            country_sum=df.groupBy('country_sender').agg({'amount': 'sum'})
            country_s_name="*********".format(new)
            country_sum.coalesce(1).write.option('header', 'true').csv(country_s_name)
            print('sum by country calculated')
            
        except:
            print('not able to process', new)

    return('Finished to process new data')

liste_job=get_new_job()

data_to_res(liste_job)