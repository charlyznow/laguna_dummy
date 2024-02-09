import pyspark
import pyspark.sql
from pyspark.sql.types import *
from pyspark.sql.functions import  *
#from pyspark.sql import SparkSession
from google.cloud import storage , bigquery 
import pandas as pd



                #validate the existence of Bucket
def bucket_check():
        client = storage.Client()
        bucket_name = 'laguna-certification-associate' 
        buckets = list(client.list_buckets())
        bucket_found = False
        for bucket in buckets:
                if bucket.name == bucket_name:
                        print(f'Bucket {bucket_name}  Found!!')
                        bucket_found = True
                        break
        if not bucket_found:
                print('Bucket not exist, please check the GCP account or create the bucket')
                
#load Data into Bigquery
client = bigquery.Client()

job_config = bigquery.JobConfig(
    schema = [
        bigquery.SchemaField('YEAR' ,'STRING'),
        bigquery.SchemaField('MONTH' ,'STRING'),
        bigquery.SchemaField('SUPPLIER' ,'STRING'),
        bigquery.SchemaField('ITEM' ,'STRING'),
        bigquery.SchemaField('YEAR' ,'STRING'),
    ]

        )

                
                



YEAR,MONTH,SUPPLIER,ITEM CODE,ITEM DESCRIPTION,ITEM TYPE,RETAIL SALES,RETAIL TRANSFERS,WAREHOUSE SALES
2020,1,REPUBLIC NATIONAL DISTRIBUTING CO,100009,BOOTLEG RED - 750ML,WINE,0.00,0.00,2.00


                
if __name__ =='__main__':
    bucket_name = 'laguna-certification-associate'
    prefix = 'gs://laguna-certification-associate/datalake/raw_data'
    


    bucket_check()

