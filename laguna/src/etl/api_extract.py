import pyspark
import pyspark.sql
from pyspark.sql.types import *
from pyspark.sql.functions import  *
#from pyspark.sql import SparkSession
from google.cloud import storage
import pandas as pd
import requests
import csv
import json


#Check Api Operation
def api_check(url):
        response = requests.get(url)
        if response.status_code == 200:
                print('The API works normally!!')
        else:
                print('API offline (u.u)')
                
                

                
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




def extract_data(url):
        s = requests.get(url)
        s.json()
        print(s.json())
        #pd.DataFrame(s.json()[1])
        

"""
      
        response = requests.get(api_url)
        jsondata = response.json()
        data = []
        #print(jsondata)
        pretty = json.dumps(jsondata, indent= 4)
        #print(pretty)
        df = pd.DataFrame(pretty('data'))
        print(df)
        
        
       
        # schema = StructType([
        #         StructField("categories", StringType(),True),
        #         StructField("created_at", StringType(),True),
        #         StructField("icon_url", StringType(),True),
        #         StructField("updated_at", StringType(),True),
        #         StructField("id", StringType(),True),
        #         StructField("url", StringType(),True),
        #         StructField("value", StringType(),True) 
        # ])
        
        # rdd = spark.sparkContext.parallelize(pretty)
        # df = spark.read.json(rdd)
        # df.show()
        #df = spark.read.format('json').load(pretty)
        #df.show()
        
        #try:
        #for item in jsondata['d']:
        
        #        print(item)
                # id_str = str(item['id'])
                
                # price_str= str(item['price'])
                #listing = [id, title, price,description]
                #data.append(listing)
                #print(data)
#except Exception as e:
        #        print('Error :( ')
        #print(data)

        #data_df = spark.read.format('json').load(api_url)

        #data_df.write.mode('overwrite').csv(gcp_raw_path)

        #spark.stop()        




def upload_to_gcp(bucket_name,source_file_name,destination_blob_name,prefix):
        try:
                storage_client = storage.Client()
                bucket = storage_client.bucket(bucket_name)
                blob = bucket.blob(destination_blob_name)
                for blob in bucket.list_blobs(bucket_name,prefix=prefix):
                        print(f'Prefix Route {blob.name}')
                print( f"File {source_file_name} uploaded to {destination_blob_name} , Successfully" )
        except Exception as e:
                print(e)
"""
            
if __name__ == '__main__':
    url = 'http://api.worldbank.org/v2/countries/ch;/indicators/SP.RUR.TOTL.?format=json&per_page=1000&date<=2001&date>=1995'
    gcp_raw_path = 'gs://laguna-certification-associate/datalake/raw_data/plati_data.csv'
    source_file_name = 'C:/Users/carlos.nieves/Documents/foopy/platzi-python/data.csv'
    bucket_name = 'laguna-certification-associate'
    destination_blob_name = 'platzi_data.csv'
    prefix = 'gs://laguna-certification-associate/datalake/raw_data'
    

    
    api_check(url)
    bucket_check()
    extract_data(url)
    #extract_data(gcp_raw_path , api_url)
    #upload_to_gcp(bucket_name,source_file_name,destination_blob_name,prefix)
    
