from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
from google.cloud import storage 
import os

keys = '/Users/charlyznow/Documents/laguna-certification-associate-450031d6e119.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = keys

# Spark Session
spark = SparkSession.builder.master("local[*]").appName("Clean Data").\
        config('spark.jars.packages', 
               'com.google.cloud.bigdataoss:gcs-connector:hadoop2-1.9.17').\
        config('spark.jars.excludes',
               'javax.jms:jms,com.sun.jdmk:jmxtools,com.sun.jmx:jmxri').\
        config('spark.driver.userClassPathFirst','true').\
        config('spark.executor.userClassPathFirst','true').\
        config('spark.hadoop.fs.gs.impl',
               'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem').\
        config('spark.hadoop.fs.gs.auth.service.account.enable', 'false').\
        getOrCreate()

#spark.sparkContext.hadoopConfiguration.set("google.cloud.auth.service.account.enable", "false")
#spark.sparkContext.hadoopConfiguration.set("fs.gs.auth.impersonation.service.account.for.user.<USER_NAME>", "username@companyname.com")
#spark.sparkContext.hadoopConfiguration.set("fs.gs.auth.impersonation.service.account.for.group.<GROUP_NAME>", "gcp_projectid")
#spark.sparkContext.hadoopConfiguration.set("fs.gs.auth.impersonation.service.account", "service-account-name@.iam.gserviceaccount.com")

        
gcs_client = storage.Client()


bucket = 'laguna-certification-associate'
path = f'gs://{bucket}/datalake/raw_data/Warehouse_and_Retail_Sales.csv'


#bucket = gcs_client.get_bucket(bucket)
#bucket_name = gcs_client.bucket_name(bucket)

df = spark.read.csv(path).csv('inferchema', True, 'header' , True)
df.head()