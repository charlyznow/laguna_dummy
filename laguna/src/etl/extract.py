from pyspark.sql import Sparksession
import requests

# Configurar la sesión de Spark
spark = SparkSession.builder \
    .appName("API Extraction") \
    .getOrCreate()

# Definir la URL de la API
api_url = "https://api.escuelajs.co/api/v1/products"

# Realizar la solicitud GET
response = requests.get(api_url)

# Imprimir el código de estado de la respuesta (200 significa éxito)
print("Código de estado de la API:", response.status_code)

# Manejar la respuesta de la API
if response.status_code == 200:
    # Convertir la respuesta JSON a un diccionario
    data_dict = response.json()

    # Crear un DataFrame de PySpark
    df = spark.createDataFrame([data_dict])
    df.show()
else:
    print("Error en la solicitud a la API.")


if __name__ = '__main__':

