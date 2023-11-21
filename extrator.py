import os
import configparser
import zipfile
import kaggle
import pandas as pd
from pyspark.sql import SparkSession

# Lê as configurações do arquivo config.ini
config = configparser.ConfigParser()
config.read("config.ini")

# Configura a variável de ambiente para a chave do Kaggle
os.environ["KAGGLE_API_KEY"] = config.get("kaggle", "api_key")

# Configurações do Spark
spark = SparkSession.builder.appName("NetflixData").getOrCreate()

# Diretório para armazenar os dados
data_dir = "/caminho/do/arquivo/para/armazenar"
os.makedirs(data_dir, exist_ok=True)

# Nome do conjunto de dados no Kaggle
dataset_name = "shivamb/netflix-shows"

# Download do conjunto de dados do Kaggle
kaggle.api.dataset_download_files(dataset_name, path=data_dir, unzip=True)

# Encontra o arquivo CSV extraído
csv_file = [file for file in os.listdir(data_dir) if file.endswith('.csv')][0]

# Caminho completo do arquivo CSV
file_path = os.path.join(data_dir, csv_file)

# Lê o CSV usando o Pandas para inspecionar os dados
pandas_df = pd.read_csv(file_path)

# Converte o DataFrame do Pandas para um DataFrame do PySpark
spark_df = spark.createDataFrame(pandas_df)

# Exibe o esquema dos dados
spark_df.printSchema()

# Exibe algumas linhas dos dados
spark_df.show()

# Fecha a sessão Spark
spark.stop()


# Exclue os arquivos baixados do Kaggle

for file in os.listdir(data_dir):
    file_path = os.path.join(data_dir, file)
    os.remove(file_path)


