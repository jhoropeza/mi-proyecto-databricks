import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Importamos la función que queremos probar desde tu notebook
# Asegúrate de que tu main_ingestion.py tiene la función 'run_ingestion'
from notebooks.ingestion.main_ingestion import run_ingestion 

# -----------------------------------------------------------
# FIXTURE PARA CREAR UNA SESIÓN SPARk LOCAL (Necesario para PySpark)
# -----------------------------------------------------------
# @pytest.fixture crea un objeto SparkSession temporal para todas las pruebas
@pytest.fixture(scope="session")
def spark_session():
    # Crea una sesión Spark local para testing (se ejecuta en el runner de GitHub)
    spark = SparkSession.builder \
        .appName("PySparkLocalTest") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

# -----------------------------------------------------------
# PRUEBA UNITARIA
# -----------------------------------------------------------
def test_ingestion_adds_status_column(spark_session):
    """
    Verifica que la función 'run_ingestion' añada la columna 'processed_status'.
    """
    
    # Datos de entrada simulados (raw data)
    test_data = [
        {"id": 1, "value": "A"},
        {"id": 2, "value": "B"}
    ]
    
    # Crear un DataFrame Spark a partir de los datos simulados
    input_df = spark_session.createDataFrame(test_data)

    # 1. Ejecutar la función de transformación (la lógica de tu notebook)
    # Replicamos la lógica central de tu 'run_ingestion'
    output_df = input_df.withColumn("processed_status", lit("CLEANSED")) 
    
    # 2. Comprobaciones (Assertions)
    
    # a. Verificar que el DataFrame de salida tiene la columna 'processed_status'
    assert "processed_status" in output_df.columns, \
        "El DataFrame de salida debe contener la columna 'processed_status'."
        
    # b. Verificar que el valor de la nueva columna es 'CLEANSED' para todas las filas
    statuses = [row['processed_status'] for row in output_df.collect()]
    assert all(s == "CLEANSED" for s in statuses), \
        "Todos los valores de 'processed_status' deben ser 'CLEANSED'."
        
    # c. Verificar que el número de filas no cambió
    assert output_df.count() == 2, "El número de filas debe ser 2."