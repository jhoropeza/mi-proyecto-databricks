# notebooks/ingestion/main_ingestion.py

from pyspark.sql import SparkSession

def run_ingestion(spark: SparkSession, input_path: str, output_path: str):
    # 1. Leer datos (simulación)
    df = spark.read.json(input_path) 
    
    # 2. Transformación sencilla (añadir columna)
    df_transformed = df.withColumn("processed_status", lit("CLEANSED"))
    
    # 3. Escribir datos
    df_transformed.write.mode("overwrite").parquet(output_path)
    print(f"Datos procesados y guardados en: {output_path}")

if __name__ == "__main__":
    # Inicialización de Spark (necesaria para el entorno de Databricks)
    spark = SparkSession.builder.appName("IngestionJob").getOrCreate()
    
    # Obtener parámetros de entrada (simulación)
    dbutils.widgets.text("input_loc", "dbfs:/mnt/raw/data.json")
    dbutils.widgets.text("output_loc", "dbfs:/mnt/curated/processed/")
    
    input_path = dbutils.widgets.get("input_loc")
    output_path = dbutils.widgets.get("output_loc")
    
    run_ingestion(spark, input_path, output_path)