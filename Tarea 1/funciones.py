from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, date_format, udf, sum, rank, countDistinct, round
from pyspark.sql.types import (DateType, IntegerType, FloatType, StringType,
                               StructField, StructType, TimestampType)
from pyspark.sql.window import Window


def leer_csv_con_schema(spark, ruta_archivo, schema):
    """
    Lee un archivo CSV usando una sesión de Spark y un esquema definidos.
    :param spark: SparkSession activa.
    :param ruta_archivo: Ruta al archivo CSV.
    :param schema: StructType para el DataFrame.
    :return: DataFrame de Spark.
    """
    dataframe = spark.read.csv(ruta_archivo,
                               schema=schema,
                               header=False)
    return dataframe

def top_5_ciclistas_por_total_km_por_provincia(df_completo: DataFrame) -> DataFrame:
    """
    Calcula el top 5 de ciclistas por kilómetros totales recorridos, agrupados por provincia.

    :param df_completo: DataFrame de Spark con las columnas 'provincia', 'nombre_completo', 
                        'cedula' y 'distancia_km'.
    :return: DataFrame con el top 5 de ciclistas por provincia, incluyendo las columnas
             'provincia', 'nombre_completo', 'total_km' y 'rank'.
    """
    # Calcular la distancia total por ciclista en cada provincia
    df_total_km = df_completo.groupBy("provincia", "cedula", "nombre_completo") \
                             .agg(sum("distancia_km").alias("total_km"))

    # Definir una "ventana" para particionar los datos por provincia y ordenarlos por km totales
    window_spec = Window.partitionBy("provincia").orderBy(col("total_km").desc())

    # Añadir una columna de ranking sobre la ventana definida
    df_ranked = df_total_km.withColumn("rank", rank().over(window_spec))

    # Filtrar para quedarse solo con los que tienen un ranking de 1 a 5
    df_top5 = df_ranked.filter(col("rank") <= 5)

    return df_top5

def top_5_ciclistas_por_promedio_diario_por_provincia(df_completo: DataFrame) -> DataFrame:
    """
    Calcula el top 5 de ciclistas por promedio de kilómetros diarios, agrupados por provincia.

    :param df_completo: DataFrame de Spark con las columnas 'provincia', 'nombre_completo', 
                        'cedula', 'distancia_km' y 'fecha_actividad'.
    :return: DataFrame con el top 5 de ciclistas por provincia, incluyendo las columnas
             'provincia', 'nombre_completo', 'promedio_diario_km' y 'rank'.
    """
    # Calcular la distancia total y el número de días de actividad únicos por ciclista en cada provincia
    df_agregado = df_completo.groupBy("provincia", "cedula", "nombre_completo") \
                             .agg(
                                 sum("distancia_km").alias("total_km"),
                                 countDistinct("fecha_actividad").alias("dias_activos")
                             )

    # Calcular el promedio diario de kilómetros
    df_promedio = df_agregado.withColumn(
        "promedio_diario_km", round(col("total_km") / (col("dias_activos") + 1e-6), 2)
    )

    # Definir una ventana para particionar los datos por provincia y ordenarlos por promedio diario
    window_x_provincia = Window.partitionBy("provincia").orderBy(col("promedio_diario_km").desc())

    # Añadir una columna de ranking sobre la ventana definida
    df_ranked = df_promedio.withColumn("rank", rank().over(window_x_provincia))

    # Filtrar para quedarse solo con los que tienen un ranking de 1 a 5
    df_top5 = df_ranked.filter(col("rank") <= 5)

    return df_top5

def convertir_fecha(fecha_str):
    """
    Convierte una cadena de fecha en formato 'yyyy-MM-dd' a un objeto datetime.date.
    :param fecha_str: Cadena de fecha en formato 'yyyy-MM-dd'.
    :return: Objeto datetime.date.
    """
    return datetime.strptime(fecha_str, '%Y-%m-%d').date()


def unir_ciclistas_y_actividades(ciclistas, actividades):
    """
    Une dos listas de diccionarios (ciclistas y actividades) basándose en la clave 'cedula'.
    """

    return actividades.join(ciclistas, on='cedula', how='inner')

def unir_actividades_y_rutas(actividades, rutas):
    """
    Une dos listas de diccionarios (actividades y rutas) basándose en la clave 'codigo_de_ruta'.
    """

    return actividades.join(rutas, on='codigo_de_ruta', how='inner')
   
