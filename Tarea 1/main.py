# llamada de ejecucion tiene que seguir este formato "spark-submit programaestudiante.py ciclista.csv ruta.csv actividad.csv" 

import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import (DateType, IntegerType, FloatType, StringType,
                               StructField, StructType, TimestampType)
from funciones import leer_csv_con_schema, top_5_ciclistas_por_total_km_por_provincia, top_5_ciclistas_por_promedio_diario_por_provincia, unir_actividades_y_rutas, unir_ciclistas_y_actividades

def main():
   
    # Validar los argumentos de entrada
    if len(sys.argv) != 4:
        print("Uso: spark-submit main.py <ruta_ciclista.csv> <ruta_ruta.csv> <ruta_actividad.csv>")
        sys.exit(-1)

    # Obtener rutas de los archivos desde los argumentos
    ruta_ciclista = sys.argv[1]
    ruta_ruta = sys.argv[2]
    ruta_actividad = sys.argv[3]

    # Crear la sesión de Spark
    spark = SparkSession.builder.appName("Procesamiento Ciclistas").getOrCreate()

    # Definir los esquemas para cada archivo
    # ciclista.csv
    schema_ciclista = StructType([
        StructField('cedula', IntegerType(), True),
        StructField('nombre_completo', StringType(), True),
        StructField('provincia', StringType(), True)
    ])

    # ruta.csv
    schema_ruta = StructType([
        StructField('codigo_de_ruta', IntegerType(), True),
        StructField('nombre_ruta', StringType(), True),
        StructField('distancia_km', FloatType(), True)
    ])

    # actividad.csv
    schema_actividad = StructType([
        StructField('codigo_de_ruta', IntegerType(), True),
        StructField('cedula', IntegerType(), True),
        StructField('fecha_actividad', DateType(), True)
    ])

    # Leer los archivos CSV usando la función y los esquemas
    df_ciclista = leer_csv_con_schema(spark, ruta_ciclista, schema_ciclista)
    df_ruta = leer_csv_con_schema(spark, ruta_ruta, schema_ruta)
    df_actividad = leer_csv_con_schema(spark, ruta_actividad, schema_actividad)

    # Mostrar los dataframes para verificar la carga (puedes quitar esto después)
    print("Datos de Ciclistas:")
    df_ciclista.show()

    print("Datos de Rutas:")
    df_ruta.show()

    print("Datos de Actividades:")
    df_actividad.show()
    
    # Unir ciclistas con actividades
    df_ciclista_actividad = unir_ciclistas_y_actividades(df_ciclista, df_actividad)
    print("Ciclistas con Actividades:")
    df_ciclista_actividad.show()

    # Unir el resultado anterior con las rutas
    def_ciclista_actividad_ruta = unir_actividades_y_rutas(df_ciclista_actividad, df_ruta)
    print("Ciclistas con Actividades y Rutas:")
    def_ciclista_actividad_ruta.show()

    df_top_5_ciclistas_por_total_km_por_provincia = top_5_ciclistas_por_total_km_por_provincia(def_ciclista_actividad_ruta)
    print("Top 5 Ciclistas por Total KM por Provincia:")
    df_top_5_ciclistas_por_total_km_por_provincia.show()

    df_top_5_ciclistas_por_promedio_diario = top_5_ciclistas_por_promedio_diario_por_provincia(def_ciclista_actividad_ruta)
    print("Top 5 Ciclistas por Promedio Diario de KM por Provincia:")
    df_top_5_ciclistas_por_promedio_diario.show()

    # Detener la sesión de Spark
    spark.stop()

if __name__ == '__main__':
    main()