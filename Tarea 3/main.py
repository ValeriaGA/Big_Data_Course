# llamada de ejecucion tiene que seguir este formato "spark-submit programaestudiante.py caja*.yaml" 

import sys
import yaml
import glob

from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType, StringType, StructField, StructType)
from pyspark.sql.functions import col, sum, desc
from funciones import flat_product_list, obtener_metricas, obtener_total_cajas, obtener_total_productos


def main():
   
    # Validar los argumentos de entrada
    if len(sys.argv) != 2:
        print("Uso: spark-submit main.py caja*.yaml")
        sys.exit(-1)
        
    # Obtener el patrón de archivo desde los argumentos
    patron_archivos = sys.argv[1]
    # Usar glob para expandir el patrón a una lista de archivos
    rutas_cajas = glob.glob(patron_archivos)

    if not rutas_cajas:
        print(f"Error: No se encontraron archivos que coincidan con el patrón '{patron_archivos}'")
        sys.exit(-1)

    print(f"Archivos a procesar: {rutas_cajas}")

    # Crear la sesión de Spark
    spark = SparkSession.builder.appName("Procesamiento Supermercado").getOrCreate()

    # Lista para almacenar los datos aplanados de todos los archivos
    datos_aplanados = []

    # Procesar cada archivo YAML en el driver
    for ruta_archivo in rutas_cajas:
        with open(ruta_archivo, 'r') as f:
            data = yaml.safe_load(f)
            numero_caja = data['numero_caja']
            
            # Iterar sobre cada compra
            for i, compra_item in enumerate(data.get('compras', [])):
                id_compra = i + 1 # Asignar un ID de compra secuencial por archivo
                
                # Iterar sobre cada producto dentro de una compra
                for producto_item in compra_item.get('compra', []):
                    producto_info_list = producto_item.get('producto', [])
                    producto_dict = flat_product_list(producto_info_list)
                    
                    datos_aplanados.append((
                        numero_caja,
                        id_compra,
                        producto_dict.get('nombre'),
                        producto_dict.get('cantidad'),
                        producto_dict.get('precio_unitario')
                    ))

    # Definir el esquema para el DataFrame
    schema = StructType([
        StructField("numero_caja", IntegerType(), True),
        StructField("id_compra", IntegerType(), True),
        StructField("nombre_producto", StringType(), True),
        StructField("cantidad_producto", IntegerType(), True),
        StructField("precio_unitario_producto", IntegerType(), True)
    ])

    # Crear el DataFrame a partir de los datos aplanados
    df_ventas = spark.createDataFrame(datos_aplanados, schema)

    print("DataFrame de Ventas Creado:")
    df_ventas.show()

    # analisis 1: total vendido por cada producto
    df_total_productos = obtener_total_productos(df_ventas)
    print("Total vendido por cada producto:")
    df_total_productos.show()

    # Guardar el resultado en un archivo CSV
    df_total_productos.coalesce(1).write.csv("total_productos.csv", header=True, mode="overwrite")

    # analisis 2: total vendido por cada caja
    df_total_cajas = obtener_total_cajas(df_ventas)
    print("Total vendido por cada caja:")
    df_total_cajas.show()

    # Guardar el resultado en un archivo CSV
    df_total_cajas.coalesce(1).write.csv("total_cajas.csv", header=True, mode="overwrite")

    # analisis 3: metricas de productos
    df_metricas = obtener_metricas(df_ventas)
    df_metricas.show()

    # Guardar el resultado en un archivo CSV
    df_metricas.coalesce(1).write.csv("metricas_de_productos.csv", header=True, mode="overwrite")

    # Detener la sesión de Spark
    spark.stop()

if __name__ == '__main__':
    main()