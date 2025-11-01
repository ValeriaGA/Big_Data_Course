from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, date_format, udf, sum, rank, countDistinct, round, lit
from pyspark.sql.types import (DateType, IntegerType, FloatType, StringType,
                               StructField, StructType, TimestampType)
from pyspark.sql.window import Window


def flat_product_list(product_list):
    """
    Aplana  la lista de atributos de un producto en un diccionario.
    Ejemplo de entrada: [{'nombre': 'Leche'}, {'cantidad': 2}, {'precio_unitario': 1200}]
    Ejemplo de salida: {'nombre': 'Leche', 'cantidad': 2, 'precio_unitario': 1200}
    """
    product_dict = {}
    for item in product_list:
        product_dict.update(item)
    return product_dict

def obtener_total_productos(df_supermercado: DataFrame) -> DataFrame:
    """
    Crea un dataframe que contiene 2 columnas que representan el nombre de cada producto
    y la cantidad total de ese producto vendida en todas las cajas.

    :param df_supermercado: DataFrame de Spark con las columnas 'numero_caja', 'id_compra', 'nombre_producto', 'cantidad_producto'
                        y 'precio_unitario_producto'.
    :return: DataFrame con el top 5 de ciclistas por provincia, incluyendo las columnas
             'nombre_producto', 'cantidad_total_producto'.
    """

    # Calcular la cantidad total de cada producto
    df_total_productos = df_supermercado.groupBy("nombre_producto") \
                                         .agg(sum("cantidad_producto").alias("cantidad_total_producto"))

    return df_total_productos

def obtener_total_cajas(df_supermercado: DataFrame) -> DataFrame:
    """
    Crea un dataframe que contiene 2 columnas que representan el identificador de cada caja y el
    total vendido por esa caja.

    :param df_supermercado: DataFrame de Spark con las columnas 'numero_caja', 'id_compra', 'nombre_producto', 'cantidad_producto'
                        y 'precio_unitario_producto'.
    :return: DataFrame con el total vendido por cada caja.
    """

    # Calcular el total vendido por cada caja
    df_total_cajas = df_supermercado.groupBy("numero_caja") \
                                      .agg(sum(col("cantidad_producto") * col("precio_unitario_producto")).alias("total_vendido"))

    return df_total_cajas


def caja_con_mas_ventas(df_supermercado: DataFrame) -> DataFrame:
    """
    Crea un dataframe que contiene el identificador de la caja con más ventas (ventas en dinero).

    :param df_supermercado: DataFrame de Spark con las columnas 'numero_caja', 'id_compra', 'nombre_producto', 'cantidad_producto'
                        y 'precio_unitario_producto'.
    :return: DataFrame con la caja que tiene más ventas.
    """
    # Calcular el total vendido por cada caja
    df_total_cajas = obtener_total_cajas(df_supermercado)

    # Encontrar la caja con más ventas
    df_caja_mas_ventas = df_total_cajas.orderBy(col("total_vendido").desc()).limit(1)

    return df_caja_mas_ventas

def caja_con_menos_ventas(df_supermercado: DataFrame) -> DataFrame:
    """
    Crea un dataframe que contiene el identificador de la caja con menos ventas (ventas en dinero).

    :param df_supermercado: DataFrame de Spark con las columnas 'numero_caja', 'id_compra', 'nombre_producto', 'cantidad_producto'
                        y 'precio_unitario_producto'.
    :return: DataFrame con la caja que tiene menos ventas.
    """
    # Calcular el total vendido por cada caja
    df_total_cajas = obtener_total_cajas(df_supermercado)

    # Encontrar la caja con menos ventas
    df_caja_menos_ventas = df_total_cajas.orderBy(col("total_vendido").asc()).limit(1)

    return df_caja_menos_ventas

def percentil_25_por_caja(df_supermercado: DataFrame) -> DataFrame:
    """
    Crea un dataframe que contiene el valor monetario que representa el percentil 25
    de las ventas por caja.

    :param df_supermercado: DataFrame de Spark con las columnas 'numero_caja', 'id_compra', 'nombre_producto', 'cantidad_producto'
                        y 'precio_unitario_producto'.
    :return: DataFrame con el percentil 25 de las ventas por caja.
    """
    # Calcular el total vendido por cada caja
    df_total_cajas = obtener_total_cajas(df_supermercado)

    # Calcular el percentil 25
    percentil_25 = df_total_cajas.approxQuantile("total_vendido", [0.25], 0.0)[0]
    df_percentil_25 = df_supermercado.sparkSession.createDataFrame([(percentil_25,)], ["percentil_25_por_caja"])

    return df_percentil_25

def percentil_50_por_caja(df_supermercado: DataFrame) -> DataFrame:
    """
    Crea un dataframe que contiene el valor monetario que representa el percentil 50
    de las ventas por caja.

    :param df_supermercado: DataFrame de Spark con las columnas 'numero_caja', 'id_compra', 'nombre_producto', 'cantidad_producto'
                        y 'precio_unitario_producto'.
    :return: DataFrame con el percentil 50 de las ventas por caja.
    """
    # Calcular el total vendido por cada caja
    df_total_cajas = obtener_total_cajas(df_supermercado)

    # Calcular el percentil 50
    percentil_50 = df_total_cajas.approxQuantile("total_vendido", [0.50], 0.0)[0]
    df_percentil_50 = df_supermercado.sparkSession.createDataFrame([(percentil_50,)], ["percentil_50_por_caja"])

    return df_percentil_50

def percentil_75_por_caja(df_supermercado: DataFrame) -> DataFrame:
    """
    Crea un dataframe que contiene el valor monetario que representa el percentil 75
    de las ventas por caja.

    :param df_supermercado: DataFrame de Spark con las columnas 'numero_caja', 'id_compra', 'nombre_producto', 'cantidad_producto'
                        y 'precio_unitario_producto'.
    :return: DataFrame con el percentil 75 de las ventas por caja.
    """
    # Calcular el total vendido por cada caja
    df_total_cajas = obtener_total_cajas(df_supermercado)

    # Calcular el percentil 75
    percentil_75 = df_total_cajas.approxQuantile("total_vendido", [0.75], 0.0)[0]
    df_percentil_75 = df_supermercado.sparkSession.createDataFrame([(percentil_75,)], ["percentil_75_por_caja"])

    return df_percentil_75

def producto_mas_vendido_por_unidad(df_supermercado: DataFrame) -> DataFrame:
    """
    Crea un dataframe que contiene el nombre del producto que tuvo más
    unidades vendidas.

    :param df_supermercado: DataFrame de Spark con las columnas 'numero_caja', 'id_compra', 'nombre_producto', 'cantidad_producto'
                        y 'precio_unitario_producto'.
    :return: DataFrame con el producto más vendido por unidad.
    """
    # Calcular la cantidad total de cada producto
    df_total_productos = obtener_total_productos(df_supermercado)

    # Encontrar el producto más vendido por unidad
    df_producto_mas_vendido = df_total_productos.orderBy(col("cantidad_total_producto").desc()).limit(1)

    return df_producto_mas_vendido

def producto_de_mayor_ingreso(df_supermercado: DataFrame) -> DataFrame:
    """
    Crea un dataframe que contiene el nombre del producto que generó más cantidad de
    dinero (e.g. cantidad * precio).

    :param df_supermercado: DataFrame de Spark con las columnas 'numero_caja', 'id_compra', 'nombre_producto', 'cantidad_producto'
                        y 'precio_unitario_producto'.
    :return: DataFrame con el producto que generó mayor ingreso.
    """
    # Calcular el ingreso total por cada producto
    df_ingreso_productos = df_supermercado.groupBy("nombre_producto") \
                                      .agg(sum(col("cantidad_producto") * col("precio_unitario_producto")).alias("total_vendido_producto"))
    # Encontrar el producto de mayor ingreso
    df_producto_mayor_ingreso = df_ingreso_productos.orderBy(col("total_vendido_producto").desc()).limit(1)

    return df_producto_mayor_ingreso

def obtener_metricas(df_supermercado: DataFrame) -> DataFrame:
    """
    Crea un dataframe que contiene 2 columnas que representan el tipo de métrica y su valor. En
    particular deberá generarse las siguientes métricas
        - caja_con_mas_ventas: identificador de la caja con más ventas (ventas en
        dinero)
        - caja_con_menos_ventas: identificador de la caja con menos ventas (ventas en
        dinero)
        - percentil_25_por_caja: si se ordenan todas las cajas de menor cantidad de
        ventas a mayor, cuál valor monetario representa el percentil 25
        - percentil_50_por_caja
        - percentil_75_por_caja
        - producto_mas_vendido_por_unidad: nombre del producto que tuvo más
        unidades vendidas
        - producto_de_mayor_ingreso: nombre del producto que generó más cantidad de
        dinero (e.g. cantidad * precio)

    :param df_supermercado: DataFrame de Spark con las columnas 'numero_caja', 'id_compra', 'nombre_producto', 'cantidad_producto'
                        y 'precio_unitario_producto'.
    :return: DataFrame con las métricas calculadas, contiene 2 columnas que representan el tipo de métrica y su valor.
    """
    # Calcular cada métrica individualmente
    df_caja_mas_ventas = caja_con_mas_ventas(df_supermercado)
    print("Caja con mas ventas:")
    df_caja_mas_ventas.show()
    df_caja_menos_ventas = caja_con_menos_ventas(df_supermercado)
    print("Caja con menos ventas:")
    df_caja_menos_ventas.show()
    df_percentil_25 = percentil_25_por_caja(df_supermercado)
    print("Percentil 25 por caja:")
    df_percentil_25.show()
    df_percentil_50 = percentil_50_por_caja(df_supermercado)
    print("Percentil 50 por caja:")
    df_percentil_50.show()
    df_percentil_75 = percentil_75_por_caja(df_supermercado)
    print("Percentil 75 por caja:")
    df_percentil_75.show()
    df_producto_mas_vendido = producto_mas_vendido_por_unidad(df_supermercado)
    print("Producto mas vendido por unidad:")
    df_producto_mas_vendido.show()
    df_producto_mayor_ingreso = producto_de_mayor_ingreso(df_supermercado)
    print("Producto de mayor ingreso:")
    df_producto_mayor_ingreso.show()


    # Estandarizar los DataFrames a un formato de 2 columnas: tipo_metrica, valor
    # El valor se convierte a string para asegurar compatibilidad en la unión.
    df_caja_mas_ventas_std = df_caja_mas_ventas.select(lit("caja_con_mas_ventas").alias("tipo_metrica"), col("numero_caja").cast("string").alias("valor"))
    df_caja_menos_ventas_std = df_caja_menos_ventas.select(lit("caja_con_menos_ventas").alias("tipo_metrica"), col("numero_caja").cast("string").alias("valor"))
    
    df_percentil_25_std = df_percentil_25.select(lit("percentil_25_por_caja").alias("tipo_metrica"), col("percentil_25_por_caja").cast("string").alias("valor"))
    df_percentil_50_std = df_percentil_50.select(lit("percentil_50_por_caja").alias("tipo_metrica"), col("percentil_50_por_caja").cast("string").alias("valor"))
    df_percentil_75_std = df_percentil_75.select(lit("percentil_75_por_caja").alias("tipo_metrica"), col("percentil_75_por_caja").cast("string").alias("valor"))

    df_producto_mas_vendido_std = df_producto_mas_vendido.select(lit("producto_mas_vendido_por_unidad").alias("tipo_metrica"), col("nombre_producto").alias("valor"))
    df_producto_mayor_ingreso_std = df_producto_mayor_ingreso.select(lit("producto_de_mayor_ingreso").alias("tipo_metrica"), col("nombre_producto").alias("valor"))
    
    # Unir todas las métricas en un solo DataFrame
    df_metricas = df_caja_mas_ventas_std.union(df_caja_menos_ventas_std) \
                                      .union(df_percentil_25_std) \
                                      .union(df_percentil_50_std) \
                                      .union(df_percentil_75_std) \
                                      .union(df_producto_mas_vendido_std) \
                                      .union(df_producto_mayor_ingreso_std)

    return df_metricas
