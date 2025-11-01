import os
from datetime import date
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, FloatType)
from funciones import caja_con_mas_ventas, caja_con_menos_ventas, obtener_total_productos, obtener_total_cajas, obtener_metricas, percentil_25_por_caja, producto_de_mayor_ingreso, producto_mas_vendido_por_unidad, percentil_75_por_caja, percentil_50_por_caja



def test_obtener_total_productos(spark_session):
    """
    Prueba unitaria para la función obtener_total_productos.
    Verifica que el conteo total de productos en el DataFrame sea correcto.
    """
    # Preparar el DataFrame de prueba
    schema = StructType([
        StructField("numero_caja", IntegerType(), True),
        StructField("id_compra", IntegerType(), True),
        StructField("nombre_producto", StringType(), True),
        StructField("cantidad_producto", IntegerType(), True),
        StructField("precio_unitario_producto", IntegerType(), True)
    ])
    datos = [
        (1, 1, 'Manzana', 2, 100),
        (1, 1, 'Banana', 3, 300),
        (1, 2, 'Naranja', 1, 70),
        (2, 1, 'Manzana', 1, 50),
        (2, 1, 'Banana', 2, 30)
    ]
    df_prueba = spark_session.createDataFrame(datos, schema)

    # Llamar a la función a probar
    total_productos = obtener_total_productos(df_prueba)
    total_productos.show()

    # Preparar datos esperados
    schema_esperado = StructType([
        StructField("nombre_producto", StringType(), True),
        StructField("cantidad_total_producto", IntegerType(), True)
    ])
    datos_esperados = [
        ('Naranja', 1),
        ('Banana', 5),
        ('Manzana', 3)
    ]
    # Crear el DataFrame esperado
    df_esperado = spark_session.createDataFrame(datos_esperados, schema_esperado)


    # Verificar el resultado
    assert total_productos.collect() == df_esperado.collect()

def test_obtener_total_cajas(spark_session):
    """
    Prueba unitaria para la función obtener_total_cajas.
    Verifica que el total vendido por caja sea calculado correctamente.
    """

    # Preparar el DataFrame de prueba
    schema = StructType([
        StructField("numero_caja", IntegerType(), True),
        StructField("id_compra", IntegerType(), True),
        StructField("nombre_producto", StringType(), True),
        StructField("cantidad_producto", IntegerType(), True),
        StructField("precio_unitario_producto", IntegerType(), True)
    ])
    datos = [
        (1, 1, 'Manzana', 2, 100),
        (1, 1, 'Banana', 3, 300),
        (1, 2, 'Naranja', 1, 70),
        (2, 1, 'Manzana', 1, 50),
        (2, 1, 'Banana', 2, 30)
    ]
    df_prueba = spark_session.createDataFrame(datos, schema)

    # Llamar a la función a probar
    total_cajas = obtener_total_cajas(df_prueba)
    total_cajas.show()

    # Preparar datos esperados
    schema_esperado = StructType([
        StructField("numero_caja", IntegerType(), True),
        StructField("total_vendido", IntegerType(), True)
    ])
    datos_esperados = [
        (1, 1170),
        (2, 110)
    ]
    # Crear el DataFrame esperado
    df_esperado = spark_session.createDataFrame(datos_esperados, schema_esperado)

    # Verificar el resultado
    assert total_cajas.collect() == df_esperado.collect()

def test_obtener_metricas(spark_session):
    """
    Prueba unitaria para la función obtener_metricas.
    Verifica que las métricas de productos sean calculadas correctamente.
    """

    # Preparar el DataFrame de prueba
    schema = StructType([
        StructField("numero_caja", IntegerType(), True),
        StructField("id_compra", IntegerType(), True),
        StructField("nombre_producto", StringType(), True),
        StructField("cantidad_producto", IntegerType(), True),
        StructField("precio_unitario_producto", IntegerType(), True)
    ])
    datos = [
        (1, 1, 'Manzana', 2, 100),
        (1, 1, 'Banana', 3, 300),
        (1, 2, 'Naranja', 1, 700),   
        (2, 1, 'Manzana', 1, 500),
        (2, 1, 'Banana', 2, 300),
        (3, 1, 'Manzana', 1, 500),
        (3, 1, 'Banana', 1, 300)
    ]
    df_prueba = spark_session.createDataFrame(datos, schema)

    # Llamar a la función a probar
    metricas = obtener_metricas(df_prueba)
    metricas.show()

    # Preparar datos esperados
    schema_esperado = StructType([
        StructField("tipo_metrica", StringType(), True),
        StructField("valor", StringType(), True)
    ])
    datos_esperados = [
        ('caja_con_mas_ventas', '1'),
        ('caja_con_menos_ventas', '3'),
        ('percentil_25_por_caja', '800.0'),
        ('percentil_50_por_caja', '1100.0'),
        ('percentil_75_por_caja', '1800.0'),
        ('producto_mas_vendido_por_unidad', 'Banana'),
        ('producto_de_mayor_ingreso', 'Banana')
    ]
    # Crear el DataFrame esperado
    df_esperado = spark_session.createDataFrame(datos_esperados, schema_esperado)

    # Verificar el resultado
    assert metricas.collect() == df_esperado.collect()


def test_caja_con_mas_ventas(spark_session):
    """
    Prueba unitaria para verificar que la caja con más ventas se identifica correctamente.
    """

    # Preparar el DataFrame de prueba
    schema = StructType([
        StructField("numero_caja", IntegerType(), True),
        StructField("id_compra", IntegerType(), True),
        StructField("nombre_producto", StringType(), True),
        StructField("cantidad_producto", IntegerType(), True),
        StructField("precio_unitario_producto", IntegerType(), True)
    ])
    datos = [
        (1, 1, 'Manzana', 2, 100),
        (1, 1, 'Banana', 3, 300),
        (1, 2, 'Naranja', 1, 700),   
        (2, 1, 'Manzana', 1, 500),
        (2, 1, 'Banana', 2, 300),
        (3, 1, 'Manzana', 1, 500),
        (3, 1, 'Banana', 1, 300)
    ]
    df_prueba = spark_session.createDataFrame(datos, schema)

    # Llamar a la función a probar
    metricas = caja_con_mas_ventas(df_prueba)
    metricas.show()

    # Preparar datos esperados
    schema_esperado = StructType([
        StructField("numero_caja", IntegerType(), True),
        StructField("total_vendido", IntegerType(), True)
    ])
    datos_esperados = [
        (1, 1800)
    ]

    # Crear el DataFrame esperado
    df_esperado = spark_session.createDataFrame(datos_esperados, schema_esperado)

    # Verificar el resultado
    assert metricas.collect() == df_esperado.collect()


def test_caja_con_menos_ventas(spark_session):
    """
    Prueba unitaria para verificar que la caja con menos ventas se identifica correctamente.
    """

    # Preparar el DataFrame de prueba
    schema = StructType([
        StructField("numero_caja", IntegerType(), True),
        StructField("id_compra", IntegerType(), True),
        StructField("nombre_producto", StringType(), True),
        StructField("cantidad_producto", IntegerType(), True),
        StructField("precio_unitario_producto", IntegerType(), True)
    ])
    datos = [
        (1, 1, 'Manzana', 2, 100),
        (1, 1, 'Banana', 3, 300),
        (1, 2, 'Naranja', 1, 700),   
        (2, 1, 'Manzana', 1, 500),
        (2, 1, 'Banana', 2, 300),
        (3, 1, 'Manzana', 1, 500),
        (3, 1, 'Banana', 1, 300)
    ]
    df_prueba = spark_session.createDataFrame(datos, schema)

    # Llamar a la función a probar
    metricas = caja_con_menos_ventas(df_prueba)
    metricas.show()

    # Preparar datos esperados
    schema_esperado = StructType([
        StructField("numero_caja", IntegerType(), True),
        StructField("total_vendido", IntegerType(), True)
    ])
    datos_esperados = [
        (3, 800)
    ]

    # Crear el DataFrame esperado
    df_esperado = spark_session.createDataFrame(datos_esperados, schema_esperado)

    # Verificar el resultado
    assert metricas.collect() == df_esperado.collect()


def test_percentil_25_por_caja(spark_session):
    """
    Prueba unitaria para verificar que el percentil 25 por caja se calcula correctamente.
    """

    # Preparar el DataFrame de prueba
    schema = StructType([
        StructField("numero_caja", IntegerType(), True),
        StructField("id_compra", IntegerType(), True),
        StructField("nombre_producto", StringType(), True),
        StructField("cantidad_producto", IntegerType(), True),
        StructField("precio_unitario_producto", IntegerType(), True)
    ])
    datos = [
        (1, 1, 'Manzana', 2, 100),
        (1, 1, 'Banana', 3, 300),
        (1, 2, 'Naranja', 1, 700),   
        (2, 1, 'Manzana', 1, 500),
        (2, 1, 'Banana', 2, 300),
        (3, 1, 'Manzana', 1, 500),
        (3, 1, 'Banana', 1, 300)
    ]
    df_prueba = spark_session.createDataFrame(datos, schema)

    # Llamar a la función a probar
    metricas = percentil_25_por_caja(df_prueba)
    metricas.show()

    # Preparar datos esperados
    schema_esperado = StructType([
        StructField("percentil_25_por_caja", FloatType(), True)
    ])
    datos_esperados = [
        (800.0,)
    ]

    # Crear el DataFrame esperado
    df_esperado = spark_session.createDataFrame(datos_esperados, schema_esperado)

    # Verificar el resultado
    assert metricas.collect() == df_esperado.collect()


def test_percentil_50_por_caja(spark_session):
    """
    Prueba unitaria para verificar que el percentil 50 por caja se calcula correctamente.
    """

    # Preparar el DataFrame de prueba
    schema = StructType([
        StructField("numero_caja", IntegerType(), True),
        StructField("id_compra", IntegerType(), True),
        StructField("nombre_producto", StringType(), True),
        StructField("cantidad_producto", IntegerType(), True),
        StructField("precio_unitario_producto", IntegerType(), True)
    ])
    datos = [
        (1, 1, 'Manzana', 2, 100),
        (1, 1, 'Banana', 3, 300),
        (1, 2, 'Naranja', 1, 700),   
        (2, 1, 'Manzana', 1, 500),
        (2, 1, 'Banana', 2, 300),
        (3, 1, 'Manzana', 1, 500),
        (3, 1, 'Banana', 1, 300)
    ]
    df_prueba = spark_session.createDataFrame(datos, schema)

    # Llamar a la función a probar
    metricas = percentil_50_por_caja(df_prueba)
    metricas.show()

    # Preparar datos esperados
    schema_esperado = StructType([
        StructField("percentil_50_por_caja", FloatType(), True)
    ])
    datos_esperados = [
        (1100.0,)
    ]

    # Crear el DataFrame esperado
    df_esperado = spark_session.createDataFrame(datos_esperados, schema_esperado)

    # Verificar el resultado
    assert metricas.collect() == df_esperado.collect()

def test_percentil_75_por_caja(spark_session):
    """
    Prueba unitaria para verificar que el percentil 75 por caja se calcula correctamente.
    """

    # Preparar el DataFrame de prueba
    schema = StructType([
        StructField("numero_caja", IntegerType(), True),
        StructField("id_compra", IntegerType(), True),
        StructField("nombre_producto", StringType(), True),
        StructField("cantidad_producto", IntegerType(), True),
        StructField("precio_unitario_producto", IntegerType(), True)
    ])
    datos = [
        (1, 1, 'Manzana', 2, 100),
        (1, 1, 'Banana', 3, 300),
        (1, 2, 'Naranja', 1, 700),   
        (2, 1, 'Manzana', 1, 500),
        (2, 1, 'Banana', 2, 300),
        (3, 1, 'Manzana', 1, 500),
        (3, 1, 'Banana', 1, 300)
    ]
    df_prueba = spark_session.createDataFrame(datos, schema)

    # Llamar a la función a probar
    metricas = percentil_75_por_caja(df_prueba)
    metricas.show()

    # Preparar datos esperados
    schema_esperado = StructType([
        StructField("percentil_75_por_caja", FloatType(), True)
    ])
    datos_esperados = [
        (1800.0,)
    ]

    # Crear el DataFrame esperado
    df_esperado = spark_session.createDataFrame(datos_esperados, schema_esperado)

    # Verificar el resultado
    assert metricas.collect() == df_esperado.collect()


def test_producto_mas_vendido_por_unidad(spark_session):
    """
    Prueba unitaria para verificar que el producto más vendido por unidad se identifica correctamente.
    """

    # Preparar el DataFrame de prueba
    schema = StructType([
        StructField("numero_caja", IntegerType(), True),
        StructField("id_compra", IntegerType(), True),
        StructField("nombre_producto", StringType(), True),
        StructField("cantidad_producto", IntegerType(), True),
        StructField("precio_unitario_producto", IntegerType(), True)
    ])
    datos = [
        (1, 1, 'Manzana', 2, 100),
        (1, 1, 'Banana', 3, 300),
        (1, 2, 'Naranja', 1, 700),   
        (2, 1, 'Manzana', 1, 500),
        (2, 1, 'Banana', 2, 300),
        (3, 1, 'Manzana', 1, 500),
        (3, 1, 'Banana', 1, 300)
    ]
    df_prueba = spark_session.createDataFrame(datos, schema)

    # Llamar a la función a probar
    metricas = producto_mas_vendido_por_unidad(df_prueba)
    metricas.show()

    # Preparar datos esperados
    schema_esperado = StructType([
        StructField("nombre_producto", StringType(), True),
        StructField("cantidad_total_producto", IntegerType(), True)
    ])
    datos_esperados = [
        ('Banana', 6)
    ]

    # Crear el DataFrame esperado
    df_esperado = spark_session.createDataFrame(datos_esperados, schema_esperado)

    # Verificar el resultado
    assert metricas.collect() == df_esperado.collect()

def test_producto_de_mayor_ingreso(spark_session):
    """
    Prueba unitaria para verificar que el producto de mayor ingreso se identifica correctamente.
    """

    # Preparar el DataFrame de prueba
    schema = StructType([
        StructField("numero_caja", IntegerType(), True),
        StructField("id_compra", IntegerType(), True),
        StructField("nombre_producto", StringType(), True),
        StructField("cantidad_producto", IntegerType(), True),
        StructField("precio_unitario_producto", IntegerType(), True)
    ])
    datos = [
        (1, 1, 'Manzana', 2, 100),
        (1, 1, 'Banana', 3, 300),
        (1, 2, 'Naranja', 1, 700),   
        (2, 1, 'Manzana', 1, 500),
        (2, 1, 'Banana', 2, 300),
        (3, 1, 'Manzana', 1, 500),
        (3, 1, 'Banana', 1, 300)
    ]
    df_prueba = spark_session.createDataFrame(datos, schema)

    # Llamar a la función a probar
    metricas = producto_de_mayor_ingreso(df_prueba)
    metricas.show()

    # Preparar datos esperados
    schema_esperado = StructType([
        StructField("nombre_producto", StringType(), True),
        StructField("total_vendido_producto", IntegerType(), True)
    ])
    datos_esperados = [
        ('Banana', 1800)
    ]

    # Crear el DataFrame esperado
    df_esperado = spark_session.createDataFrame(datos_esperados, schema_esperado)

    # Verificar el resultado
    assert metricas.collect() == df_esperado.collect()