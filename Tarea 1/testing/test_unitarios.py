import os
from datetime import date
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, 
                               DateType, FloatType)
from funciones import leer_csv_con_schema, top_5_ciclistas_por_total_km_por_provincia, top_5_ciclistas_por_promedio_diario_por_provincia, unir_ciclistas_y_actividades, unir_actividades_y_rutas


def test_leer_csv_con_schema(spark_session, tmp_path):
    """
    Prueba unitaria para la función leer_csv_con_schema.
    Utiliza la fixture de spark_session y un archivo temporal creado por tmp_path.
    """
    # crear csv temporal
    ruta_archivo_csv = tmp_path / "test_data.csv"
    contenido_csv = "1,Ana\n2,Carlos"
    ruta_archivo_csv.write_text(contenido_csv, encoding='utf-8')

    # Definir el esquema esperado
    schema_esperado = StructType([
        StructField("id", IntegerType(), True),
        StructField("nombre", StringType(), True)
    ])
    
    # Crear el DataFrame esperado para la comparación
    datos_esperados = [(1, "Ana"), (2, "Carlos")]
    df_esperado = spark_session.createDataFrame(datos_esperados, schema=schema_esperado)
    df_resultado = leer_csv_con_schema(spark_session, str(ruta_archivo_csv), schema_esperado)
    
    # Verificar que el resultado es el esperado
    assert df_resultado.schema == df_esperado.schema
    assert df_resultado.collect() == df_esperado.collect()

def test_unir_ciclistas_y_actividades(spark_session):
    """
    Prueba unitaria para la función unir_ciclistas_y_actividades.
    Verifica que el inner join entre ciclistas y actividades funciona correctamente.
    """
    # Preparar los DataFrames de entrada y el resultado esperado
    schema_ciclistas = StructType([
        StructField('cedula', IntegerType(), True),
        StructField('nombre_completo', StringType(), True),
        StructField('provincia', StringType(), True)
    ])
    datos_ciclistas = [(123, 'Juan Perez', 'San Jose'), (124, 'Ana Gomez', 'Cartago')]
    df_ciclistas = spark_session.createDataFrame(datos_ciclistas, schema_ciclistas)

    schema_actividades = StructType([
        StructField('codigo_de_ruta', IntegerType(), True),
        StructField('cedula', IntegerType(), True),
        StructField('fecha_actividad', DateType(), True)
    ])
    # date format YYYY-MM-DD
    datos_actividades = [(1, 123, date(2023, 1, 1)), (2, 124, date(2023, 1, 2))] 
    df_actividades = spark_session.createDataFrame(datos_actividades, schema_actividades)

    schema_esperado = StructType([
        StructField('cedula', IntegerType(), True),
        StructField('codigo_de_ruta', IntegerType(), True),
        StructField('fecha_actividad', DateType(), True),
        StructField('nombre_completo', StringType(), True),
        StructField('provincia', StringType(), True)
    ])
    datos_esperados = [(123, 1, date(2023, 1, 1), 'Juan Perez', 'San Jose'), (124, 2, date(2023, 1, 2), 'Ana Gomez', 'Cartago')]
    df_esperado = spark_session.createDataFrame(datos_esperados, schema_esperado)
    df_resultado = unir_ciclistas_y_actividades(df_ciclistas, df_actividades)

    df_esperado.show()
    df_resultado.show()

    # Verificar el resultado
    assert df_resultado.count() == 2
    assert sorted(df_resultado.columns) == sorted(df_esperado.columns)
    assert df_resultado.collect() == df_esperado.collect()

def test_unir_ciclistas_sin_actividad_y_actividades(spark_session):
    """
    Prueba unitaria para la función unir_ciclistas_y_actividades.
    Verifica que el inner join entre ciclistas y actividades funciona correctamente.
    """
    # Preparar los DataFrames de entrada y el resultado esperado
    schema_ciclistas = StructType([
        StructField('cedula', IntegerType(), True),
        StructField('nombre_completo', StringType(), True),
        StructField('provincia', StringType(), True)
    ])
    datos_ciclistas = [(123, 'Juan Perez', 'San Jose'), (124, 'Ana Gomez', 'Cartago')]
    df_ciclistas = spark_session.createDataFrame(datos_ciclistas, schema_ciclistas)

    schema_actividades = StructType([
        StructField('codigo_de_ruta', IntegerType(), True),
        StructField('cedula', IntegerType(), True),
        StructField('fecha_actividad', DateType(), True)
    ])
    # date format YYYY-MM-DD
    datos_actividades = [(1, 123, date(2023, 1, 1))] 
    df_actividades = spark_session.createDataFrame(datos_actividades, schema_actividades)

    schema_esperado = StructType([
        StructField('cedula', IntegerType(), True),
        StructField('codigo_de_ruta', IntegerType(), True),
        StructField('fecha_actividad', DateType(), True),
        StructField('nombre_completo', StringType(), True),
        StructField('provincia', StringType(), True)
    ])
    datos_esperados = [(123, 1, date(2023, 1, 1), 'Juan Perez', 'San Jose')]
    df_esperado = spark_session.createDataFrame(datos_esperados, schema_esperado)
    df_resultado = unir_ciclistas_y_actividades(df_ciclistas, df_actividades)

    # Verificar el resultado
    assert df_resultado.count() == 1
    assert sorted(df_resultado.columns) == sorted(df_esperado.columns)
    assert df_resultado.collect() == df_esperado.collect()

def test_unir_actividades_y_rutas(spark_session):
    """
    Prueba unitaria para la función unir_actividades_y_rutas.
    Verifica que el inner join entre actividades y rutas funciona correctamente.
    """
    # Preparar los DataFrames de entrada y el resultado esperado
    schema_actividades = StructType([
        StructField('cedula', IntegerType(), True),
        StructField('nombre_completo', StringType(), True),
        StructField('provincia', StringType(), True),
        StructField('codigo_de_ruta', IntegerType(), True),
        StructField('fecha_actividad', DateType(), True)
    ])
    datos_actividades = [(123, 'Juan Perez', 'San Jose', 1, date(2023, 1, 1)), (124, 'Ana Gomez', 'Cartago', 2, date(2023, 1, 2))]
    df_actividades = spark_session.createDataFrame(datos_actividades, schema_actividades)

    schema_rutas = StructType([
        StructField('codigo_de_ruta', IntegerType(), True),
        StructField('nombre_ruta', StringType(), True),
        StructField('distancia_km', FloatType(), True)
    ])

    datos_rutas = [(1,"Ruta 1", 100.0), (2, "Ruta 2", 150.0)] 
    df_rutas = spark_session.createDataFrame(datos_rutas, schema_rutas)

    # DataFrame esperado después del join
    schema_esperado = StructType([
        StructField('codigo_de_ruta', IntegerType(), True),
        StructField('cedula', IntegerType(), True),
        StructField('nombre_completo', StringType(), True),
        StructField('provincia', StringType(), True),
        StructField('fecha_actividad', DateType(), True),
        StructField('nombre_ruta', StringType(), True),
        StructField('distancia_km', FloatType(), True)
    ])
    datos_esperados = [(1, 123, 'Juan Perez', 'San Jose',  date(2023, 1, 1), 'Ruta 1', 100.0), (2, 124, 'Ana Gomez', 'Cartago',  date(2023, 1, 2), 'Ruta 2', 150.0)]
    df_esperado = spark_session.createDataFrame(datos_esperados, schema_esperado)
    df_resultado = unir_actividades_y_rutas(df_actividades, df_rutas)

    df_esperado.show()
    df_resultado.show()

    # Verificar el resultado
    assert df_resultado.count() == 2
    assert sorted(df_resultado.columns) == sorted(df_esperado.columns)
    assert df_resultado.collect() == df_esperado.collect()

def test_top_5_ciclistas_por_total_km_por_provincia(spark_session):
    """
    Prueba unitaria para la función top_5_ciclistas_por_total_km_por_provincia.
    Verifica que la agrupación, suma y ranking por provincia funcionen correctamente.
    """
    # Preparar datos de entrada complejos
    schema_entrada = StructType([
        StructField('codigo_de_ruta', IntegerType()), StructField('cedula', IntegerType()),
        StructField('nombre_completo', StringType()), StructField('provincia', StringType()),
        StructField('fecha_actividad', DateType()), StructField('nombre_ruta', StringType()),
        StructField('distancia_km', FloatType())
    ])
    # Datos para 6 ciclistas en San Jose (para probar el filtro top 5)
    # y 3 en Cartago (para probar que devuelve todos si son menos de 5)
    datos_entrada = [
        (1, 101, 'Carlos Mora', 'San Jose', date(2023, 1, 1), 'R1', 100.0), # Total: 100
        (1, 102, 'Sofia Rojas', 'San Jose', date(2023, 1, 1), 'R1', 200.0), # Total: 200
        (1, 103, 'Luis Castro', 'San Jose', date(2023, 1, 1), 'R1', 50.0),  # Total: 50 (no debe aparecer)
        (1, 104, 'Ana Solano', 'San Jose', date(2023, 1, 1), 'R1', 300.0), # Total: 300
        (1, 105, 'David Guzman', 'San Jose', date(2023, 1, 1), 'R1', 250.0), # Total: 250
        (1, 106, 'Elena Vega', 'San Jose', date(2023, 1, 1), 'R1', 150.0), # Total: 150
        (1, 101, 'Carlos Mora', 'San Jose', date(2023, 1, 2), 'R2', 20.0),  # Carlos Mora ahora tiene 120
        (2, 201, 'Maria Solis', 'Cartago', date(2023, 1, 1), 'R3', 80.0),   # Total: 80
        (2, 202, 'Jose Araya', 'Cartago', date(2023, 1, 1), 'R3', 120.0),  # Total: 120
        (2, 203, 'Lucia Salas', 'Cartago', date(2023, 1, 1), 'R3', 40.0)    # Total: 40
    ]
    df_entrada = spark_session.createDataFrame(datos_entrada, schema_entrada)

    # Datos esperados: Top 5 de San Jose y los 3 de Cartago
    schema_esperado = StructType([
        StructField('provincia', StringType()), StructField('cedula', IntegerType()),
        StructField('nombre_completo', StringType()), StructField('total_km', FloatType()),
        StructField('rank', IntegerType())
    ])
    datos_esperados = [
        # Cartago (3 resultados)
        ('Cartago', 202, 'Jose Araya', 120.0, 1),
        ('Cartago', 201, 'Maria Solis', 80.0, 2),
        ('Cartago', 203, 'Lucia Salas', 40.0, 3),
        # San Jose (Top 5 de 6)
        ('San Jose', 104, 'Ana Solano', 300.0, 1),
        ('San Jose', 105, 'David Guzman', 250.0, 2),
        ('San Jose', 102, 'Sofia Rojas', 200.0, 3),
        ('San Jose', 106, 'Elena Vega', 150.0, 4),
        ('San Jose', 101, 'Carlos Mora', 120.0, 5) # 100 + 20
    ]
    df_esperado = spark_session.createDataFrame(datos_esperados, schema_esperado)
    df_resultado = top_5_ciclistas_por_total_km_por_provincia(df_entrada)
    
    df_esperado.show()
    df_resultado.show()

    # Se compara el contenido como conjuntos para ignorar el orden de las filas
    resultado_set = set(map(tuple, df_resultado.collect()))
    esperado_set = set(map(tuple, df_esperado.collect()))

    assert df_resultado.count() == 8 # 5 de San Jose + 3 de Cartago
    assert sorted(df_resultado.columns) == sorted(df_esperado.columns)
    assert resultado_set == esperado_set

def test_top_5_ciclistas_por_promedio_diario_por_provincia(spark_session):
    """
    Prueba unitaria para la función top_5_ciclistas_por_promedio_diario_por_provincia.
    Verifica que el cálculo del promedio diario, agrupación y ranking funcionen.
    """
    # Preparar datos de entrada
    schema_entrada = StructType([
        StructField('codigo_de_ruta', IntegerType()), StructField('cedula', IntegerType()),
        StructField('nombre_completo', StringType()), StructField('provincia', StringType()),
        StructField('fecha_actividad', DateType()), StructField('nombre_ruta', StringType()),
        StructField('distancia_km', FloatType())
    ])
    # Datos diseñados para probar el promedio:
    # - SJ_1: 2 actividades en el mismo día, 1 en otro.
    # - SJ_4: 3 actividades en 3 días distintos.
    datos_entrada = [
        # San Jose
        (1, 101, 'Carlos Mora', 'San Jose', date(2023, 1, 1), 'R1', 100.0), # Día 1
        (1, 101, 'Carlos Mora', 'San Jose', date(2023, 1, 1), 'R1', 20.0),  # Día 1
        (1, 101, 'Carlos Mora', 'San Jose', date(2023, 1, 2), 'R2', 30.0),  # Día 2. Total: 150km / 2 días = 75 avg
        (1, 102, 'Sofia Rojas', 'San Jose', date(2023, 1, 1), 'R1', 200.0), # Total: 200km / 1 día = 200 avg
        (1, 103, 'Luis Castro', 'San Jose', date(2023, 1, 1), 'R1', 50.0),  # Total: 50km / 1 día = 50 avg (no debe aparecer)
        (1, 104, 'Ana Solano', 'San Jose', date(2023, 1, 1), 'R1', 100.0), # Día 1
        (1, 104, 'Ana Solano', 'San Jose', date(2023, 1, 2), 'R2', 100.0), # Día 2
        (1, 104, 'Ana Solano', 'San Jose', date(2023, 1, 3), 'R3', 100.0), # Día 3. Total: 300km / 3 días = 100 avg
        (1, 105, 'David Guzman', 'San Jose', date(2023, 1, 1), 'R1', 250.0), # Total: 250km / 1 día = 250 avg
        (1, 106, 'Elena Vega', 'San Jose', date(2023, 1, 1), 'R1', 150.0), # Total: 150km / 1 día = 150 avg
        # Cartago
        (2, 201, 'Maria Solis', 'Cartago', date(2023, 1, 1), 'R3', 80.0),   # Total: 80km / 1 día = 80 avg
        (2, 202, 'Jose Araya', 'Cartago', date(2023, 1, 1), 'R3', 120.0),  # Total: 120km / 1 día = 120 avg
    ]
    df_entrada = spark_session.createDataFrame(datos_entrada, schema_entrada)
    df_resultado = top_5_ciclistas_por_promedio_diario_por_provincia(df_entrada)

    # Verificar el resultado
    schema_esperado = StructType([
        StructField('provincia', StringType()), StructField('cedula', IntegerType()),
        StructField('nombre_completo', StringType()), StructField('total_km', FloatType()),
        StructField('dias_activos', IntegerType()), StructField('promedio_diario_km', FloatType()),
        StructField('rank', IntegerType())
    ])
    # Datos esperados calculados manualmente con el promedio redondeado a 2 decimales
    datos_esperados = [
        # Cartago (2 resultados)
        ('Cartago', 202, 'Jose Araya', 120.0, 1, 120.00, 1),
        ('Cartago', 201, 'Maria Solis', 80.0, 1, 80.00, 2),
        # San Jose (Top 5 de 6)
        ('San Jose', 105, 'David Guzman', 250.0, 1, 250.00, 1),
        ('San Jose', 102, 'Sofia Rojas', 200.0, 1, 200.00, 2),
        ('San Jose', 106, 'Elena Vega', 150.0, 1, 150.00, 3),
        ('San Jose', 104, 'Ana Solano', 300.0, 3, 100.00, 4),
        ('San Jose', 101, 'Carlos Mora', 150.0, 2, 75.00, 5)
    ]
    df_esperado = spark_session.createDataFrame(datos_esperados, schema_esperado)

    df_esperado.show()
    df_resultado.show()

    # Se compara el contenido como conjuntos para ignorar el orden de las filas
    resultado_set = set(map(tuple, df_resultado.collect()))
    esperado_set = set(map(tuple, df_esperado.collect()))

    assert df_resultado.count() == 7 # 5 de San Jose + 2 de Cartago
    assert sorted(df_resultado.columns) == sorted(df_esperado.columns)
    assert resultado_set == esperado_set
