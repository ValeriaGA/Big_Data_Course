FROM openjdk:17-alpine

# 1. Instalar dependencias de compilación y del sistema.
RUN apk add --no-cache --virtual .build-deps \
        build-base \
        python3-dev \
        libffi-dev \
        openblas-dev \
        zlib-dev \
        jpeg-dev \
        zeromq-dev \
    && apk add --no-cache \
        bash \
        nano \
        postgresql-client \
        python3 \
        py3-pip

# 2. Actualizar pip e instalar las dependencias de Python.
#    Se usa --ignore-installed para evitar conflictos con los paquetes de Alpine.
RUN pip3 install --no-cache-dir --upgrade pip setuptools wheel \
    && pip3 install --no-cache-dir --ignore-installed \
        numpy \
        matplotlib \
        seaborn \
        pyspark \
        pytest \
        notebook \
        findspark

# 3. Limpiar dependencias de compilación y crear enlace simbólico.
RUN apk del .build-deps \
    && ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /src

# 4. Copiar el resto del código fuente.
#    Esto aprovecha el caché de Docker. Si solo cambias tu código Python,
#    las capas anteriores no se reconstruirán.
COPY . /src
