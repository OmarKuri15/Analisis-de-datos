import streamlit as st
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
import zipfile
import os

# Inicializar sesión de Spark
spark = SparkSession.builder.appName("DataAnalysis").getOrCreate()

# Función para cargar los datos
@st.cache_data
def load_data():
    zip_path = "data.zip"
    csv_path = "data.csv"

    # Verificar si el archivo ZIP existe y descomprimirlo
    if os.path.exists(zip_path):
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(".")  # Extrae en el directorio actual

    # Cargar los datos desde el CSV extraído
    if os.path.exists(csv_path):
        df = spark.read.csv(csv_path, header=True, inferSchema=True)
        return df.toPandas()
    else:
        st.error("No se encontró el archivo data.csv después de descomprimir.")
        return pd.DataFrame()

# Cargar datos
data = load_data()

# Configurar la interfaz de Streamlit
st.title("Dashboard de Análisis de Datos")
st.write("Este dashboard analiza datos públicos utilizando PySpark y Streamlit.")

# Mostrar datos
if not data.empty:
    if st.checkbox("Mostrar datos crudos"):
        st.dataframe(data.head())

    # Gráfica de ejemplo
    st.subheader("Distribución de una columna")
    selected_column = st.selectbox("Selecciona una columna para analizar", data.columns)
    st.bar_chart(data[selected_column].value_counts())
else:
    st.warning("No hay datos disponibles para mostrar.")

# Finalizar sesión de Spark
spark.stop()