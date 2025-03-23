import streamlit as st
import pandas as pd
import pyspark
from  pyspark.sql import SparkSession

# Inicializar sesión de Spark
spark = SparkSession.builder.appName("DataAnalysis").getOrCreate()

# Cargar datos desde Kaggle (ejemplo con CSV local para prueba)
@st.cache_data
def load_data():
    df = spark.read.csv("data.csv", header=True, inferSchema=True)
    return df.toPandas()

data = load_data()

# Configurar la interfaz de Streamlit
st.title("Dashboard de Análisis de Datos")
st.write("Este dashboard analiza datos públicos utilizando PySpark y Streamlit.")

# Mostrar datos
if st.checkbox("Mostrar datos crudos"):
    st.dataframe(data.head())

# Gráfica de ejemplo
st.subheader("Distribución de una columna")
selected_column = st.selectbox("Selecciona una columna para analizar", data.columns)
st.bar_chart(data[selected_column].value_counts())

# Finalizar sesión de Spark
spark.stop()
