import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg

# Opcional: redirigir carpeta temporal para evitar usar AppData
os.environ["SPARK_LOCAL_DIRS"] = "C:/SparkTemp"

# Crear SparkSession con configuración de MongoDB
spark = SparkSession.builder \
    .appName("MongoDBConnection") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/BDproyectoFinal.BDproyectoFinal") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/BDproyectoFinal.BDproyectoFinal") \
    .getOrCreate()

# Leer datos desde MongoDB
df = spark.read.format("mongo").load()

# Mostrar el esquema
df.printSchema()

# Mostrar los primeros datos
df.show(5)

# CONSULTAS

# 1. Total de migrantes por año
print("1. Total de migrantes por año:")
df.groupBy("Año").agg(sum("Total").alias("TotalMigrantes")).orderBy("Año").show()

# 2. Top 5 nacionalidades con más migrantes en total
print("2. Top 5 nacionalidades con más migrantes:")
df.groupBy("Nacionalidad").agg(sum("Total").alias("TotalMigrantes")).orderBy("TotalMigrantes", ascending=False).limit(5).show()

# 3. Número de migrantes por género en 2022
print("3. Migrantes por género en 2022:")
df.filter(df["Año"] == 2022).agg(
    sum("Femenino").alias("TotalFemenino"),
    sum("Masculino").alias("TotalMasculino"),
    sum("Indefinido").alias("TotalIndefinido")
).show()

# 4. Evolución mensual de migrantes en 2023
print("4. Evolución mensual en 2023:")
df.filter(df["Año"] == 2023).groupBy("Mes").agg(sum("Total").alias("TotalMensual")).orderBy("Mes").show()

# 5. Migrantes por país en 2022
print("5. Migrantes por país en 2022:")
df.filter(df["Año"] == 2022).groupBy("Nacionalidad").agg(sum("Total").alias("Total2022")).orderBy("Total2022", ascending=False).show()

# 6. Promedio de migrantes por registro
print("6. Promedio de migrantes por registro:")
df.select(avg("Total").alias("PromedioMigrantes")).show()

# 7. País con mayor número de migrantes femeninas
print("7. País con más migrantes femeninas:")
df.groupBy("Nacionalidad").agg(sum("Femenino").alias("TotalFemenino")).orderBy("TotalFemenino", ascending=False).limit(1).show()

# 8. Cantidad de registros con género indefinido mayor a 0
print("8. Registros con género indefinido > 0:")
print(df.filter(df["Indefinido"] > 0).count())

# 9. Latitudes y longitudes más frecuentes
print("9. Coordenadas más frecuentes:")
df.groupBy("Latitud - Longitud").count().orderBy("count", ascending=False).show(5)

# 10. Número total de migrantes en el dataset
print("10. Total global de migrantes en el dataset:")
df.agg(sum("Total").alias("TotalGlobal")).show()
