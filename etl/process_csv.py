import time
import psycopg2
from pyspark.sql import SparkSession

def wait_for_postgres(host, port, retries=10, delay=5):
    attempt = 0
    while attempt < retries:
        try:
            # Tentative de connexion à PostgreSQL
            conn = psycopg2.connect(host=host, port=port, user="postgres", password="password", dbname="full_data_db")
            conn.close()
            print("PostgreSQL est prêt !")
            return True
        except psycopg2.OperationalError as e:
            print(f"Connexion à PostgreSQL échouée : {e}. Réessai {attempt + 1}/{retries}")
            attempt += 1
            time.sleep(delay)
    raise Exception("Impossible de se connecter à PostgreSQL après plusieurs tentatives.")

print('Starting ETL PROCESS ON CSV!')

# Attendre que PostgreSQL soit prêt
wait_for_postgres(host="postgres", port=5432)

# Créer une session Spark
spark = SparkSession.builder\
        .appName("CSV_PROCESSING")\
        .getOrCreate()

# Chemins des fichiers CSV
consultations_file_path = "../data/Consultations.csv"
assurance_maladie_file_path = "../data/Assurance_Maladie.csv"
hopitaux_file_path = "../data/Hopitaux.csv"
indicateurs_sante_file_path = "../data/Indicateurs_Sante.csv"
patients_file_path = "../data/Patients.csv"
medecins_file_path = "../data/Medecins.csv"
prescriptions_file_path = "../data/Prescriptions.csv"

# Lire les fichiers CSV
df_consultations = spark.read.csv(consultations_file_path, header=True, inferSchema=True)
df_assurance_maladie = spark.read.csv(assurance_maladie_file_path, header=True, inferSchema=True)
df_hopitaux = spark.read.csv(hopitaux_file_path, header=True, inferSchema=True)
df_indicateurs_sante = spark.read.csv(indicateurs_sante_file_path, header=True, inferSchema=True)
df_patients = spark.read.csv(patients_file_path, header=True, inferSchema=True)
df_medecins = spark.read.csv(medecins_file_path, header=True, inferSchema=True)
df_prescriptions = spark.read.csv(prescriptions_file_path, header=True, inferSchema=True)

# Afficher les données pour vérifier le chargement
# df_consultations.show(5)
# df_assurance_maladie.show(5)
# df_hopitaux.show(5)
# df_indicateurs_sante.show(5)
# df_patients.show(5)
# df_medecins.show(5)
# df_prescriptions.show(5)

# Définir les paramètres de connexion JDBC
jdbc_url = "jdbc:postgresql://postgres:5432/full_data_db"
jdbc_properties = {
    "user": "postgres",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

# Écrire les données dans la base de données PostgreSQL
df_consultations.write.jdbc(url=jdbc_url, table="consultations", mode="overwrite", properties=jdbc_properties)
df_assurance_maladie.write.jdbc(url=jdbc_url, table="assurance_maladie", mode="overwrite", properties=jdbc_properties)
df_hopitaux.write.jdbc(url=jdbc_url, table="hopitaux", mode="overwrite", properties=jdbc_properties)
df_indicateurs_sante.write.jdbc(url=jdbc_url, table="indicateurs_sante", mode="overwrite", properties=jdbc_properties)
df_patients.write.jdbc(url=jdbc_url, table="patients", mode="overwrite", properties=jdbc_properties)
df_medecins.write.jdbc(url=jdbc_url, table="medecins", mode="overwrite", properties=jdbc_properties)
df_prescriptions.write.jdbc(url=jdbc_url, table="prescriptions", mode="overwrite", properties=jdbc_properties)

print('ETL PROCESS COMPLETED!')

# Arrêter la session Spark
spark.stop()
