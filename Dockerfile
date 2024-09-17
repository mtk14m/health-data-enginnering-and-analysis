# Utiliser l'image Spark de Bitnami
FROM bitnami/spark:latest

USER root

# Créer un répertoire pour les données dans le conteneur sous /opt
RUN mkdir -p /opt/data /opt/driver

# Installer wget pour télécharger le driver JDBC
RUN apt-get update && apt-get install -y wget

# Télécharger et copier le driver JDBC PostgreSQL
RUN wget -O /opt/driver/postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

RUN pip install psycopg2-binary

# Copier les fichiers CSV depuis votre machine locale vers le conteneur
COPY ./data /opt/data

# Copier les scripts Python dans le conteneur
COPY ./etl /opt/etl

# Définir le répertoire de travail dans le conteneur
WORKDIR /opt/etl

# Commande par défaut pour exécuter Spark avec votre script
CMD ["bash", "-c", "spark-submit --master local --jars /opt/driver/postgresql-42.6.0.jar process_csv.py"]
