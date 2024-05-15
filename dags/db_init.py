import psycopg2
from sqlalchemy import create_engine

class Database: 
    """Classe pour interagir avec une base de données PostgreSQL."""
    def __init__(self):
        """Initialise une connexion à la base de données."""
        db_params = {
            "dbname": "evalmlops",
            "user": "adminmlops",
            "password": "User1234",
            "host": "mlopsadmin.postgres.database.azure.com",
            "port": "5432",
            "sslmode": "require",
        }

        self.connection = psycopg2.connect(**db_params)
        self.engine = create_engine(
            f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
        )

    def insert(self, insert_query):
        """Exécute une requête d'insertion dans la base de données.

        Args:
            insert_query (str): La requête d'insertion SQL.

        """
        cursor = self.connection.cursor()

        cursor.execute(insert_query)
        self.connection.commit()
        cursor.close()

    def execute(self, query):
        """Exécute une requête SQL dans la base de données.

        Args:
            query (str): La requête SQL à exécuter.

        """

        cursor = self.connection.cursor()

        cursor.execute(query)
        self.connection.commit()
        cursor.close()

    def close(self):
        """Ferme la connexion à la base de données."""
        self.connection.close()
        self.engine.dispose()
