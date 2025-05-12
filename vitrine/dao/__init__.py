import os

import psycopg2
import psycopg2.extras

# Garantir que ele consiga converter UUID's
psycopg2.extras.register_uuid()


class Connection:
    def __init__(
        self,
        database: str = os.getenv("VITRINE_DATABASE", "vitrine"),
        user: str = os.getenv("VITRINE_USER", "postgres"),
        host: str = os.getenv("VITRINE_HOST", "localhost"),
        password: str = os.getenv("VITRINE_PASSWORD", "postgres"),
        port: str = os.getenv("VITRINE_PORT", "5432"),
    ):
        self.database = database
        self.user = user
        self.host = host
        self.password = password
        self.port = port

    def __connect(self):
        try:
            connection = psycopg2.connect(
                database=self.database,
                user=self.user,
                host=self.host,
                password=self.password,
                port=self.port,
            )
            return connection, connection.cursor()
        except psycopg2.Error as e:
            print("Erro ao conectar ao banco de dados:", e)
            return None, None

    def __close(self, connection, cursor):
        if cursor:
            cursor.close()
        if connection:
            connection.close()

    def select(self, script_sql, params=None):
        connection, cursor = self.__connect()
        query = []
        if not connection or not cursor:
            return query
        try:
            cursor.execute(script_sql, params)
            query = cursor.fetchall()
        except psycopg2.errors.InvalidTextRepresentation as e:
            print(f"[Erro] Possivelmente de Djavan: {e.pgcode}")
        finally:
            self.__close(connection, cursor)
        return query

    def exec(self, script_sql: str, params=None):
        connection, cursor = self.__connect()
        if not connection or not cursor:
            return
        try:
            cursor.execute(script_sql, params)
            connection.commit()
        except psycopg2.errors.UniqueViolation:
            raise psycopg2.errors.UniqueViolation
        except (Exception, psycopg2.DatabaseError) as e:
            connection.rollback()
            print(f"[Erro]\n\n{e}")
            raise psycopg2.errors.UniqueViolation
        finally:
            self.__close(connection, cursor)

    def execmany(self, script_sql: str, params=None):
        connection, cursor = self.__connect()
        if not connection or not cursor:
            return
        try:
            cursor.executemany(script_sql, params)
            connection.commit()
        except psycopg2.errors.UniqueViolation:
            raise psycopg2.errors.UniqueViolation
        except (Exception, psycopg2.DatabaseError) as e:
            connection.rollback()
            print(f"[Erro]\n\n{e}")
        finally:
            self.__close(connection, cursor)
