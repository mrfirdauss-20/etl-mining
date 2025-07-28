from abc import ABC, abstractmethod
import mysql.connector

class IDatabaseConnector(ABC):
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def execute_query(self, query: str):
        pass

    @abstractmethod
    def close(self):
        pass


class MySQLAdapter(IDatabaseConnector):
    def __init__(self, config: dict):
        self.config = config
        self.connection = None
        self.cursor = None

    def connect(self):
        if self.connection is None:
            self.connection = mysql.connector.connect(**self.config)
            self.cursor = self.connection.cursor()
            print("MySQLAdapter: Connection established")
        return self.connection

    def execute_query(self, query: str):
        if self.cursor is None:
            raise Exception("Connection is not established. Call connect() first.")
        self.cursor.execute(query)
        if query.strip().lower().startswith("select"):
            columns = [desc[0] for desc in self.cursor.description] 
            rows = self.cursor.fetchall()
            results = [dict(zip(columns, row)) for row in rows]
            return results
        else:
            self.connection.commit()
            return f"Query executed: {query}"

    def close(self):
        if self.cursor:
            self.cursor.close()
            print("MySQLAdapter: Cursor closed")
        if self.connection:
            self.connection.close()
            print("MySQLAdapter: Connection closed")
        self.cursor = None
        self.connection = None