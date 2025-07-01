import mysql.connector
from mysql.connector import Error
import logging
from typing import Optional, List, Dict, Any
from contextlib import contextmanager
from .config import config


class SQLManager:
    """MySQL connection manager for direct database operations"""
    
    def __init__(self):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
    def get_connection_params(self) -> Dict[str, Any]:
        """Get MySQL connection parameters from config"""
        return {
            'host': self.config.mysql_host,
            'port': self.config.mysql_port,
            'database': self.config.mysql_database,
            'user': self.config.mysql_user,
            'password': self.config.mysql_password,
            'charset': 'utf8mb4',
            'autocommit': False
        }
    
    @contextmanager
    def get_connection(self):
        """Context manager for MySQL connections"""
        connection = None
        try:
            connection_params = self.get_connection_params()
            connection = mysql.connector.connect(**connection_params)
            
            if connection.is_connected():
                self.logger.info("Successfully connected to MySQL")
                yield connection
            else:
                raise Error("Failed to connect to MySQL")
                
        except Error as e:
            self.logger.error(f"MySQL connection error: {e}")
            raise
        finally:
            if connection and connection.is_connected():
                connection.close()
                self.logger.info("MySQL connection closed")
    
    def execute_query(self, query: str, params: Optional[tuple] = None, fetch: bool = True) -> Optional[List[Dict]]:
        """Execute a SELECT query and return results"""
        try:
            with self.get_connection() as connection:
                cursor = connection.cursor(dictionary=True)
                cursor.execute(query, params or ())
                
                if fetch:
                    results = cursor.fetchall()
                    self.logger.info(f"Query executed successfully, returned {len(results)} rows")
                    return results
                else:
                    self.logger.info("Query executed successfully")
                    return None
                    
        except Error as e:
            self.logger.error(f"Error executing query: {e}")
            raise
    
    def execute_insert(self, query: str, params: Optional[tuple] = None) -> int:
        """Execute an INSERT query and return the last inserted ID"""
        try:
            with self.get_connection() as connection:
                cursor = connection.cursor()
                cursor.execute(query, params or ())
                connection.commit()
                
                last_id = cursor.lastrowid
                self.logger.info(f"Insert executed successfully, last ID: {last_id}")
                return last_id
                
        except Error as e:
            self.logger.error(f"Error executing insert: {e}")
            raise
    
    def execute_bulk_insert(self, query: str, data: List[tuple]) -> int:
        """Execute bulk INSERT and return number of affected rows"""
        try:
            with self.get_connection() as connection:
                cursor = connection.cursor()
                cursor.executemany(query, data)
                connection.commit()
                
                affected_rows = cursor.rowcount
                self.logger.info(f"Bulk insert executed successfully, {affected_rows} rows affected")
                return affected_rows
                
        except Error as e:
            self.logger.error(f"Error executing bulk insert: {e}")
            raise
    
    def execute_update(self, query: str, params: Optional[tuple] = None) -> int:
        """Execute an UPDATE query and return number of affected rows"""
        try:
            with self.get_connection() as connection:
                cursor = connection.cursor()
                cursor.execute(query, params or ())
                connection.commit()
                
                affected_rows = cursor.rowcount
                self.logger.info(f"Update executed successfully, {affected_rows} rows affected")
                return affected_rows
                
        except Error as e:
            self.logger.error(f"Error executing update: {e}")
            raise
    
    def execute_delete(self, query: str, params: Optional[tuple] = None) -> int:
        """Execute a DELETE query and return number of affected rows"""
        try:
            with self.get_connection() as connection:
                cursor = connection.cursor()
                cursor.execute(query, params or ())
                connection.commit()
                
                affected_rows = cursor.rowcount
                self.logger.info(f"Delete executed successfully, {affected_rows} rows affected")
                return affected_rows
                
        except Error as e:
            self.logger.error(f"Error executing delete: {e}")
            raise
    
    def test_connection(self) -> bool:
        """Test MySQL connection"""
        try:
            with self.get_connection() as connection:
                cursor = connection.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                return result is not None
                
        except Error as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
    
    def get_table_info(self, table_name: str) -> List[Dict]:
        """Get table structure information"""
        query = f"DESCRIBE {table_name}"
        return self.execute_query(query)
    
    def get_table_count(self, table_name: str) -> int:
        """Get row count for a table"""
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        result = self.execute_query(query)
        return result[0]['count'] if result else 0


# Global instance for easy usage
sql_manager = SQLManager()