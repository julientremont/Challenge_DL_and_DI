import mysql.connector
from mysql.connector import Error, PoolError
from mysql.connector.pooling import MySQLConnectionPool
import logging
import time
from typing import Optional, List, Dict, Any
from contextlib import contextmanager
from .config import config


class SQLManager:
    """Enhanced MySQL connection manager with connection pooling and retry logic"""
    
    def __init__(self):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.connection_pool = None
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialize MySQL connection pool"""
        try:
            pool_config = self.get_connection_params()
            self.connection_pool = MySQLConnectionPool(**pool_config)
            self.logger.info("MySQL connection pool initialized successfully")
        except Error as e:
            self.logger.warning(f"Failed to initialize connection pool: {e}")
            self.connection_pool = None
        
    def get_connection_params(self) -> Dict[str, Any]:
        """Get MySQL connection parameters with enhanced timeout and stability settings"""
        return {
            'host': self.config.mysql_host,
            'port': self.config.mysql_port,
            'database': self.config.mysql_database,
            'user': self.config.mysql_user,
            'password': self.config.mysql_password,
            'charset': 'utf8mb4',
            'autocommit': False,
            # Enhanced connection settings for stability
            'connection_timeout': 60,  # Connection timeout in seconds
            'use_unicode': True,
            'sql_mode': 'TRADITIONAL',
            'raise_on_warnings': False,
            # Connection pool settings
            'pool_name': 'data_pipeline_pool',
            'pool_size': 10,
            'pool_reset_session': True,
            # Keep-alive settings
            'autocommit': False,
            'time_zone': '+00:00'
        }
    
    @contextmanager
    def get_connection(self, max_retries: int = 3, retry_delay: float = 1.0):
        """Enhanced context manager for MySQL connections with retry logic and pooling"""
        connection = None
        attempt = 0
        
        while attempt <= max_retries:
            try:
                # Try to get connection from pool first
                if self.connection_pool:
                    try:
                        connection = self.connection_pool.get_connection()
                        self.logger.debug("Got connection from pool")
                    except PoolError:
                        self.logger.warning("Pool exhausted, creating direct connection")
                        connection = None
                
                # Fallback to direct connection
                if connection is None:
                    connection_params = self.get_connection_params()
                    # Remove pool-specific parameters for direct connection
                    direct_params = {k: v for k, v in connection_params.items() 
                                   if k not in ['pool_name', 'pool_size', 'pool_reset_session']}
                    connection = mysql.connector.connect(**direct_params)
                
                # Test connection
                if connection and connection.is_connected():
                    # Ping to ensure connection is alive
                    connection.ping(reconnect=True, attempts=3, delay=1)
                    self.logger.debug("Successfully connected to MySQL")
                    yield connection
                    break
                else:
                    raise Error("Failed to establish MySQL connection")
                    
            except Error as e:
                attempt += 1
                if attempt <= max_retries:
                    self.logger.warning(f"MySQL connection attempt {attempt} failed: {e}. Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    self.logger.error(f"MySQL connection failed after {max_retries} attempts: {e}")
                    raise
            finally:
                if connection and connection.is_connected():
                    connection.close()
                    self.logger.debug("MySQL connection closed")
    
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
    
    def execute_bulk_insert(self, query: str, data: List[tuple], batch_size: int = 1000) -> int:
        """Execute bulk INSERT with batching for large datasets"""
        total_affected = 0
        
        try:
            # Process data in batches to avoid memory issues and connection timeouts
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                
                with self.get_connection() as connection:
                    cursor = connection.cursor()
                    cursor.executemany(query, batch)
                    connection.commit()
                    
                    affected_rows = cursor.rowcount
                    total_affected += affected_rows
                    self.logger.info(f"Batch {i//batch_size + 1}: {affected_rows} rows inserted")
            
            self.logger.info(f"Bulk insert completed successfully, {total_affected} total rows affected")
            return total_affected
                
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
    
    def health_check(self) -> Dict[str, Any]:
        """Comprehensive health check for MySQL connection"""
        health_status = {
            'connected': False,
            'pool_available': False,
            'server_version': None,
            'current_database': None,
            'connection_count': 0,
            'errors': []
        }
        
        try:
            with self.get_connection() as connection:
                cursor = connection.cursor(dictionary=True)
                
                # Basic connection test
                cursor.execute("SELECT 1 as test")
                health_status['connected'] = True
                
                # Get server version
                cursor.execute("SELECT VERSION() as version")
                result = cursor.fetchone()
                health_status['server_version'] = result['version'] if result else 'Unknown'
                
                # Get current database
                cursor.execute("SELECT DATABASE() as db")
                result = cursor.fetchone()
                health_status['current_database'] = result['db'] if result else 'None'
                
                # Check connection pool status
                if self.connection_pool:
                    health_status['pool_available'] = True
                
                self.logger.info("MySQL health check completed successfully")
                
        except Error as e:
            health_status['errors'].append(str(e))
            self.logger.error(f"MySQL health check failed: {e}")
        
        return health_status
    
    def reset_pool(self):
        """Reset the connection pool"""
        try:
            if self.connection_pool:
                # Note: mysql-connector-python doesn't have a direct reset method
                # So we reinitialize the pool
                self._initialize_pool()
                self.logger.info("Connection pool reset successfully")
        except Exception as e:
            self.logger.error(f"Error resetting connection pool: {e}")


# Global instance for easy usage
sql_manager = SQLManager()