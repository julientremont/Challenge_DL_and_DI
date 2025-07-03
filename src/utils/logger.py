import sqlite3
import datetime
import os
import sys
import traceback
import inspect
from functools import wraps
from typing import Any, Callable, Optional
import threading

class DataProcessingLogger:
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, db_path: str = "silver_logs.db", date_format: str = "%Y-%m-%d %H:%M:%S"):
        """Singleton pattern pour éviter les conflits"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance.initialized = False
        return cls._instance
    
    def __init__(self, db_path: str = "silver_logs.db", date_format: str = "%Y-%m-%d %H:%M:%S"):
        if not self.initialized:
            self.db_path = db_path
            self.date_format = date_format
            self.init_database()
            self.initialized = True
    
    def init_database(self):
        """Crée la table de logs si elle n'existe pas"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS execution_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                script_name TEXT NOT NULL,
                input_lines INTEGER,
                output_lines INTEGER,
                start_datetime TEXT NOT NULL,
                end_datetime TEXT,
                execution_time_seconds REAL,
                status TEXT DEFAULT 'RUNNING',
                error_message TEXT,
                input_source TEXT,
                output_destination TEXT,
                additional_info TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def _get_caller_info(self):
        """Récupère automatiquement les infos du script appelant"""
        frame = inspect.currentframe()
        try:
            # Remonter dans la stack pour trouver le vrai appelant
            caller_frame = frame.f_back.f_back.f_back
            if caller_frame is None:
                caller_frame = frame.f_back.f_back
            
            script_path = caller_frame.f_code.co_filename
            script_name = os.path.basename(script_path)
            
            return script_name, script_path
        finally:
            del frame
    
    def _format_datetime(self, dt: datetime.datetime) -> str:
        """Formate la date selon le format spécifié"""
        return dt.strftime(self.date_format)
    
    def start_execution(self, 
                       input_lines: int = None,
                       script_name: str = None,
                       input_source: str = None,
                       additional_info: str = None) -> int:
        """
        Démarre l'enregistrement d'une exécution
        
        Args:
            input_lines: Nombre de lignes en entrée
            script_name: Nom du script (auto-détecté si None)
            input_source: Source des données (fichier, table, etc.)
            additional_info: Informations supplémentaires
        """
        if script_name is None:
            script_name, _ = self._get_caller_info()
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO execution_logs 
            (script_name, input_lines, start_datetime, status, input_source, additional_info)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            script_name,
            input_lines,
            self._format_datetime(datetime.datetime.now()),
            'RUNNING',
            input_source,
            additional_info
        ))
        
        log_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return log_id
    
    def end_execution(self, 
                     log_id: int, 
                     output_lines: int = None,
                     status: str = 'SUCCESS', 
                     error_message: str = None,
                     output_destination: str = None):
        """
        Termine l'enregistrement d'une exécution
        
        Args:
            log_id: ID du log à mettre à jour
            output_lines: Nombre de lignes en sortie
            status: Statut final ('SUCCESS', 'ERROR', 'CANCELLED')
            error_message: Message d'erreur le cas échéant
            output_destination: Destination des données (fichier, table, etc.)
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Récupérer la date de début pour calculer le temps d'exécution
        cursor.execute('SELECT start_datetime FROM execution_logs WHERE id = ?', (log_id,))
        result = cursor.fetchone()
        
        if result:
            start_time = datetime.datetime.strptime(result[0], self.date_format)
            end_time = datetime.datetime.now()
            execution_time = (end_time - start_time).total_seconds()
        else:
            end_time = datetime.datetime.now()
            execution_time = None
        
        cursor.execute('''
            UPDATE execution_logs 
            SET output_lines = ?, 
                end_datetime = ?, 
                execution_time_seconds = ?,
                status = ?,
                error_message = ?,
                output_destination = ?
            WHERE id = ?
        ''', (
            output_lines,
            self._format_datetime(end_time),
            execution_time,
            status,
            error_message,
            output_destination,
            log_id
        ))
        
        conn.commit()
        conn.close()
    
    def log_data_processing(self, 
                           input_lines: int = None,
                           input_source: str = None,
                           script_name: str = None,
                           additional_info: str = None):
        """
        Context manager pour logger automatiquement un traitement de données
        
        Usage:
            with logger.log_data_processing(
                input_lines=1000, 
                input_source="fichier.csv"
            ) as log_context:
                # Votre traitement ici
                result = process_data()
                log_context.set_output(output_lines=800, output_destination="table_silver")
        """
        class DataProcessingContext:
            def __init__(self, logger, input_lines, input_source, script_name, additional_info):
                self.logger = logger
                self.input_lines = input_lines
                self.input_source = input_source
                self.script_name = script_name
                self.additional_info = additional_info
                self.log_id = None
                self.output_lines = None
                self.output_destination = None
                
            def __enter__(self):
                self.log_id = self.logger.start_execution(
                    self.input_lines, 
                    self.script_name,
                    self.input_source,
                    self.additional_info
                )
                return self
                
            def set_output(self, output_lines: int, output_destination: str = None):
                """Définit les paramètres de sortie"""
                self.output_lines = output_lines
                self.output_destination = output_destination
                
            def __exit__(self, exc_type, exc_val, exc_tb):
                if exc_type is not None:
                    # Une erreur s'est produite
                    error_msg = f"{exc_type.__name__}: {exc_val}"
                    self.logger.end_execution(
                        self.log_id,
                        self.output_lines,
                        'ERROR',
                        error_msg,
                        self.output_destination
                    )
                else:
                    # Succès
                    self.logger.end_execution(
                        self.log_id,
                        self.output_lines,
                        'SUCCESS',
                        None,
                        self.output_destination
                    )
        
        return DataProcessingContext(self, input_lines, input_source, script_name, additional_info)
    
    def get_logs(self, script_name: str = None, limit: int = 100) -> list:
        """Récupère les logs d'exécution"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if script_name:
            cursor.execute('''
                SELECT * FROM execution_logs 
                WHERE script_name = ? 
                ORDER BY created_at DESC 
                LIMIT ?
            ''', (script_name, limit))
        else:
            cursor.execute('''
                SELECT * FROM execution_logs 
                ORDER BY created_at DESC 
                LIMIT ?
            ''', (limit,))
        
        logs = cursor.fetchall()
        conn.close()
        
        return logs
    
    def get_summary_stats(self, script_name: str = None):
        """Récupère des statistiques résumées"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        where_clause = "WHERE script_name = ?" if script_name else ""
        params = (script_name,) if script_name else ()
        
        cursor.execute(f'''
            SELECT 
                COUNT(*) as total_executions,
                SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_executions,
                SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) as failed_executions,
                AVG(execution_time_seconds) as avg_execution_time,
                SUM(input_lines) as total_input_lines,
                SUM(output_lines) as total_output_lines,
                AVG(input_lines) as avg_input_lines,
                AVG(output_lines) as avg_output_lines
            FROM execution_logs 
            {where_clause}
            AND status != 'RUNNING'
        ''', params)
        
        stats = cursor.fetchone()
        conn.close()
        
        return {
            'total_executions': stats[0],
            'successful_executions': stats[1],
            'failed_executions': stats[2],
            'avg_execution_time': stats[3],
            'total_input_lines': stats[4],
            'total_output_lines': stats[5],
            'avg_input_lines': stats[6],
            'avg_output_lines': stats[7]
        }


# Décorateur pour logger automatiquement les fonctions de traitement
def log_data_function(input_lines_param: str = None, 
                     output_lines_param: str = None,
                     input_source_param: str = None,
                     output_destination_param: str = None,
                     logger_instance=None):
    """
    Décorateur pour logger automatiquement l'exécution d'une fonction de traitement
    
    Args:
        input_lines_param: Nom du paramètre contenant le nombre de lignes d'entrée
        output_lines_param: Nom du paramètre de retour contenant le nombre de lignes de sortie
        input_source_param: Nom du paramètre contenant la source des données
        output_destination_param: Nom du paramètre contenant la destination des données
        logger_instance: Instance du logger (si None, utilise l'instance globale)
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = logger_instance or DataProcessingLogger()
            
            # Extraire les paramètres
            input_lines = None
            input_source = None
            
            if input_lines_param and input_lines_param in kwargs:
                input_lines = kwargs[input_lines_param]
            if input_source_param and input_source_param in kwargs:
                input_source = kwargs[input_source_param]
            
            # Démarrer le log
            log_id = logger.start_execution(
                input_lines=input_lines,
                script_name=f"{func.__module__}.{func.__name__}",
                input_source=input_source
            )
            
            try:
                # Exécuter la fonction
                result = func(*args, **kwargs)
                
                # Extraire les paramètres de sortie
                output_lines = None
                output_destination = None
                
                if isinstance(result, dict):
                    if output_lines_param and output_lines_param in result:
                        output_lines = result[output_lines_param]
                    if output_destination_param and output_destination_param in result:
                        output_destination = result[output_destination_param]
                
                # Terminer le log avec succès
                logger.end_execution(
                    log_id, 
                    output_lines=output_lines,
                    status='SUCCESS',
                    output_destination=output_destination
                )
                
                return result
                
            except Exception as e:
                # Terminer le log avec erreur
                logger.end_execution(
                    log_id, 
                    status='ERROR',
                    error_message=f"{type(e).__name__}: {str(e)}"
                )
                raise
        
        return wrapper
    return decorator


# Instance globale avec format de date personnalisable
def create_logger(db_path: str = "silver_logs.db", date_format: str = "%Y-%m-%d %H:%M:%S"):
    """Crée une instance de logger avec un format de date spécifique"""
    return DataProcessingLogger(db_path, date_format)

# Instance par défaut
logger = DataProcessingLogger()

# Fonctions utilitaires globales
def start_data_log(input_lines: int = None, 
                   script_name: str = None,
                   input_source: str = None,
                   additional_info: str = None) -> int:
    """Fonction globale pour démarrer un log de traitement de données"""
    return logger.start_execution(input_lines, script_name, input_source, additional_info)

def end_data_log(log_id: int, 
                 output_lines: int = None,
                 status: str = 'SUCCESS', 
                 error_message: str = None,
                 output_destination: str = None):
    """Fonction globale pour terminer un log de traitement de données"""
    logger.end_execution(log_id, output_lines, status, error_message, output_destination)

def log_data_processing(input_lines: int = None,
                       input_source: str = None,
                       script_name: str = None,
                       additional_info: str = None):
    """Fonction globale pour le context manager de traitement de données"""
    return logger.log_data_processing(input_lines, input_source, script_name, additional_info)


# Exemples d'utilisation
if __name__ == "__main__":
    print("=== Test du logger de traitement de données ===")
    
    # Exemple 1: Logger avec format de date personnalisé
    custom_logger = create_logger(date_format="%d/%m/%Y %H:%M:%S")
    
    # Exemple 2: Utilisation simple
    print("\n1. Utilisation simple:")
    log_id = start_data_log(
        input_lines=1500,
        input_source="fichier_source.csv",
        additional_info="Traitement des commandes"
    )
    
    import time
    time.sleep(1)
    
    end_data_log(
        log_id,
        output_lines=1200,
        output_destination="table_silver.commandes"
    )
    
    # Exemple 3: Context manager
    print("\n2. Context manager:")
    with log_data_processing(
        input_lines=2000,
        input_source="raw_data.json",
        additional_info="Transformation ETL"
    ) as log_context:
        time.sleep(0.5)
        # Simulation du traitement
        processed_lines = 1800
        log_context.set_output(
            output_lines=processed_lines,
            output_destination="silver_table.processed_data"
        )
    
    # Exemple 4: Décorateur
    print("\n3. Décorateur:")
    
    @log_data_function(
        input_lines_param='nb_lignes_entree',
        output_lines_param='nb_lignes_sortie',
        input_source_param='fichier_source',
        output_destination_param='table_destination'
    )
    def traiter_donnees(nb_lignes_entree, fichier_source):
        time.sleep(0.3)
        # Simulation du traitement
        nb_lignes_sortie = int(nb_lignes_entree * 0.85)  # 15% de lignes filtrées
        return {
            'nb_lignes_sortie': nb_lignes_sortie,
            'table_destination': 'silver.donnees_traitees',
            'status': 'success'
        }
    
    result = traiter_donnees(
        nb_lignes_entree=3000,
        fichier_source="data_input.csv"
    )
    
    # Statistiques
    print("\n=== Statistiques ===")
    stats = logger.get_summary_stats()
    print(f"Total exécutions: {stats['total_executions']}")
    print(f"Succès: {stats['successful_executions']}")
    print(f"Erreurs: {stats['failed_executions']}")
    print(f"Temps moyen: {stats['avg_execution_time']:.2f}s")
    print(f"Lignes totales traitées: {stats['total_input_lines']} → {stats['total_output_lines']}")
    
    # Logs récents
    print("\n=== Logs récents ===")
    logs = logger.get_logs(limit=5)
    for log in logs:
        print(f"Script: {log[1]}, In: {log[2]}, Out: {log[3]}, Durée: {log[6]}s, Status: {log[7]}")