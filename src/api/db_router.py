"""
Custom database router to handle table naming for Django system tables
"""

class DjangoTableRouter:
    """
    Router to customize Django system table names
    """
    
    def db_for_read(self, model, **hints):
        """Suggest which database to read from"""
        return 'default'
    
    def db_for_write(self, model, **hints):
        """Suggest which database to write to"""
        return 'default'
    
    def allow_relation(self, obj1, obj2, **hints):
        """Allow relations if models are in the same app"""
        return True
    
    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """Allow all migrations"""
        return True