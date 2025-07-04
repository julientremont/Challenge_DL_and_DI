"""
Database router for Analysis app to use the gold database.
"""


class AnalysisDBRouter:
    """
    A router to control all database operations on models for the analysis app
    to use the gold database.
    """
    
    route_app_labels = {'analysis'}

    def db_for_read(self, model, **hints):
        """Suggest the database for reads of analysis app models."""
        if model._meta.app_label == 'analysis':
            return 'gold'
        return None

    def db_for_write(self, model, **hints):
        """Suggest the database for writes of analysis app models."""
        if model._meta.app_label == 'analysis':
            return 'gold'
        return None

    def allow_relation(self, obj1, obj2, **hints):
        """Allow relations between analysis models."""
        # Allow relations within analysis app using gold DB
        if obj1._meta.app_label == 'analysis' and obj2._meta.app_label == 'analysis':
            return True
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """Ensure that the analysis app's models get created on the gold database."""
        if app_label == 'analysis':
            return db == 'gold'
        elif db == 'gold':
            # Only analysis app models should be in gold DB
            return app_label == 'analysis'
        return None