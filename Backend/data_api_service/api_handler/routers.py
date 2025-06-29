class MultiDBRouter:
    def db_for_read(self, model, **hints):
        if model._meta.app_label == 'api_handler':
            if model._meta.model_name == 'stockdata2':
                return 'secondary_db'
            return 'default'
        return None

    def db_for_write(self, model, **hints):
        if model._meta.app_label == 'api_handler':
            if model._meta.model_name == 'stockdata2':
                return 'secondary_db'
            return 'default'
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        if app_label == 'api_handler':
            if model_name == 'stockdata2':
                return db == 'secondary_db'
            return db == 'default'
        return None