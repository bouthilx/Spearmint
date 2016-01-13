import importlib


default_database_type = "mongodb"
default_database_address = "localhost"
default_database = dict(type=default_database_type,
                        address=default_database_address)


def database_factory(options):
    database_options = default_database.copy()
    if "database" in options:
        database_options.update(options["database"])

    db = importlib.import_module(
        'spearmint.utils.database.%s'
        % database_options["type"]).init(database_options)

    return db
