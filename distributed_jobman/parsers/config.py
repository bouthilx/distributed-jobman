from ConfigParser import ConfigParser
import os

p2_config = ConfigParser()
p2_config.read(os.path.join(os.environ["HOME"], ".distributed_jobman.rc"))

default_values = dict(cache_timeout=str(60 * 5))

keys = ["username", "password", "address", "name", "cache_timeout"]

database = dict()
for key in keys:
    value = p2_config.get("database", key, vars=default_values)

    if value is None:
        raise ValueError("Option %s must be set in configuration file "
                         "~/.distributed_jobman.rc")
    database[key] = value

database["cache_timeout"] = float(database["cache_timeout"])

config = dict(database=database)
