
from dynaconf import Dynaconf

cfg = Dynaconf(
    settings_files=['settings.toml', '.secrets.toml'],

)
