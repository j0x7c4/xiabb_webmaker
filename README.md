# xiabb_webmaker

## config

add following config to settings.py

```
# encoding=utf-8
mysql_conf = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'port': 3306,
    'database': ''
}

log_conf = {
    "level": "INFO",
    "filename": "logs/run.log",
    "format": "%(asctime)s %(levelname)s: %(message)s"
}
```