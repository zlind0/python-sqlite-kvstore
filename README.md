# Python KV Local Store using SQLite

Using SQLite to simplify local data storage.

usage:

```
from sqlitekv import SQLiteKV

with SQLiteKV("default") as kvs:
    print(kvs["mundo"])
    # None

    print(kvs.getorputdefault("mundo", "Bonjour"))
    # Bonjour

    print(kvs["mundo"])
    # Bonjour

    kvs["mundo"]="ola"
    print(kvs["mundo"])
    # ola

    print(kvs.keys())
    # ["mundo"]

```