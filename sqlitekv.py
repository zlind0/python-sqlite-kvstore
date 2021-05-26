import sqlite3

class SQLiteKVCached:
    con=None
    cache={}
    cachesize=1000
    cacheused=1
    def __init__(self, namespace, cachesize=1000, in_memory=False):
        self.con=sqlite3.connect(":memory:" if in_memory else namespace+".db")
        self.con.execute("CREATE TABLE IF NOT EXISTS kvtable (Key TEXT UNIQUE, Value TEXT)")
        self.con.commit()
        self.cachesize=cachesize

    def put(self, Key, Value):
        # self.cacheused+=1
        # self.cache[Key]=Value
        # if self.cacheused==self.cachesize:
            self.con.executemany("INSERT OR REPLACE INTO kvtable VALUES(?,?)", self.cache.items())
            self.cache.clear()
            self.cacheused=1
    
    def get(self, Key):
        # if Key in self.cache:
        #     return str(self.cache[Key])
        cur = self.con.cursor()
        cur.execute("SELECT Value FROM kvtable WHERE Key=?", (Key,))
        res=cur.fetchone()
        if res is None: return None
        else: return res[0]

    def delete(self, Key):
        self.con.execute("DELETE FROM kvtable WHERE Key=?", (Key, ))

    def deleteall(self):
        self.con.execute("DROP TABLE kvtable")
        self.con.execute("CREATE TABLE IF NOT EXISTS kvtable (Key TEXT UNIQUE, Value TEXT)")

    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        self.con.executemany("INSERT OR REPLACE INTO kvtable VALUES(?,?)", self.cache.items())
        self.con.commit()
        self.con.close()

class SQLiteKV:
    con=None
    cache={}
    cachesize=1000
    cacheused=1
    def __init__(self, namespace, cachesize=1000, in_memory=False):
        self.con=sqlite3.connect(":memory:" if in_memory else namespace+".db")
        self.con.execute("CREATE TABLE IF NOT EXISTS kvtable (Key TEXT UNIQUE, Value TEXT)")
        self.con.commit()
        self.cachesize=cachesize

    def put(self, Key, Value):
        self.con.execute("INSERT OR REPLACE INTO kvtable VALUES(?,?)", (Key, Value))
    
    def get(self, Key):
        cur = self.con.cursor()
        cur.execute("SELECT Value FROM kvtable WHERE Key=?", (Key,))
        res=cur.fetchone()
        if res is None: return None
        else: return res[0]

    def delete(self, Key):
        self.con.execute("DELETE FROM kvtable WHERE Key=?", (Key, ))

    def deleteall(self):
        self.con.execute("DROP TABLE kvtable")
        self.con.execute("CREATE TABLE IF NOT EXISTS kvtable (Key TEXT UNIQUE, Value TEXT)")

    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        self.con.executemany("INSERT OR REPLACE INTO kvtable VALUES(?,?)", self.cache.items())
        self.con.commit()
        self.con.close()

class SQLiteKVCompare:
    con=None
    def __init__(self, namespace):
        self.con={}

    def put(self, Key, Value):
        self.con[Key]=Value
    
    def get(self, Key):
        return self.con[Key]

    def delete(self, Key):
        self.con.execute("DELETE FROM kvtable WHERE Key=?", (Key, ))

    def deleteall(self):
        self.con.execute("DROP TABLE kvtable")
        self.con.execute("CREATE TABLE IF NOT EXISTS kvtable (Key TEXT UNIQUE, Value TEXT)")

    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        return self