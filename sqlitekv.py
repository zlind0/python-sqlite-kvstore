import sqlite3, re

class SQLiteKV:
    con=None
    def __init__(self, namespace, in_memory=False):
        self.con=sqlite3.connect(":memory:" if in_memory else namespace+".db")
        self.con.execute("CREATE TABLE IF NOT EXISTS kvtable (Key TEXT UNIQUE, Value TEXT)")
        self.con.commit()

    def __getitem__(self, Key):
        cur = self.con.cursor()
        cur.execute("SELECT Value FROM kvtable WHERE Key=?", (Key,))
        res=cur.fetchone()
        if res is None: return None
        else: return res[0]
    
    def __setitem__(self, Key, Value):
        self.con.execute("INSERT OR REPLACE INTO kvtable VALUES(?,?)", (Key, Value))

    def __len__(self):
        cur = self.con.cursor()
        cur.execute("SELECT COUNT(*) FROM kvtable")
        res=cur.fetchone()
        return res[0]
    
    def get(self, Key):
        cur = self.con.cursor()
        cur.execute("SELECT Value FROM kvtable WHERE Key=?", (Key,))
        res=cur.fetchone()
        if res is None: return None
        else: return res[0]
    
    def put(self, Key, Value):
        self.con.execute("INSERT OR REPLACE INTO kvtable VALUES(?,?)", (Key, Value))

    def delete(self, Key):
        self.con.execute("DELETE FROM kvtable WHERE Key=?", (Key, ))

    def clear(self):
        self.con.execute("DROP TABLE kvtable")
        self.con.commit()
        self.con.execute("CREATE TABLE IF NOT EXISTS kvtable (Key TEXT UNIQUE, Value TEXT)")

    def keys(self):
        cur = self.con.cursor()
        cur.execute("SELECT Key FROM kvtable")
        return (i[0] for i in cur)

    def items(self):
        cur = self.con.cursor()
        cur.execute("SELECT Key, Value FROM kvtable")
        return (i for i in cur)
    
    def getorputdefault(self, Key, Value):
        res = self[Key]
        if res is None: 
            self[Key]=Value
            return Value

    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        self.con.commit()
        self.con.close()

class SQLiteDict:
    con=None
    def __init__(self, namespace, in_memory=False):
        self.con=sqlite3.connect(":memory:" if in_memory else namespace+".db")
        self.con.row_factory=sqlite3.Row
        self.con.execute("CREATE TABLE IF NOT EXISTS dicttable (Key TEXT UNIQUE)")
        self.con.commit()
    def put(self, key, value):
        tablename="dicttable"
        quoted_values={"Key":key}
        for k in value.keys():
            quotedk="\"" + k.replace("\"", "\"\"") + "\""
            quoted_values[quotedk]=value[k]

        keys=quoted_values.keys()

        stmt="INSERT OR REPLACE INTO %s(%s) VALUES(%s)"%\
            (tablename, ','.join(keys), ','.join('?'*len(keys)))
        valueslist=[quoted_values[k] for k in keys]
        while True:
            try:
                self.con.execute(stmt, valueslist)
                break
            except sqlite3.OperationalError as e:
                errmsg=str(e)
                if 'has no column named' in errmsg:
                    missingcolname=re.findall(r'has no column named (.*)',errmsg)[0]
                    missingcolname="\"" + missingcolname.replace("\"", "\"\"") + "\""
                    if isinstance(quoted_values[missingcolname], int):
                        self.con.execute(f'ALTER TABLE {tablename} ADD COLUMN {missingcolname} INTEGER')
                    elif isinstance(quoted_values[missingcolname], float):
                        self.con.execute(f'ALTER TABLE {tablename} ADD COLUMN {missingcolname} REAL')
                    else:
                        self.con.execute(f'ALTER TABLE {tablename} ADD COLUMN {missingcolname} TEXT')
                    self.con.commit()
                    print("[Info] Adding missing column", missingcolname)
                else:
                    print("[Error]", errmsg)
    def get(self, key, ret_dict=False):
        tablename="dicttable"
        cur = self.con.cursor()
        cur.execute(f"SELECT * FROM {tablename} WHERE Key=?", (key,))
        res=cur.fetchone()
        if res is None: return None
        elif ret_dict: 
            res=dict(res)
            del res["Key"]
            return res
        else: 
            return res

    def setattr_dict(self, key, attr_dict):
        tablename="dicttable"
        quoted_values={}
        for k in attr_dict.keys():
            quotedk="\"" + k.replace("\"", "\"\"") + "\""
            quoted_values[quotedk]=attr_dict[k]
        keys=quoted_values.keys()
        stmt="UPDATE %s set %s WHERE Key=?"%(tablename, ",".join([f"{i}=?" for i in keys]))
        stmtvalues=[quoted_values[k] for k in keys]+[key]
        print(stmt, stmtvalues)
        self.con.execute(stmt, stmtvalues)
    
    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        return self.put(key, value)

    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        self.con.commit()
        self.con.close()

# class SQLiteKVCompare:
#     con=None
#     def __init__(self, namespace):
#         self.con={}

#     def put(self, Key, Value):
#         self.con[Key]=Value
    
#     def get(self, Key):
#         return self.con[Key]

#     def delete(self, Key):
#         self.con.execute("DELETE FROM kvtable WHERE Key=?", (Key, ))

#     def deleteall(self):
#         self.con.execute("DROP TABLE kvtable")
#         self.con.execute("CREATE TABLE IF NOT EXISTS kvtable (Key TEXT UNIQUE, Value TEXT)")

#     def __enter__(self):
#         return self
#     def __exit__(self, exc_type, exc_value, traceback):
#         return self

# class SQLiteKVCached:
#     con=None
#     cache={}
#     cachesize=1000
#     cacheused=1
#     def __init__(self, namespace, cachesize=1000, in_memory=False):
#         self.con=sqlite3.connect(":memory:" if in_memory else namespace+".db")
#         self.con.execute("CREATE TABLE IF NOT EXISTS kvtable (Key TEXT UNIQUE, Value TEXT)")
#         self.con.commit()
#         self.cachesize=cachesize

#     def put(self, Key, Value):
#         # self.cacheused+=1
#         # self.cache[Key]=Value
#         # if self.cacheused==self.cachesize:
#             self.con.executemany("INSERT OR REPLACE INTO kvtable VALUES(?,?)", self.cache.items())
#             self.cache.clear()
#             self.cacheused=1
    
#     def get(self, Key):
#         # if Key in self.cache:
#         #     return str(self.cache[Key])
#         cur = self.con.cursor()
#         cur.execute("SELECT Value FROM kvtable WHERE Key=?", (Key,))
#         res=cur.fetchone()
#         if res is None: return None
#         else: return res[0]

#     def delete(self, Key):
#         self.con.execute("DELETE FROM kvtable WHERE Key=?", (Key, ))

#     def deleteall(self):
#         self.con.execute("DROP TABLE kvtable")
#         self.con.execute("CREATE TABLE IF NOT EXISTS kvtable (Key TEXT UNIQUE, Value TEXT)")

#     def __enter__(self):
#         return self
#     def __exit__(self, exc_type, exc_value, traceback):
#         self.con.executemany("INSERT OR REPLACE INTO kvtable VALUES(?,?)", self.cache.items())
#         self.con.commit()
#         self.con.close()