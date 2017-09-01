from dictorm import DictDB

def error(msg):
    return {'error':True, 'message':str(msg)}


class APITable(object):

    exposed = True

    def __init__(self, api, table):
        self.api = api
        self.table = table


    def PUT(self, **kw):
        wheres = {pk:kw[pk] for pk in self.table.pks if pk in kw}
        if wheres:
            # Overwrite an entry
            entry = self.table.get_one(**wheres)
            if not entry:
                entry = self.table(**kw).flush()
            else:
                entry.update(kw)
                entry.flush()
        else:
            # Insert a new entry
            entry = self.table(**kw).flush()
        self.api.db_conn.commit()
        return dict(entry)


    def GET(self, *a, **kw):
        entry = None
        if not kw and len(a) == len(self.table.pks):
            # Convert positional arguments to keyword arguments if there are the
            # same amount of primary keys
            kw = dict(zip(self.table.pks, a))
        elif not kw and len(a) > len(self.table.pks):
            a = list(a)
            # Requesting a reference/substratum, get the primary keys for this
            # table
            wheres = {pk:a.pop(0) for pk in self.table.pks}
            # The object that contains references
            referenced = self.table.get_one(**wheres)
            # Keep moving down the object until the last reference is gotten
            while a:
                try:
                    referenced = referenced[a.pop(0)]
                except KeyError:
                    # Bad reference was passed
                    return error('No reference found')
            return dict(referenced)
        if kw:
            entry = self.table.get_one(**kw)
        if not entry:
            return error('No entry matching: {}'.format(str(kw)))
        self.api.db_conn.rollback()
        return dict(entry)



class API(object):

    def __init__(self, db_conn):
        self.db_conn = db_conn
        self.dictdb = DictDB(db_conn)
        self.init_tables()


    def init_tables(self):
        for table_name in self.dictdb:
            table = self.dictdb[table_name]
            apitable = self.table_factory()
            setattr(self, table_name, apitable(self, table))


    @classmethod
    def table_factory(cls): return APITable



