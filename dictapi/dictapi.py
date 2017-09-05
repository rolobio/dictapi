from dictorm import DictDB

def error(msg):
    return {'error':True, 'message':str(msg)}

OK = 200
CREATED = 201
BAD_REQUEST = 400
NOT_FOUND = 404


class APITable(object):

    def __init__(self, api, table):
        self.api = api
        self.table = table


    def GET(self, *a, **kw):
        entry = None
        if not kw and len(a) == len(self.table.pks):
            # Convert positional arguments to keyword arguments if there are the
            # same amount of primary keys.  It is assumed that the arguments are
            # in the same order as the primary keys
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
                    self.api.db_conn.rollback()
                    return (NOT_FOUND, error('No reference found'))
            return (OK, dict(referenced))
        if kw:
            entry = self.table.get_one(**kw)
        if not entry:
            self.api.db_conn.rollback()
            return (NOT_FOUND, error('No entry matching: {}'.format(str(kw))))
        self.api.db_conn.rollback()
        return (OK, dict(entry))


    def PUT(self, **kw):
        wheres = {pk:kw[pk] for pk in self.table.pks if pk in kw}
        code = CREATED
        entry = None
        if wheres:
            # The primary key(s) were specified for the put, overwrite the entry
            entry = self.table.get_one(**wheres)
            if entry:
                # Overwrite an entry
                entry.update(kw)
                entry.flush()
                code = OK
        # No entry was updated, so create a new one with the primary key(s) if
        # they were provided
        if not entry:
            # Insert a new entry
            entry = self.table(**kw).flush()
        self.api.db_conn.commit()
        return (code, dict(entry))


    def DELETE(self, *a):
        if len(a) > len(self.table.pks):
            self.api.db_conn.rollback()
            return (BAD_REQUEST, error('No entry found'))
        a = list(a)
        wheres = {pk:a.pop(0) for pk in self.table.pks}
        entry = self.table.get_one(**wheres)
        if entry:
            # As of writing this code, entry.delete() will always return a None
            result = entry.delete()
            self.api.db_conn.commit()
            return (OK, result)
        self.api.db_conn.rollback()
        return (NOT_FOUND, error('No entry found'))



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



