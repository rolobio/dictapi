from dictapi.dictapi import API, COLLECTION_SIZE, NoRead, NoWrite, LastModified
from functools import partial
import os
import psycopg2
import requests
import unittest


if 'CI' in os.environ.keys():
    test_db_login = {
            'database':'dictorm',
            'user':'postgres',
            'password':'',
            'host':'localhost',
            'port':'5432',
            }
else:
    test_db_login = {
            'database':'dictorm',
            'user':'dictorm',
            'password':'dictorm',
            'host':'localhost',
            'port':'5432',
            }


DB_SCHEMA = '''
DROP TABLE IF EXISTS person_department CASCADE;
DROP TABLE IF EXISTS person CASCADE;
DROP TABLE IF EXISTS department CASCADE;

CREATE TABLE person (
    id SERIAL PRIMARY KEY,
    name TEXT,
    manager_id INTEGER REFERENCES person(id),
    password_hash TEXT,
    last_modified TIMESTAMP DEFAULT current_timestamp
);
CREATE TABLE department (
    id SERIAL PRIMARY KEY,
    name TEXT
);
CREATE TABLE person_department (
    person_id INTEGER REFERENCES person(id),
    department_id INTEGER REFERENCES department(id),
    PRIMARY KEY (person_id, department_id)
);
'''

class BaseTest(unittest.TestCase):

    def setUp(self):
        self.conn = psycopg2.connect(**test_db_login)
        self.curs = self.conn.cursor()
        self.curs.execute(DB_SCHEMA)
        self.conn.commit()
        self.api = API(self.conn)


    def tearDown(self):
        try:
            # If this command succeeds, then all errors were rolled back
            self.curs.execute('''select count(*) from pg_locks''')
            if self.curs.fetchall()[0][0] > 2:
                raise AssertionError('Uncommited changes in test')
        except psycopg2.InternalError:
            raise AssertionError('Transaction errors not rolled-back')
        finally:
            self.conn.rollback()
        self.conn.close()


    @classmethod
    def assertDictContains(cls, a, b):
        missing = dict(set(b.items()).difference(set(a.items())))
        if missing:
            raise TypeError('Dict is missing items {}'.format(str(missing)))


    def assertError(self, expected_code, response):
        code, entry = response
        self.assertEqual(code, expected_code)
        self.assertIn('error', entry)


    def assertResponse(self, expected_code, response, expected_entry):
        code, entry = response
        self.assertEqual(code, expected_code)
        if expected_entry != None:
            self.assertDictContains(entry, expected_entry)
        else:
            # Response is expected to be None
            self.assertEqual(entry, None)



class TestAPI(BaseTest):

    def test_put(self):
        # A new person can be PUT
        response = self.api.person.PUT(name='Jake')
        self.assertResponse(201, response,
                {'name':'Jake', 'id':1})

        # Another person with no name can be PUT
        response = self.api.person.PUT(id=2)
        self.assertResponse(201, response,
                {'id':2})

        # You can overwrite an entry using its primary keys
        _, jake = response
        jake['name'] = 'Phil'
        response = self.api.person.PUT(**jake)

        self.assertResponse(200, response,
                {'id':2, 'name':'Phil'})


    def test_invalid_id(self):
        """
        PUTing and invalid ID is handled
        """
        error = self.api.person.PUT(id='foo')
        self.assertError(400, error)
        error = self.api.person.GET(id='foo')
        self.assertError(400, error)
        error = self.api.person.DELETE(id='foo')
        self.assertError(400, error)


    def test_get(self):
        # Insert a person directly using DictORM
        jake1 = self.api.dictdb['person'](name='Jake').flush()

        # The inserted person can be gotten
        _, jake2 = self.api.person.GET(name='Jake')
        self.assertEqual(jake1, jake2)


    def test_get_pagination(self):
        # Range greater than any entry returns a 404
        code, error = self.api.person.GET_RANGE('40-60')
        self.assertEqual(404, code)

        # End is higher than the start
        code, error = self.api.person.GET_RANGE('50-40')
        self.assertEqual(400, code)

        # Too many values
        code, error = self.api.person.GET_RANGE('1-2-3')
        self.assertEqual(400, code)

        names = ('Jake', 'Phil', 'Bob', 'Steve', 'Alice', 'Frank')*4
        for name in names:
            self.api.dictdb['person'](name=name).flush()
        self.conn.commit()

        # Attempt to get bad range
        error = self.api.person.GET_RANGE('foo')
        self.assertError(400, error)

        # Get all Persons inserted
        code, persons = self.api.person.GET_RANGE(None)
        self.assertEqual(200, code)
        self.assertEqual(len(persons), COLLECTION_SIZE)
        last_id = 0
        # Check that the people are as expected
        for person, name in zip(persons, names):
            self.assertEqual(last_id+1, person['id'])
            last_id = person['id']
            self.assertDictContains(person, {'name':name})

        # Get next batch of people
        code, persons = self.api.person.GET_RANGE('21-40')
        self.assertEqual(200, code)
        self.assertEqual(len(persons), 4)
        for person, name in zip(persons, names[-4:]):
            self.assertDictContains(person, {'name':name})

        # It is possible to request entries greater than the start
        code, persons = self.api.person.GET_RANGE('1-')
        self.assertEqual(code, 200, msg=persons)
        self.assertEqual(len(persons), COLLECTION_SIZE)
        last_id = 0
        for person, name in zip(persons, names):
            self.assertEqual(last_id+1, person['id'])
            last_id = person['id']
            self.assertDictContains(person, {'name':name})


    def test_reference(self):
        _, jake = self.api.person.PUT(name='Jake')
        _, sales = self.api.department.PUT(name='Sales')
        _, jake_sales = self.api.person_department.PUT(
                person_id=jake['id'],
                department_id=sales['id'])

        # Department has not yet been defined
        error = self.api.person.GET(1, 'department')
        self.assertError(400, error)

        # department is referenced through person_department
        Person, Department = self.api.person.table, self.api.department.table
        PD = self.api.person_department.table
        Person['person_department'] = Person['id'] == PD['person_id']
        PD['department'] = PD['department_id'] == Department['id']
        Person['department'] = Person['person_department'].substratum(
                'department')

        _, sales2 = self.api.person.GET(1, 'department')
        self.assertEqual(sales, sales2)


    def test_reference_delete(self):
        _, jake = self.api.person.PUT(name='Jake')
        _, sales = self.api.department.PUT(name='Sales')
        _, jake_sales = self.api.person_department.PUT(
                person_id=jake['id'],
                department_id=sales['id'])

        # Can't delete sales
        error = self.api.department.DELETE(1)
        self.assertError(400, error)



    def test_delete(self):
        # Deleting non-existant entry
        error = self.api.person.DELETE(1)
        self.assertError(404, error)

        jake = self.api.person.PUT(name='Jake')
        response = self.api.person.DELETE(1)
        self.assertResponse(200, response, None)

        # Jake was already deleted
        error = self.api.person.DELETE(1)
        self.assertError(404, error)

        # Changes survive a rollback
        self.conn.rollback()
        error = self.api.person.DELETE(1)
        self.assertError(404, error)

        # Bad Request
        response = self.api.person.DELETE(1,2)
        self.assertError(400, response)

        # Deletion by keyword
        jake = self.api.person.PUT(name='Jake')
        response = self.api.person.DELETE(id=2)
        self.assertResponse(200, response, None)


    def test_head(self):
        # HEADing non-existant entry
        code, entry = self.api.person.HEAD(1)
        self.assertEqual(404, code)
        self.assertEqual(entry, None)

        jake = self.api.person.PUT(name='Jake')
        response = self.api.person.HEAD(1)
        self.assertResponse(200, response, None)


    def test_modify(self):
        _, frank = self.api.person.PUT(name='Frank')
        self.assertIn('password_hash', frank)

        # stop password_hash from being read
        self.api.person.PUT.modify(NoRead, 'password_hash')
        _, jake = self.api.person.PUT(name='Jake')
        self.assertNotIn('password_hash', jake)

        # password_hash can still be written to
        _, john = self.api.person.PUT(name='John', password_hash='foo')
        self.assertNotIn('password_hash', john)
        john = self.api.dictdb['person'].get_one(name='John')
        self.assertEqual(john['password_hash'], 'foo')

        # stop password_hash from being written to
        self.api.person.PUT.modify(NoWrite, 'password_hash')
        error = self.api.person.PUT(name='Alice', password_hash='foo')
        self.assertError(400, error)

        # name is also removed
        self.api.person.PUT.modify(NoRead, 'name')
        _, steve = self.api.person.PUT(name='Steve')
        self.assertDictContains(steve, {'id':4})
        self.assertNotIn('name', steve)


    def test_last_modified(self):
        self.api.person.PUT.modify(LastModified, 'last_modified')
        _, jake = self.api.person.PUT(name='Jake')
        self.assertNotEqual(jake['last_modified'], None)

        # Update Jake after time has passed
        from time import sleep
        sleep(0.1)
        _, jake2 = self.api.person.PUT(id=1)
        self.assertEqual(jake['id'], jake2['id'])
        # Jake's last_modified date is now updated
        self.assertGreater(jake2['last_modified'], jake['last_modified'])


    def test_idempotent(self):
        from datetime import date

        # PUT in a person, their last_modified should be the current datetime
        _, jake = self.api.person.PUT(name='Jake')
        self.assertGreater(jake['last_modified'].date(), date.min)

        def FakeLastModified(call, column_name, *a, **kw):
            code, result = call(*a, **kw)
            # Attempt to change the column, this should be rolled back
            result[column_name] = date.min
            result.flush()
            return (code, result)

        self.api.person.GET.modify(FakeLastModified, 'last_modified')

        # The inserted person can be gotten
        _, jake2 = self.api.person.GET(1)
        self.assertEqual(jake['id'], jake2['id'])
        self.assertEqual(jake['name'], jake2['name'])
        self.assertNotEqual(jake2['last_modified'], date.min)




