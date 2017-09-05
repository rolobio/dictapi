from dictapi.dictapi import API
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
CREATE TABLE person (
    id SERIAL PRIMARY KEY,
    name TEXT,
    manager_id INTEGER REFERENCES person(id)
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

DB_RESET = '''
DELETE FROM person_department;
DELETE FROM person;
DELETE FROM department;
ALTER SEQUENCE person_id_seq RESTART WITH 1;
ALTER SEQUENCE department_id_seq RESTART WITH 1;
'''

class BaseTest(unittest.TestCase):

    def setUp(self):
        self.conn = psycopg2.connect(**test_db_login)
        self.curs = self.conn.cursor()
        try:
            self.curs.execute(DB_SCHEMA)
        except psycopg2.IntegrityError:
            # Tables already exist, reset them
            self.db_reset()
        except psycopg2.ProgrammingError:
            # Tables already exist, reset them
            self.db_reset()
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


    def db_reset(self):
        self.conn.rollback()
        self.curs.execute(DB_RESET)


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


    def test_reference(self):
        _, jake = self.api.person.PUT(name='Jake')
        _, sales = self.api.department.PUT(name='Sales')
        _, jake_dept = self.api.person_department.PUT(
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



