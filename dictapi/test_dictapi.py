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
        self.conn.close()


    def db_reset(self):
        self.conn.rollback()
        self.curs.execute(DB_RESET)


    def assertDictContains(cls, a, b):
        if not set(b.items()).issubset(set(a.items())):
            raise TypeError('Dict is missing items {}'.format(
                str(dict(set(b.items()).difference(set(a.items()))))))




class TestAPI(BaseTest):

    def test_put(self):
        # A new person can be PUT
        jake = self.api.person.PUT(name='Jake')
        self.assertDictContains(jake, {'name':'Jake', 'id':1})

        # Another person with no name can be PUT
        two = self.api.person.PUT(id=2)
        self.assertDictContains(two, {'id':2})

        # You can overwrite an entry using its primary keys
        two['name'] = 'Phil'
        phil = self.api.person.PUT(**two)

        self.assertDictContains(phil,
                {'id':2, 'name':'Phil'})


    def test_get(self):
        # Insert a person directly using DictORM
        jake1 = self.api.dictdb['person'](name='Jake').flush()

        # The inserted person can be gotten
        jake2 = self.api.person.GET(name='Jake')
        self.assertEqual(jake1, jake2)


    def test_reference(self):
        jake = self.api.person.PUT(name='Jake')
        sales = self.api.department.PUT(name='Sales')
        jake_dept = self.api.person_department.PUT(
                person_id=jake['id'],
                department_id=sales['id'])

        # Department has not yet been defined
        error = self.api.person.GET(1, 'department')
        self.assertIn('error', error)

        # department is referenced through person_department
        Person, Department = self.api.person.table, self.api.department.table
        PD = self.api.person_department.table
        Person['person_department'] = Person['id'] == PD['person_id']
        PD['department'] = PD['department_id'] == Department['id']
        Person['department'] = Person['person_department'].substratum(
                'department')

        sales2 = self.api.person.GET(1, 'department')
        self.assertEqual(sales, sales2)



