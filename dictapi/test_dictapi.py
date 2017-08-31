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


    def db_reset(self):
        self.conn.rollback()
        self.curs.execute(DB_RESET)


    def assertDictContains(cls, a, b):
        if not set(b.items()).issubset(set(a.items())):
            raise TypeError('Dict is missing items {}'.format(
                str(dict(set(b.items()).difference(set(a.items()))))))




class TestAPI(BaseTest):

    def test_put(self):
        john = self.api.person.PUT(name='John')
        self.assertDictContains(john, {'name':'John', 'id':1})

        two = self.api.person.PUT(id=2)
        self.assertDictContains(two, {'id':2})



