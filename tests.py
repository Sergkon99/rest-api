# -*- coding: utf-8 -*-
import unittest
import requests
# import json


class TestException(Exception):
    pass


class TestRequest(unittest.TestCase):

    SITE = 'http://127.0.0.1:5000'
    import_id = None
    data_list = [{
        "citizen_id": 1,
        "town": "Москва",
        "street": "Льва Толстого",
        "building": "16к7стр5",
        "apartment": 7,
        "name": "Иванов Иван Иванович",
        "birth_date": "26.12.1986",
        "gender": "male",
        "relatives": [2]
    }, {
        "citizen_id": 2,
        "town": "Москва",
        "street": "Льва Толстого",
        "building": "16к7стр5",
        "apartment": 7,
        "name": "Иванов Сергей Иванович",
        "birth_date": "17.04.1997",
        "gender": "male",
        "relatives": [1]
    }, {
        "citizen_id": 3,
        "town": "Керчь",
        "street": "Иосифа Бродского",
        "building": "2",
        "apartment": 11,
        "name": "Романова Мария Леонидовна",
        "birth_date": "23.11.1986",
        "gender": "female",
        "relatives": []
    }]
    data_list_en = [{
        "citizen_id": 1,
        "town": "town",
        "street": "street",
        "building": "building",
        "apartment": 7,
        "name": "name",
        "birth_date": "26.12.1986",
        "gender": "male",
        "relatives": []
    }]

    def setUp(self):
        try:
            r = requests.post(self.SITE + '/imports',
                              json={'citizens': self.data_list_en})
            self.import_id = r.json()['data']['import_id']
        except Exception as ex:
            raise TestException(str(ex))

    def test_correct_import(self):
        r = requests.post(self.SITE + '/imports',
                          json={'citizens': self.data_list})
        self.assertEqual(r.status_code, 201)
        # self.assertEqual(r.json()['data']['import_id'], self.import_id + 1)

    def test_correct_get(self):
        self.assertIsNotNone(self.import_id)
        q = self.SITE + '/imports/{id}/citizens'.format(id=str(self.import_id))
        print(q)
        r = requests.get(q)
        self.assertEqual(r.status_code, 200)
        r = r.json()
        for i, citizen in enumerate(r['data']):
            self.assertEqual(citizen, self.data_list_en[i])


if __name__ == '__main__':
    unittest.main()
