import unittest
import requests


class TestRequest(unittest.TestCase):

    SITE = 'http://localhost:5000'

    def test_correct_import(self):
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

        r = requests.post(self.SITE + '/imports',
                          json={'citizens': data_list})
        self.assertEqual(r.status_code, 201)


if __name__ == '__main__':
    unittest.main()
