# -*- coding: utf-8 -*-
from flask import Flask, jsonify, request, make_response, abort
from flask import render_template, json
from functools import wraps
from datetime import date
from dateutil.relativedelta import relativedelta
import math

import config
from dbconnection import DBConnection, DBConnectionError,\
                         CredentialsError, SQLError
from logger import LogMsg
from checker import *

app = Flask(__name__)


@app.route('/')
def index():
    return 'It is work'


def init():
    LogMsg('Начальная инициализация')
    success = False
    try:
        with DBConnection(config.dbconfig) as cursor:
            query = """
            SELECT
                MAX(`import_id`)
            FROM
                `citizens`
            """
            cursor.execute(query)
            res = cursor.fetchone()[0]
            config.import_id = res if res else 0
        success = True
    except DBConnectionError as err:
        LogMsg('Ошибка подключения к базе данных ' + str(err))
    except CredentialsError as err:
        LogMsg('Неверные имя/пароль ' + str(err))
    except SQLError as err:
        LogMsg('Ошибка SQL запроса ' + str(err))
    except Exception as err:
        LogMsg('Неизвестная ошибка ' + str(err))
    finally:
        if not success:
            abort(400)


@app.route('/imports', methods=['POST'])
def import_data():
    LogMsg('[Method import_data] start')
    if not request.json or 'citizens' not in request.json:
        LogMsg('Ошибка в запросе: нет citizens или запрос пуст')
        abort(400)

    config.import_id += 1
    success = False
    try:
        with DBConnection(config.dbconfig) as cursor:
            query = """
                INSERT INTO `citizens`(
                    `citizen_id`,
                    `town`,
                    `street`,
                    `building`,
                    `name`,
                    `apartment`,
                    `birth_date`,
                    `gender`,
                    `relatives`,
                    `import_id`
                )
                VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            relatives = {}
            for citizen in request.json['citizens']:
                if not check_fields(citizen):
                    abort(400)
                _birth_date = parse_date(citizen['birth_date'])
                if not _birth_date:
                    abort(400)
                if not relatives.get(citizen['citizen_id']):
                    relatives[citizen['citizen_id']] = citizen['relatives']
                else:
                    LogMsg('Два человека с однаковыми id')
                    abort(400)

                args = (
                    citizen['citizen_id'],
                    citizen['town'],
                    citizen['street'],
                    citizen['building'],
                    citizen['name'],
                    citizen['apartment'],
                    _birth_date,
                    citizen['gender'],
                    ';'.join(map(str, citizen['relatives'])),
                    config.import_id
                    )
                cursor.execute(query, args)
        success = True
    except DBConnectionError as err:
        LogMsg('Ошибка подключения к базе данных ' + str(err))
    except CredentialsError as err:
        LogMsg('Неверные имя/пароль ' + str(err))
    except SQLError as err:
        LogMsg('Ошибка SQL запроса ' + str(err))
    except Exception as err:
        LogMsg('Неизвестная ошибка ' + str(err))
    finally:
        if not success:
            abort(400)
    if not check_relatives(relatives):
        abort(400)
    LogMsg('[Method import_data] end')
    return jsonify({'data': {'import_id': config.import_id}}), 201


@app.route('/imports/<int:import_id>/citizens', methods=['GET'])
def get_data(import_id, citizen_id=None):
    LogMsg('[Method get_data] start whit import_id = ', import_id)
    if import_id > config.import_id:
        LogMsg('Некорректный import_id: ', import_id)
        abort(400)

    citizens = []
    success = False
    try:
        with DBConnection(config.dbconfig) as cursor:
            query = """
                SELECT
                    `citizen_id`,
                    `town`,
                    `street`,
                    `building`,
                    `name`,
                    `apartment`,
                    `birth_date`,
                    `gender`,
                    `relatives`
                FROM
                    `citizens`
                WHERE
                    `import_id` = %s
                    {where}
            """
            if citizen_id:
                query = query.format(where="AND `citizen_id` = %s")
                args = (import_id, citizen_id)
            else:
                query = query.format(where="")
                args = (import_id, )
            cursor.execute(query, args)

            for citizen in cursor.fetchall():
                citizens.append({
                    'citizen_id': citizen[0],
                    'town': citizen[1],
                    'street': citizen[2],
                    'building': citizen[3],
                    'name': citizen[4],
                    'apartment': citizen[5],
                    'birth_date': citizen[6].strftime('%d.%m.%Y'),
                    'gender': citizen[7],
                    'relatives': [int(i)
                                  for i in citizen[8].strip().split(';')
                                  if i]
                })
        success = True
    except DBConnectionError as err:
        LogMsg('Ошибка подключения к базе данных ' + str(err))
    except CredentialsError as err:
        LogMsg('Неверные имя/пароль ' + str(err))
    except SQLError as err:
        LogMsg('Ошибка SQL запроса ' + str(err))
    except Exception as err:
        LogMsg('Неизвестная ошибка ' + str(err))
    finally:
        if not success:
            abort(400)

    LogMsg('[Method get_data] end')
    return make_response(jsonify({'data': citizens}), 200)


@app.route('/imports/<int:import_id>/citizens/<int:citizen_id>',
           methods=['PATCH'])
def update(import_id, citizen_id):
    LogMsg('[Method update] start')
    if import_id > config.import_id:
        LogMsg('Некорректный import_id: ', import_id)
        abort(400)
    if not request.json:
        LogMsg('Ошибка в запросе: запрос пуст')
        abort(400)
    data = request.json
    if not check_fields_for_update(data):
        LogMsg('Ошибка обновления данных:\
есть поле null или нет полей для изменения')
        abort(400)
    new_birth_date = None
    if data.get('birth_date'):
        new_birth_date = parse_date(data['birth_date'])
        if not new_birth_date:
            abort(400)
    success = False
    try:
        with DBConnection(config.dbconfig) as cursor:  # TODO нужны изменения!
            set_ = ""
            args = []
            for field in data:
                if field in ('town', 'street', 'building', 'name',
                             'gender', 'apartment'):
                    set_ += ' `{field}` = %s ,'.format(field=field)
                    args.append(data[field])
            if new_birth_date:
                set_ += ' `birth_date` = %s,'
                args.append(new_birth_date)
            if set_:
                set_ = set_[0:-1]  # удаляем последнюю запятую
                query = """
                    UPDATE `citizens`
                    SET
                        {set_}
                    WHERE
                        `import_id` = %s AND
                        `citizen_id` = %s
                """.format(set_=set_)
                args.append(import_id)
                args.append(citizen_id)
                cursor.execute(query, args)
        success = True
    except DBConnectionError as err:
        LogMsg('Ошибка подключения к базе данных ' + str(err))
    except CredentialsError as err:
        LogMsg('Неверные имя/пароль ' + str(err))
    except SQLError as err:
        LogMsg('Ошибка SQL запроса ' + str(err))
    except Exception as err:
        LogMsg('Неизвестная ошибка ' + str(err))
    finally:
        if not success:
            abort(400)
    if 'relatives' in data:
        update_relatives(import_id, citizen_id, data['relatives'])
    res = get_data(import_id, citizen_id)
    # Достаем тело ответа
    res = res.json
    LogMsg(res)
    LogMsg('[Method update] end')
    # Возвращаем первое поле из ответа
    return jsonify({'data': res['data'][0]})


def change_relative(import_id, citizen_id, relative, add=True):
    """Добавит/Удалит родственника с ид relative к citizen_id"""
    LogMsg('[change_relative] start')
    if import_id > config.import_id:
        LogMsg('Некорректный import_id: ', import_id)
        abort(400)
    query_select = """
        SELECT
            `relatives`
        FROM
            `citizens`
        WHERE
            `import_id` = %s AND
            `citizen_id` = %s
    """
    query_set = """
        UPDATE
            `citizens`
        SET
            `relatives` = %s
        WHERE
            `import_id` = %s AND
            `citizen_id` = %s
    """
    cur_relatives = []  # Список родственников у citizen_id
    success = False  # Признак успешности запроса
    try:
        with DBConnection(config.dbconfig) as cursor:
            cursor.execute(query_select, (import_id, citizen_id))
            res = cursor.fetchone()[0]
        success = True
    except DBConnectionError as err:
        LogMsg('Ошибка подключения к базе данных ' + str(err))
    except CredentialsError as err:
        LogMsg('Неверные имя/пароль ' + str(err))
    except SQLError as err:
        LogMsg('Ошибка SQL запроса ' + str(err))
    except Exception as err:
        LogMsg('Неизвестная ошибка ' + str(err))
    finally:
        if not success:
            abort(400)

    cur_relatives = [int(i) for i in res.split(';') if i]
    LogMsg("cur relatives", cur_relatives)
    if add:
        if relative not in cur_relatives:
            cur_relatives.append(relative)
    else:
        if relative in cur_relatives:
            cur_relatives.remove(relative)
    new_relatives = ';'.join(map(str, cur_relatives))

    try:
        with DBConnection(config.dbconfig) as cursor:
            cursor.execute(query_set, (new_relatives, import_id, citizen_id))
        success = True
    except DBConnectionError as err:
        LogMsg('Ошибка подключения к базе данных ' + str(err))
    except CredentialsError as err:
        LogMsg('Неверные имя/пароль ' + str(err))
    except SQLError as err:
        LogMsg('Ошибка SQL запроса ' + str(err))
    except Exception as err:
        LogMsg('Неизвестная ошибка ' + str(err))
    finally:
        if not success:
            abort(400)
    LogMsg('[change_relative] end')


def add_relative(import_id, citizen_id, relative):
    """Добавит родственника с ид relative к citizen_id"""
    change_relative(import_id, citizen_id, relative)


def del_relative(import_id, citizen_id, relative):
    """Удалит родственника с ид relative у citizen_id"""
    change_relative(import_id, citizen_id, relative, add=False)


def update_relatives(import_id, citizen_id, new_relatives: list):
    if import_id > config.import_id:
        LogMsg('Некорректный import_id: ', import_id)
        abort(400)
    LogMsg('[update_relatives] start')
    if citizen_id in new_relatives:
        LogMsg("Родственник сам себе")
        abort(400)
    query = """
        SELECT
            `citizen_id`
        FROM
            `citizens`
        WHERE
            `import_id` = %s
    """
    citizen_ids = []
    success = False  # Признак успешности запроса
    try:
        with DBConnection(config.dbconfig) as cursor:
            cursor.execute(query, (import_id, ))
            res = cursor.fetchall()
        success = True
    except DBConnectionError as err:
        LogMsg('Ошибка подключения к базе данных ' + str(err))
    except CredentialsError as err:
        LogMsg('Неверные имя/пароль ' + str(err))
    except SQLError as err:
        LogMsg('Ошибка SQL запроса ' + str(err))
    except Exception as err:
        LogMsg('Неизвестная ошибка ' + str(err))
    finally:
        if not success:
            abort(400)
    for id_ in res:
        citizen_ids.append(id_[0])
    for relative in new_relatives:
        if relative not in citizen_ids:
            LogMsg("Некоректный ид родственника", relative)
            abort(400)
    query_select = """
        SELECT
            `relatives`
        FROM
            `citizens`
        WHERE
            `import_id` = %s AND
            `citizen_id` = %s
    """
    cur_relatives = []  # Список родственников у citizen_id
    success = False  # Признак успешности запроса
    try:
        with DBConnection(config.dbconfig) as cursor:
            cursor.execute(query_select, (import_id, citizen_id))
            res = cursor.fetchone()[0]
        success = True
    except DBConnectionError as err:
        LogMsg('Ошибка подключения к базе данных ' + str(err))
    except CredentialsError as err:
        LogMsg('Неверные имя/пароль ' + str(err))
    except SQLError as err:
        LogMsg('Ошибка SQL запроса ' + str(err))
    except Exception as err:
        LogMsg('Неизвестная ошибка ' + str(err))
    finally:
        if not success:
            abort(400)

    cur_relatives = [int(i) for i in res.split(';') if i]
    LogMsg("update_relatives", cur_relatives)
    for relative in new_relatives:
        if relative not in cur_relatives:
            add_relative(import_id, relative, citizen_id)
    for relative in cur_relatives:
        if relative not in new_relatives:
            del_relative(import_id, relative, citizen_id)

    new_relatives = ';'.join(map(str, new_relatives))
    query_set = """
        UPDATE
            `citizens`
        SET
            `relatives` = %s
        WHERE
            `import_id` = %s AND
            `citizen_id` = %s
    """
    try:
        with DBConnection(config.dbconfig) as cursor:
            cursor.execute(query_set, (new_relatives, import_id, citizen_id))
        success = True
    except DBConnectionError as err:
        LogMsg('Ошибка подключения к базе данных ' + str(err))
    except CredentialsError as err:
        LogMsg('Неверные имя/пароль ' + str(err))
    except SQLError as err:
        LogMsg('Ошибка SQL запроса ' + str(err))
    except Exception as err:
        LogMsg('Неизвестная ошибка ' + str(err))
    finally:
        if not success:
            abort(400)
    LogMsg('[update_relatives] end')


@app.route('/imports/<int:import_id>/citizens/birthdays', methods=['GET'])
def birthdays(import_id):
    LogMsg('[birthdays] start')
    if import_id > config.import_id:
        LogMsg('import_id', import_id)
        abort(400)
    # Получаем список жителей
    res = get_data(import_id)
    citizens = res.json['data']
    citizens = {citizen['citizen_id']: {
                'birth_date': citizen['birth_date'],
                'relatives': citizen['relatives']
                } for citizen in citizens}
    LogMsg("Citizens", citizens)
    res_data = {}
    _data = {}
    for i in range(1, 13):
        res_data[i] = {}
    for citizen_id, data in citizens.items():
        for relative in data['relatives']:
            rel_birth_month = parse_date(citizens[relative]['birth_date'])
            rel_birth_month = rel_birth_month.month
            LogMsg(citizen_id, relative, rel_birth_month)
            res_data[rel_birth_month].setdefault(citizen_id, 0)
            res_data[rel_birth_month][citizen_id] += 1
    LogMsg(res_data)
    for i in range(1, 13):
        tmp = []
        for citizen_id, presents in res_data[i].items():
            tmp.append({'citizen_id': citizen_id,
                        'presents': presents})
        _data[str(i)] = tmp
    LogMsg('[birthdays] end')
    return jsonify({'data': _data}), 200


@app.route('/imports/<int:import_id>/towns/stat/percentile/age',
           methods=['GET'])
def age(import_id):
    LogMsg('[age] start')
    if import_id > config.import_id:
        LogMsg('import_id', import_id)
        abort(400)
    res = get_data(import_id)
    citizens = res.json['data']
    towns = {}
    for citizen in citizens:
        town = citizen['town'].strip()
        towns.setdefault(town, [])
        age = relativedelta(date.today(), parse_date(citizen['birth_date']))
        towns[town].append(age.years)
    _data = []
    for town, ages in towns.items():
        l = len(ages)
        p50 = math.floor(len([i for i in ages if i < 50]) / l * 100)
        p75 = math.floor(len([i for i in ages if i < 75]) / l * 100)
        p99 = math.floor(len([i for i in ages if i < 99]) / l * 100)
        tmp = {
            'town': town,
            'p50': p50,
            'p75': p75,
            'p99': p99
        }
        _data.append(tmp)
    LogMsg('[age] end')
    return jsonify({'data': _data})

if __name__ == '__main__':
    LogMsg('Приложение запущенно')
    init()
    app.run(debug=True)
    LogMsg('Приложение остановлено')
