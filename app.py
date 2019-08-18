# -*- coding: utf-8 -*-
from flask import Flask, jsonify, request, make_response, abort
from flask import render_template, json
from functools import wraps

import config
from dbconnection import DBConnection, DBConnectionError,\
                         CredentialsError, SQLError
from logger import LogMsg
from checker import *

app = Flask(__name__)
# TODO Подумать на переход на f-строки в запрсах к БД


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
                VALUES(
                    {citizen_id},
                    "{town}",
                    "{street}",
                    "{building}",
                    "{name}",
                    {apartment},
                    "{birth_date}",
                    "{gender}",
                    "{relatives}",
                    "{import_id}"
                )
            """
            relatives = {}
            for citizen in request.json['citizens']:
                if not check_fields(citizen):
                    abort(400)
                _birth_date = parse_date(citizen['birth_date'])
                if not _birth_date:
                    abort(400)
                if not relatives.get(citizen['citizen_id']):
                    # citizen['relatives'].sort()  # Нужно ли?
                    relatives[citizen['citizen_id']] = citizen['relatives']
                else:
                    LogMsg('Два человека с однаковыми id')
                    abort(400)

                cur_query = query.format(
                    citizen_id=citizen['citizen_id'],
                    town=citizen['town'],
                    street=citizen['street'],
                    building=citizen['building'],
                    name=citizen['name'],
                    apartment=citizen['apartment'],
                    birth_date=_birth_date,
                    gender=citizen['gender'],
                    relatives=';'.join(map(str, citizen['relatives'])),
                    import_id=config.import_id)
                cursor.execute(cur_query)
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
    # LogMsg(relatives)
    if not check_relatives(relatives):
        abort(400)
    LogMsg('[Method import_data] end')
    return jsonify({'data': {'import_id': config.import_id}}), 201


@app.route('/imports/<int:import_id>/citizens', methods=['GET'])
def get_data(import_id, citizen_id=None):
    LogMsg('[Method get_data] start whit import_id = ' + str(import_id))
    if import_id > config.import_id:
        LogMsg('Некорректный import_id: ' + str(import_id))
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
                    `import_id` = {_import_id}
                    {_citizen_id}
            """.format(_import_id=import_id,
                       _citizen_id=f"AND `citizen_id` = {citizen_id}"
                       if citizen_id else "")
            cursor.execute(query)

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
            for field in data:
                if field in ('town', 'street', 'building', 'name',
                             'gender', 'relatives', 'apartment'):
                    set_ += f' `{field}` = "{data[field]}" ,'
            if new_birth_date:
                set_ += f' `birth_date` = "{new_birth_date}",'
            set_ = set_[0:-1]  # удаляем последнюю запятую
            query = f"""
                UPDATE `citizens`
                SET
                    {set_}
                WHERE
                    `import_id` = {import_id} AND
                    `citizen_id` = {citizen_id}
            """
            cursor.execute(query)
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
    res = get_data(import_id, citizen_id)
    # Достаем тело ответа
    res = res.json
    LogMsg(res)
    LogMsg('[Method update] end')
    # Возвращаем первое поле из ответа
    return jsonify(res['data'][0])


if __name__ == '__main__':
    # Выполняется дважды из-за дебаг мода
    LogMsg('Приложение запущенно')
    init()
    app.run(debug=True)
    LogMsg('Приложение остановлено')
