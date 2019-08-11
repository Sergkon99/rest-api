from logger import LogMsg
import datetime as dt
import re


def check_relatives(relatives: dict):
    for citizen_id in relatives:
        for relative in relatives[citizen_id]:
            if citizen_id not in relatives[relative]:
                LogMsg(f'Ошибка в родственных связях! {citizen_id}\
                       нет в родственниках у {relative}')
                return False
    return True


def check_fields(citizen: dict):
    LogMsg('Запущена проверка наличия записей')
    diff = {'citizen_id', 'town', 'street', 'building',
            'apartment', 'name', 'birth_date', 'relatives'}
    diff = diff.difference(set(citizen.keys()))
    if diff:
        LogMsg('Отсутсвуют поля: ' + ', '.join(i for i in diff))
        return False
    LogMsg('Проверка наличия записей - OK')
    return True


def check_fields_for_update(data: dict):
    field_exist = False
    for field in data:
        if field in ('town', 'street', 'building', 'name', 'birth_date',
                     'gender', 'relatives', 'apartment'):
            field_exist = True
            if data[field] is None:
                return False
    return field_exist


def parse_date(date_str: str) -> dt.datetime:
    LogMsg('Запущен парсинг времени')
    if not re.fullmatch(r'\d{2}.\d{2}.\d{4}', date_str):
        LogMsg('Дата не соответствует формату ' + date_str)
        return None
    date = None
    try:
        day, month, year = map(int, date_str.split('.'))
        date = dt.date(year, month, day)
    except:
        LogMsg('Ошибка парсинга времени: ' + date_str)
        return None
    else:
        LogMsg('Парсинг времени прошел успешно')
    return date
