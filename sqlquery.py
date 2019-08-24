import config
from dbconnection import DBConnection, DBConnectionError,\
                         CredentialsError, SQLError
from logger import LogMsg


def SqlQuery(query, *args):
    LogMsg("SqlQuery")
    success = False
    res = None
    sql_args = []
    for arg in args:
        sql_args.append(arg)
    try:
        with DBConnection(config.dbconfig) as cursor:
            if args:
                cursor.execute(query, sql_args)
            else:
                cursor.execute(query)
            res = cursor.fetchall() or []
            LogMsg(res)
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
            LogMsg("SqlQuery not success")
            res = None

    return res


def SqlQueryScalar(query, *args):
    LogMsg("SqlQueryScalar")
    success = False
    res = None
    sql_args = []
    for arg in args:
        sql_args.append(arg)
    try:
        with DBConnection(config.dbconfig) as cursor:
            if args:
                cursor.execute(query, args)
            else:
                cursor.execute(query)
            res = cursor.fetchone()
            if res is not None:
                res = res[0]
            else:
                res = 0
            LogMsg(res)
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
            LogMsg("SqlQueryScalar not success")
            res = None

    return res
