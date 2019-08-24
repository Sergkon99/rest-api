# -*- coding: utf-8 -*-
from datetime import datetime


def LogMsg(*args, console=False):
    with open('app.log', 'a', encoding='utf-8') as log:
        s = ""
        for arg in args:
            s += str(arg) + " "
        log.write(f'[log {datetime.now()}]: ' + s + '\n')
    if console:
        print(msg)


class _LogMsg():
    def __init__(self, msg: str, console=False):
        with open('app.log', 'a', encoding='utf-8') as log:
            log.write(f'[log {datetime.now()}]: ' + msg + '\n')
        if console:
            print(msg)
