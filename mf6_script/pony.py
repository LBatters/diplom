#!/usr/bin/python
# -*- coding: utf-8 -*-

import psycopg2

#вспомогательная функция для парсинга конфигурационного файла.
def ConfigSectionMap(section, config):
    dict1 = {}
    options = config.options(section)
    for option in options:
        try:
            print ("Reading the %s in configuration file..." % option)
            dict1[option] = config.get(section, option)
            if dict1[option] == -1:
                DebugPrint("skip: %s" % option)
        except:
            print("exception on %s!" % option)
            dict1[option] = None
    return dict1

#функция для подключения к БД
def connection(database, user, password, host):
    try:
        print ("Trying to connect to the database %s ..." % database)
        conn = psycopg2.connect(database=database,
                                user=user,
                                password=password,
                                host=host)
        return conn.cursor()
    except Exception, e:
        print "Exception while connect:"
        print e.pgerror

