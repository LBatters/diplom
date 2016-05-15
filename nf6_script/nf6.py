#!/usr/bin/python
# -*- coding: utf-8 -*-

import psycopg2, sys, getpass, ConfigParser

#--------------------------------------------------------------------------
#*************************Вспомогательные функции**************************
#--------------------------------------------------------------------------
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
#**************************************************************************
#--------------------------------------------------------------------------

if len(sys.argv) != 2:
    print "You should to point file name which you want to save result"
    sys.exit(1)

#Сначала собираем информацию для создания запроса

#Открываем конфигурационный файл
try:
    config = ConfigParser.ConfigParser()
    config.read("conf.ini")
except Exception, e:
    print "Somethin goes wrong while reading the configuration file."
    print e
    print "Exit from programme"
    sys.exit()

info = ConfigSectionMap("DataBaseInfo",config)

passw = getpass.getpass(prompt= "Input password for database:\n")

#Подкючаемся к БД
cur = connection(info["database"],
                 info["user"],
                 passw,
                 info["host"])

#информация о порядке следования колонок в таблицах
info = ConfigSectionMap("OrderColumn",config)
column_with_date = int(info["date"])
column_with_id = int(info["id"])
column_with_name = int(info["attribute"])

#Читаем названия таблиц для которых нужно построить запрос
try:
    file_table_names = ConfigSectionMap("TableName",config)
except Exception, e:
    print "Somethin goes wrong while reading the table_name file."
    print e
    print "Exit from programme"
    sys.exit()

#Массив result_info для хранения словарей с информацией о таблицах (название таблицы и названия ее колонок)
result_info = []
table_names = file_table_names["name"].split(",")

#получаем информацию о таблицах из БД
for table_name in table_names:
    if table_name != '':
        try:
            sql = "SELECT * FROM " + table_name
            cur.execute(sql)
        except psycopg2.Error as e:
            #на случай если в конфиге указана таблица которой нет в БД
            if e.pgcode == "42P01":
                print "Seems like you have the wrong name table '%s' in config, so:\n" % table_name
                print e.pgerror
                skip = raw_input("Do you want to skip this error?(Y or N):\n")
                if skip == "Y":
                    table_names.remove(table_name)
                    continue
                else:
                    print "then i am finishing working..."
                    sys.exit(0)
            else: print e
        name = table_name
        columns = [desc[0] for desc in cur.description]
        result_info.append({'name' : name,'columns': columns})


print "Starting build the query..."
hran_sql = "SELECT "
first = True

#Получаем первую строку запроса вида: SELECT column1, column2, column3
for r in result_info:
    if first is True :
        hran_sql = hran_sql + r['columns'][column_with_name]
        first = False
    else:
        hran_sql = hran_sql + ", "+r['columns'][column_with_name]

hran_sql = hran_sql + """
                FROM"""
first = True
count = 0

#Строим запрос дальше, подставляя информацию о таблицах в шаблон.
for r in result_info:
    id = r['columns'][column_with_id]
    atr = r['columns'][column_with_name]
    date = r['columns'][column_with_date]
    table = r['name']

    #Шаблон для запроса
    piece_sql = """
                    --Таблцица с """+atr+"""
                    (select a."""+id+""","""+atr+"""
                       from (
                               select """+id+""", """+date+""","""+atr+""", abs("""+date+""" -d) as div
                               from """+table+"""
                            ) as a
                       inner join
                            (
                             select """+id+""", min( abs(d-"""+date+""")) as mindiv
                             from """+table+"""
                             where """+date+""" <= d
                             group by """+id+"""
                            ) as b
                       on a."""+id+"""= b."""+id+""" and a.div = b.mindiv) as """+atr+"""
                """

    if count == 0:
        hran_sql = hran_sql+piece_sql+"""    inner join"""
        count = count+1
        continue
    if count >0 and count < len(result_info)-1:
        prev_table_info = result_info[count-1]
        prev_table_id = prev_table_info['columns'][column_with_id]
        prev_table_atr = prev_table_info['columns'][column_with_name]

        hran_sql = hran_sql+piece_sql+"""    on """+prev_table_atr+"""."""+prev_table_id+""" = """+atr+"""."""+id+"""
                       inner join
                       """
        count = count +1
        continue
    if count == len(result_info)-1:
        prev_table_info = result_info[count-1]
        prev_table_id = prev_table_info['columns'][column_with_id]
        prev_table_atr = prev_table_info['columns'][column_with_name]

        hran_sql = hran_sql+piece_sql+"""    on """+prev_table_atr+"""."""+prev_table_id+""" = """+atr+"""."""+id+""";
                           """

print ("Writing the result in file %s..." % sys.argv[1])

try:
    result_file = open(sys.argv[1], "w+")
    result_file.write(hran_sql)
except Exception, e:
    print "Somethin goes wrong while writing the %s file." % sys.argv[1]
    print e
    print "Exit from program"
    sys.exit()

print "Bye"
print "Have a nice day!"