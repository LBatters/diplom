#!/usr/bin/python
# -*- coding: utf-8 -*-

import psycopg2, sys, getpass, ConfigParser
import pony

if len(sys.argv) != 2:
    print "You should to point file name which you want to save result"
    sys.exit(1)

#Сначала собираем информацию для создания запроса

#Выбираем способ задания конфигурации (вручную т.е через консоль или через конфигурационный файл)
is_manual = raw_input("Manual mode or configuration file mode?(Tap 0 or 1 respectively)\n")

#массив для хранения информации о подключении
arr_connect_info = []

#Если выбран ручной режим
if is_manual == "0":
    #Запрашиваем информацию о подключениии к БД
    connect_info = raw_input("Input database, user, host in point order:\n")
    arr_connect_info = connect_info.split(" ")

#Если выбран режим конфигурационного файла
else:
    #Открываем конфигурационный файл
    try:
        config = ConfigParser.ConfigParser()
        config.read("conf.ini")
    except Exception, e:
        print "Somethin goes wrong while reading the configuration file."
        print e
        print "Exit from programme"
        sys.exit()

    info = pony.ConfigSectionMap("DataBaseInfo",config)

    arr_connect_info.append(info["database"])
    arr_connect_info.append(info["user"])
    arr_connect_info.append(info["host"])

passw = getpass.getpass(prompt= "Input password for database:\n")

#Подключаемся к БД
cur = pony.connection(database=arr_connect_info[0],
                      user=arr_connect_info[1],
                      password=passw,
                      host=arr_connect_info[2])

if is_manual == "0":
    #Узнаем в каком порядке идут колонки в таблицах
    order_column = raw_input("Input in which order is your column(id, date, attribute) in table. For example: date id attribute\n")
    arr_order_column = order_column.split(" ")
    count_col = 0

    #Парсим полученную строку и определяем порядок колонок для последующей работы
    for column in arr_order_column:
        if column == "date":
            column_with_date = count_col
        if column == "id":
            column_with_id = count_col
        if column == "attribute":
            column_with_name = count_col
        count_col = count_col+1
#или берем все из конфигурационного файла
else:
    info = pony.ConfigSectionMap("OrderColumn",config)
    column_with_date = int(info["date"])
    column_with_id = int(info["id"])
    column_with_name = int(info["attribute"])

#Читаем названия таблиц для которых нужно построить запрос
try:
    file_table_names = open('table_names.txt','r')
except Exception, e:
    print "Somethin goes wrong while reading the table_name file."
    print e
    print "Exit from programme"
    sys.exit()

#Массив result_info для хранения словарей с информацией о таблицах (название таблицы и названия ее колонок)
result_info = []
table_names = file_table_names.read().split("\n")

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
    print "Exit from programme"
    sys.exit()

print "Bye"
print "Have a nice day!"