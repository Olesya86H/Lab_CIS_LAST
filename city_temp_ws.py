#!/usr/bin/env python
# coding: utf-8

# In[19]:


#импортируем нужные библиотеки:
import psycopg2
from flask import Flask
from flask import request


# In[20]:


#определяем методы запросов работы приложения (метод GET - получаем данные)
web_service=Flask(__name__)
@web_service.route('/', methods=['GET'])

def get_data():
    #подключаемся к БД:
    conn = psycopg2.connect(host="localhost", port = 5432, database="postgres", user="postgres", password="зщыепкуы")
    #заводим курсор (объект БД, в котором будут храниться данные, полученные в результате запроса к БД):
    cur = conn.cursor()
    #заводим список, который потом выведем на страницу html в режиме разметки (через теги <ul>, <li>):
    list_query_results = list()
    #открываем блок с обработкой исключения - вдруг пользователь введет некорректный запрос в адресную строку браузера:
    try: #в части "try" описывается нормальный ход программы, когда пользователь ее не ломает:
        #считываем название города из адресной строки:
        city=request.args.get('city')
        #добавляем название города в список, чтобы потом вывести название на экран:
        list_query_results.append(city)
        #отправляем запрос в БД:
        cur.execute("""select date_temp from t_city_temp where lower(city) = lower('""" + city + """')""")
        #заполняем курсор данными из БД, полученными в результате запроса:
        query_results = cur.fetchall()
        #заводим дополнительный счетчик, который покажет, сколько записей мы получили в результате запроса (для каждого города в БД - 5 записей о погоде):
        counter = 0
        #заполняем список данных о погоде в цикле:
        for i in query_results:
            list_query_results.append(f"{i[0]}") #i[] - это одна строка-запись из БД с данными о погоде по выбранному городу
            counter = counter + 1 #увеличиваем счетчик записей в списке с данными о погоде
        #закрываем подключение к БД:
        conn.close()
        
        #поверяем, ввел ли пользователь в адресную строку браузера название города, которое есть в БД (тогда counter > 0), или которого нет в БД (тогда counter = 0): 
        if counter == 0:
            list_query_results.append("Для данного города не существует записей о погоде в БД!")
            #в случае, что записей для запрошенного города в БД нет - заполняем список вывода на экран пустыми строками:
            for k in range(4): 
                list_query_results.append("")
    except Exception as e:  #в части "except" описывается ненормальный ход программы, когда пользователь ее ломает:
        list_query_results.append("Ошибка чтения названия города!")
        pass #это заглушка
    #выводим на страницу html название города и 5 строк с записями о погоде в виде элементов списка, т.к. на каждый город у нас в БД прогноз на текущий день+4дня вперед:
    return f"""<ul>
    <li>{list_query_results[0]}</li>
    <li>{list_query_results[1]}</li>
    <li>{list_query_results[2]}</li>
    <li>{list_query_results[3]}</li>
    <li>{list_query_results[4]}</li>
    <li>{list_query_results[5]}</li>
</ul>"""

#запуск программы:
if __name__ == '__main__':
    web_service.run(debug=True,port=2700,host="127.0.0.1") #строку вида: http://127.0.0.1:2700/?city=surgut - вводим в адресную строку браузера после запуска в командной строке команды: python3 city_temp_ws.py.


# In[ ]:




