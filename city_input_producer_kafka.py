#!/usr/bin/env python
# coding: utf-8

# In[14]:


#импортируем нужные библиотеки:
from time import sleep
from json import dumps
from kafka import KafkaProducer
import requests


# In[15]:


appid = "71cd8104521484b0bf0dc7bb1f194183" #APIkey, полученный при регистрации на OpenWeatherMap.org.

#получаем id города, в случаеб что есть такой город на ресурсе openweathermap.org:
def get_city_id(s_city_name):
    try:
        res = requests.get("http://api.openweathermap.org/data/2.5/find",
                     params={'q': s_city_name, 'type': 'like', 'units': 'metric', 'lang': 'ru', 'APPID': appid})
        data = res.json()
        cities = ["{} ({})".format(d['name'], d['sys']['country'])
                  for d in data['list']]
        city_id = data['list'][0]['id']
        #print('city_id=', city_id)
    except Exception as e:
        print("Город не найден:", e)
        pass
    assert isinstance(city_id, int)
    return city_id

#получаем температуру воздуха и дату:
def request_current_weather(city_id, city_name):
    try:
        res = requests.get("http://api.openweathermap.org/data/2.5/forecast",
                     params={'id': city_id, 'units': 'metric', 'lang': 'ru', 'APPID': appid})
        data = res.json()
        
        #задаем отправителя сообщений:
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'),
                         api_version=(0, 10, 1),
                         security_protocol='PLAINTEXT')
        
        from kafka.admin import KafkaAdminClient, NewTopic
        #задаем API администратора для формирования топика и партицирования топика: 
        #два получателя - два раздела одного топика - каждому получателю свой раздел (по одному сообщению каждому)
        admin = KafkaAdminClient(
                client_id ='admin',
                bootstrap_servers=['localhost:9092'],
                security_protocol='PLAINTEXT',
                )

        topic_name = "city_temp_topic_kafka"

        topic = NewTopic(name=topic_name, num_partitions=2, replication_factor=1)

        admin.create_topics([topic])
        
        #первый раздел топика - название города - получит consumer_city:
        producer.send(topic_name, value=city_name, partition=0)
        
        #блокировка отправителя, пока сообщения не будут доставлены:
        #producer.flush()

        for i in data['list']:
            if i['dt_txt'][11] == '1' and i['dt_txt'][12] == '2':
                print( i['dt_txt'], '{0:+1.0f}'.format(i['main']['temp']))
                s=i['dt_txt'] + ' ' + '{0:+1.0f}'.format(i['main']['temp'])
                #второй раздел топика - температура воздуха и дата на каждый из пяти дней - получит consumer_date_temp:
                producer.send(topic_name, value=s, partition=1)
                sleep(1)
                #producer.flush()
    except Exception as e:
        print("Температура воздуха не найдена:", e)
        pass


# In[16]:


#видимая работа программы начинается здесь
#запрашиваем у пользователя название города, для которого будем получать записи о погоде на ресурсе openweathermap.org, затем сохранять в очередь сообщений kafka с партицированием (топик один, но два раздела: один раздел - название города, второй раздел - 5 записей о погоде для этого города):
a = input()
#вызываем функцию get_city_id, которая описана в подпрограмме выше, - получаем id города 
i = get_city_id(a)
#вызываем функцию request_current_weather, которая описана в подпрограмме выше, - получаем записи о погоде и отправляем их в kafk'у 
request_current_weather(i, a)


# In[ ]:




