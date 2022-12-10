#!/usr/bin/env python
# coding: utf-8

# In[9]:


#импортируем нужные библиотеки:
from kafka import TopicPartition
from kafka import KafkaConsumer
from json import loads
import psycopg2


# In[10]:


#подключаемся к БД:
conn = psycopg2.connect(host="localhost", port = 5432, database="postgres", user="postgres", password="зщыепкуы")


# In[11]:


#заводим курсор (объект БД, через который будем выполнять запрос на вставку данных в таблицу БД):
cur = conn.cursor()


# In[12]:


#задаем первого получателя (consumer_city):
topic_name = "city_temp_topic_kafka"
consumer_city = KafkaConsumer(
     group_id="topic_partitioned",
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     api_version=(0, 10, 1),
     security_protocol='PLAINTEXT')

#привязываем первого получателя к топику (получает первый раздел топика, 1-е сообщение с названием города):
consumer_city.assign([TopicPartition(topic_name, 0)])
consumer_city.subscription()
for message in consumer_city:
    #print("p=%d value=%s" % (message.partition, message.value)) #строка для тестовой проверки
    city_name = str(message.value)
    city_name = city_name[3:len(city_name) - 2] #здесь очищаем строку от остатков (кавычки и пр.), полученных после преобразования из формата json 
    consumer_city.close() #закрываем нашего получателя


# In[13]:


#print(city_name) #строка для тестовой проверки, что все ок, город есть


# In[14]:


#задаем второго получателя (consumer_date_temp):
topic_name = "city_temp_topic_kafka"
consumer_date_temp = KafkaConsumer(
     group_id="topic_partitioned",
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     api_version=(0, 10, 1),
     security_protocol='PLAINTEXT')

#привязываем второго получателя к топику (получает второй раздел топика, 2-е сообщение):
consumer_date_temp.assign([TopicPartition(topic_name, 1)])
consumer_date_temp.subscription()
i = 0
for message in consumer_date_temp:
    #print ("p=%d value=%s" % (message.partition, message.value)) #строка для тестовой проверки
    i = i + 1
    temp_dat = str(message.value)
    temp_dat = temp_dat[3:len(temp_dat) - 2] #здесь очищаем строку от остатков (кавычки и пр.), полученных после преобразования из формата json 
    #print(temp_dat) #строка для тестовой проверки
    cur.execute("""insert into t_city_temp (city, date_temp) values ('""" + city_name + """','""" + temp_dat + """')""")
    if i == 5:
        consumer_date_temp.close()


# In[15]:


conn.commit() #фиксируем изменения в БД (то, что записали 5 новых строк в БД)


# In[16]:


cur.close() #закрываем курсор
conn.close() #закрываем подключение к БД

