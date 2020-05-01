from kafka import KafkaProducer
from Employee import Employee
from Product import Product
from Process import Process
import datetime
import random
from random import randint
import time
import json

# gizemsutcu


def phn():
    n = '0000000000'
    while '9' in n[3:6] or n[3:6] == '000' or n[6] == n[7] == n[8] == n[9]:
        n = str(random.randint(10 ** 9, 10 ** 10 - 1))
    return n[:3] + '-' + n[3:6] + '-' + n[6:]


def randomDate():
    start_date = datetime.date(1960, 1, 1)
    end_date = datetime.date(2002, 1, 1)
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    random_date = start_date + datetime.timedelta(days=random_number_of_days)
    return random_date


def to_dict(obj):
    return json.loads(json.dumps(obj, default=lambda o: o.__dict__))


class Main:

    bootstrap_servers = ['209.250.250.135:9092']
    topicName = 'virtual-market-analysis'

    product_list = ['ice tea', 'coca cola', 'lemonade', 'orange juice', 'chery juice', 'feta cheese', 'olive', 'jam',
                    'honey', 'sausage', 'pasta', 'lentil', 'fame', 'sugar', 'salt', 'baking powder', 'vanilla', 'cocoa',
                    'curly', 'parsley', 'carrot', 'potato', 'onion', 'tomato', 'dish soap', 'shampoo']
    product_cost_list = [4.25, 3.75, 2.85, 5.75, 3.25, 15.50, 14.00, 6.00, 40.00, 8.00, 4.50, 17.75, 15.25,
                         12.00, 10.00, 2.00, 2.15, 4.50, 3.50, 3.25, 4.50, 5.00, 4.75, 5.50, 22.50, 12.00]

    while True:
        # user info
        userId = random.randint(100, 150)
        date = randomDate()
        date_time = "{:%d/%m/%Y}".format(date)
        telephoneNumber = phn()
        cityId = random.randint(1, 81)

        # process info
        productPiece = random.randint(1, 5)
        time = datetime.datetime.now()
        timestamp = time.strftime("%d/%m/%Y, %H:%M:%S")

        # product info
        productName = random.choice(product_list)
        index = product_list.index(productName)
        productCost = product_cost_list[index]
        productId = index + 1

        # creating an objects
        employee = Employee(userId, date_time, telephoneNumber, cityId)
        product = Product(productId, productName, productCost)
        process = Process(employee, product, productPiece, timestamp)

        jsonProcess = to_dict(process)

        # producer
        producer = KafkaProducer(bootstrap_servers=bootstrap_servers, retries=5,
                                 value_serializer=lambda m: json.dumps(m).encode('ascii'))

        producer.send(topicName, jsonProcess)
