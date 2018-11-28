import boto3
import random
import time
import pprint
import json
import base64
import urllib.parse as urlparse
import re
import string
import datetime
import unicodedata
import os
import sys

def get_anio(fecha):
	return int(fecha[:4])

def get_mes(fecha):
	return int(fecha[:7][-2:])

def get_dia(fecha):
	return int(fecha[-2:])

def diferencia_en_dias(fecha1,fecha2):
	d0 = datetime.date(get_anio(fecha1),get_mes(fecha1),get_dia(fecha1))
	d1 = datetime.date(get_anio(fecha2),get_mes(fecha2),get_dia(fecha2))	
	delta = d1 - d0
	return delta.days

def lambda_handler(event, context):
    print(event)
    Table = 'productFeed'
    client = boto3.client('dynamodb')
    print(str(event))
    payload = json.loads(json.dumps(event))['queryStringParameters']
    productID = payload['productID']
    name = urlparse.unquote(payload['name'])
    brand = payload['brand']
    description= urlparse.unquote(payload['description'])
    url= urlparse.unquote(payload['url'])
    image = payload['image']        
    price = payload['price']
    sale_price = price
    priceCurrency = payload['priceCurrency']
    availability = payload['availability']
    condition = payload['condition']
    category = urlparse.unquote(payload['category'])
    dateToday = datetime.datetime.now().strftime('20%y-%m-%d')
    global_trade_identifier = "Global Trade ID"
    manufacturer_part_number = "Manufacturer Part Number"
    def add_item():	
        response = client.put_item(TableName=Table,Item={
            'productID': {'S': productID},
            'name': {'S': name},
            'brand': {'S': brand},
            'description': {'S': description},
            'category': {'S': category},
            'price': {'N': price},
            'priceCurrency': {'S': priceCurrency},
            'sale_price': {'N': sale_price},
            'url': {'S': url},
            'image': {'S': image},
            'availability': {'S': availability},
            'condition': {'S': condition},
            'category': {'S': category},
            'manufacturer_part_number': {'S': manufacturer_part_number},
            'global_trade_identifier' : {'S': global_trade_identifier},
            'lastUpdated': {'S': dateToday}
		})
        return response
    response = client.get_item(TableName=Table,Key={'productID': {'S': productID}})
    print(response)
    if 'Item' in response:
        print(response['Item'])	
        disponibilidad_actual = response['Item']['availability']['S']
        if 'price' in response['Item']:
            precioExistente = int(response['Item']['price']['N'].replace('.',''))
        
        fechaGuardada = response['Item']['lastUpdated']['S']
        
        if availability != disponibilidad_actual:
            add_item()
            print("Se agrega item por cambio de disponibilidad, {}".format(productID))
        elif precioExistente != int(price):
            add_item()
            print("Se agrega item por cambio de precio, {}".format(productID))
        elif diferencia_en_dias(fechaGuardada,dateToday) > 1:
            add_item()
            print("Se agrega item por antigüedad del anterior, {}".format(productID))
        else:
            print("No se agrega item, {}".format(productID))
    else:
        add_item()
        print("Se agrega nuevo item, {}".format(productID))
    dev = {"isBase64Encoded": False,"statusCode": 200,"body": "response"}
    return dev
