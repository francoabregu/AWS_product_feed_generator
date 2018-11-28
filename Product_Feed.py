
import os, boto3, pandas as pd, datetime as dt, numpy as np, pprint as pp, sys, urllib.request
import datetime
from requests.utils import quote

date = (dt.datetime.now() - dt.timedelta(days=1)).strftime('20%y%m%d%H')

def get_script_path():
	return os.path.dirname(os.path.realpath(sys.argv[0]))

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



script_path = get_script_path()

s3 = boto3.resource('s3')

#Descargo Tabla de DynamoDB
raw_file = script_path + "/Product_Feed.csv"
bash = "node "+ script_path +"/dynamoDBtoCSV.js -t JC-productFeed > " + raw_file
bucket = "jazmin-chebar"
print("Starting DynamoDB query")
os.system(bash)
print("DynamoDB query finished!")

#Transaformo la tabla a necesidad
frame = pd.read_csv(filepath_or_buffer=raw_file,encoding='utf-8')
dateToday = datetime.datetime.now().strftime('20%y-%m-%d')

frame['available'] = frame['lastUpdated'].apply(lambda date: diferencia_en_dias(date,dateToday) < 3)
frame = frame.loc[frame['available'] == True]
frame = frame.loc[frame['availability'] == 'disponible'] 
frame['esPagoDeDiferencia'] = frame['category'].apply(lambda category: "PAGO DE DIF" in category)
frame = frame.loc[frame['esPagoDeDiferencia'] == False] 
frame['description'] = frame['description'].apply(lambda x: x.replace("&quot;",""))
frame['description'] = frame['description'].apply(lambda x: x.replace('\n',""))
frame['description'] = frame['description'].apply(lambda x: x.replace('\r',""))
frame['url'] = frame['url'].apply(lambda x: x.replace(" ","%20").replace("[","%5B").replace("]","%5D"))
frame['image'] = frame['url'].apply(lambda x: x.replace(" ","%20").replace("[","%5B").replace("]","%5D"))
frame = frame.drop(labels=['available','esPagoDeDiferencia'],axis=1)

#Subir a S3
file = script_path + '/Product_Feed.csv'
frame.to_csv(path_or_buf=file,index=False)
s3.meta.client.upload_file(file, bucket, 'JC_Product_Feed.csv',ExtraArgs={'ACL': 'public-read'})