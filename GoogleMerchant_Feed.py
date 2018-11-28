import os, boto3, pandas as pd, datetime as dt, numpy as np, pprint as pp, sys, urllib.request
import csv


def get_script_path():
	return os.path.dirname(os.path.realpath(sys.argv[0]))

def get_availability(availability):
    switcher = {
        "disponible": "in stock",
        "agotado": "out of stock",
    }
    return switcher.get(availability, "Invalid availability")

def get_price_and_currency(price,currency):
    return str(price) + " " + str(currency)


script_path = get_script_path()

ec2 = boto3.resource('ec2', region_name='sa-east-1')
s3 = boto3.resource('s3')
raw_file = script_path + "/Product_Feed.csv"
bucket = "jazmin-chebar"

frame = pd.read_csv(filepath_or_buffer=raw_file)

frame['condition'] = 'new'
frame['availability'] = frame['availability'].apply(lambda x: get_availability(x))
frame['name'] = frame['name'].apply(lambda x: x.capitalize()) 
price_with_currency = []

for index, fila in frame.iterrows():
    price_with_currency.append(str(frame.loc[index]['price']) + " " + frame.loc[index]['priceCurrency'])

frame['price'] = pd.DataFrame.from_dict(price_with_currency)

frame.rename(columns={
	'productID': 'id',
	'name': 'title',
	'url': 'link',
    'image': 'image_link',
    'category': 'product_type',
    'global_trade_identifier':'gtin',
    'manufacturer_part_number': 'mpn'
    
}, inplace=True)

frame['gtin'] = ''
frame['mpn'] = ''
frame['gender'] = 'female'
frame['size'] = 'Size'
frame['material'] = 'Material'
frame['adult'] = 'no'
frame['google_product_category'] = 'Google_product_category'
frame = frame.drop(labels=['sale_price','lastUpdated'],axis=1)
frame = frame.sort_values(by=['price'],axis=0)
frame = frame.drop_duplicates(subset=['id'],keep='first')
frame['identifier_​exists'] = 'no'
frame = frame[['id','title','description','link','image_link','availability','price','google_product_category','product_type','brand','identifier_​exists','gtin','mpn','condition','adult','gender','material','size']]
file = script_path + '/GoogleMerchant_Feed.csv'
frame.to_csv(path_or_buf=file,index=False,encoding='utf8')

# Convertir a txt separado por tabluaciones
with open(file) as csv_file:
    with open(script_path + '/GoogleMerchant_Feed.txt', 'w', newline='') as fout:
        reader = csv.DictReader(csv_file, delimiter=',')
        writer = csv.DictWriter(fout, reader.fieldnames, delimiter='\t')
        writer.writeheader()
        writer.writerows(reader)


s3.meta.client.upload_file(file, bucket, 'GoogleMerchant_Feed.csv',ExtraArgs={'ACL': 'public-read'})
s3.meta.client.upload_file(file, bucket, 'GoogleMerchant_Feed.txt',ExtraArgs={'ACL': 'public-read'})

#instanceid = nstanceid = urllib.request.urlopen('http://169.254.169.254/latest/meta-data/instance-id').read().decode()
#ec2.instances.filter(InstanceIds=[instanceid]).stop()
