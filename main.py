import os
import csv
import time
import json
import boto3
import openpyxl
import pandas as pd
import datetime as dt
from collections.abc import MutableMapping

s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')
response = s3_client.list_buckets()
list_res = []
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)


def list_bucket():
    print('Список хранилищ:')
    for bucket in response['Buckets']:
        list_res.append(bucket["Name"])
        print(f'{list_res.index(bucket["Name"])} - {bucket["Name"]}')
    return list


def choice_bucket():
    try:
        number_bucket = int(input("Номер хранилища для загрузки данных: "))
        bucketName = list_res[number_bucket]
        print(f"Выбрано хранилище: {list_res[number_bucket]}")
        return bucketName
    except:
        print("Такого хранилища не существует")


def create_csv(obj):
    d = {}
    with open(obj) as file:
        for line in file:
            line = line.replace('"', "`").replace("'", '"').replace("False", '"False"')
            json_replace = json.loads(line)
            flatten_json = flatten_dict(json_replace)
            write_to_csv(flatten_json)
            #df = pd.DataFrame(flatten_json, index=[0])
            #print(df)
            #df.to_csv('log.csv', index=False)


def flatten_dict(d: MutableMapping, sep: str= '.') -> MutableMapping:
    [flat_dict] = pd.json_normalize(d, sep=sep).to_dict(orient='records')
    return flat_dict


def write_to_csv(dict):
    with open("log.csv", "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(dict[0].keys()), quoting=csv.QUOTE_NONNUMERIC)
        writer.writeheader()
        for d in dict:
            writer.writerow(d)
 # row={"time": event["requestContext"]["time"],"routeKey": event["routeKey"],"rawPath": event["rawPath"],
    #   "accept": event["headers"]["accept"],"accept-language": event["headers"]["accept-language"],"host": event["headers"]["host"],
    #   "sec-fetch-dest": event["headers"]["sec-fetch-dest"],
    #   "sec-fetch-mode": event["headers"]["sec-fetch-mode"],"sec-fetch-site": event["headers"]["sec-fetch-site"], "sec-fetch-user": event["headers"]["sec-fetch-user"],
    #   "upgrade-insecure-requests": event["headers"]["upgrade-insecure-requests"],"user-agent": event["headers"]["user-agent"],"x-amzn-trace-id": event["headers"]["x-amzn-trace-id"],
    #   "x-forwarded-for": event["headers"]["x-forwarded-for"],"x-forwarded-port": event["headers"]["x-forwarded-port"],"x-forwarded-proto": event["headers"]["x-forwarded-proto"],
    #   "accountId": event["requestContext"]["accountId"],"queryStringParameters": event["queryStringParameters"]}


def dowload_file(datebefore, dateafter):
    print("Загруженные файлы:")
    start_date = dt.datetime(int(datebefore[2]), int(datebefore[1]), int(datebefore[0]))
    end_date = dt.datetime(int(dateafter[2]), int(dateafter[1]), int(dateafter[0]))
    all_date = pd.date_range(min(start_date, end_date),max(start_date, end_date)).strftime('%d:%m:%Y').tolist()
    for number in all_date:
        for obj in bucket.objects.filter(Prefix=f"year={number[6:]}/month={number[3:5]}/day={number[:2]}/"):
            print(obj.key)
            # сделать проверку на пустоту
            if not os.path.exists(os.path.dirname(obj.key)):
                os.makedirs(os.path.dirname(obj.key))
            bucket.download_file(obj.key, obj.key)


# s3.download_file('MyBucket', 'hello-remote.txt', 'hello2.txt')
# print(open('hello2.txt').read())
if __name__ == "__main__":
    try:
        list_bucket()
        bucketName = choice_bucket()
        bucket = s3_resource.Bucket(bucketName)
        option = int(input("1. Выгрузить данные за период\n2. Перевести данные в csv\nВыберите пункт меню:"))
        if option == 1:
            datebefore = input(f"Дата начала периода (формат {time.strftime('%d:%m:%Y')}): ").split(':')
            dateafter = input(f"Дата конца периода (формат {time.strftime('%d:%m:%Y')}): ").split(':')
            dowload_file(datebefore, dateafter)
        if option == 2:
            create_csv()
    except:
        print("Ошибка ввода данных")