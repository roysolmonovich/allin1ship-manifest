import gridfs
import redis
from rq import Queue
from flask import Flask, request  # , flash, redirect
from bg_task import background_task
from pymongo import MongoClient
import pandas as pd
from datetime import date, datetime, timedelta
from dateutil import relativedelta
from numpy import inf
import re
from c import db_cred
import csv
import json
import pickle
from bson.objectid import ObjectId
import mysql.connector
# from decimal import Decimal
# for i in range(20):
#     print(f'{i:02}')
# dict_tst = {1: 3, 2: 4, 3: 5}
# x = dict_tst.pop(4, dict_tst.pop(3))
# print(x, dict_tst)


def func(**kwargs):
    print(kwargs)

    # func_sum = 0
    # for i in args:
    #     func_sum += i
    # return func_sum
# var = (1, 2, 3, 4, 5, 6)
print(func(x=1, y=2, z=3))
# pprint library is used to make the output look more pretty
# from pprint import pprint
# connect to MongoDB, change the << MONGODB URL >> to reflect your own connection string
# pw = '+3mp0rary'
url = 'mongodb+srv://user1:temp0rary@manifests.ynpg6.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'
client = MongoClient(url)
db = client.manifests
# print(db.collection)
# Created or Switched to collection
# names: GeeksForGeeks
Collection = db.raw_manifests
t1 = datetime.now()

# format = Collection.find_one({"_id": ObjectId("605b48c443fb2bf66ebb32c0")})
# print(format)


def get(platform):
    platformat = {}
    format.pop('_id')
    for field in format:
        if platform in format[field]:
            platformat[field] = {'header': format[field][platform]['header']}
            if 'header_alt' in format[field]:
                platformat[field]['header_alt'] = format[field][platform]['header_alt']
            if 'header1' in format[field]:
                platformat[field]['header1'] = format[field][platform]['header1']
    return platformat


def put(platform, field, header, value):
    if field not in format:
        return f'"{field}" field not found in format.'
    if platform not in format[field]:
        return f'"{platform}" platform not found in format.'
    if header not in format[field][platform]:
        return f'"{header}" header not found for "{platform}" platform.'
    if value in format[field][platform][header]:
        return f'"{value}" value exists already for header "{header}" within "{platform}.{header}".'
    Collection.update_one({"_id": ObjectId("605b48c443fb2bf66ebb32c0")}, {
                          '$push': {f'{field}.{platform}.{header}': value}})


# print(get('shipstation'))
# print(put('shipstation', 'order_no', 'header', 'test'))
# print(format)
# print(Collection.find_one({"_id": ObjectId("605b48c443fb2bf66ebb32c0")}))
# Collection.update({"_id": ObjectId("605b48c443fb2bf66ebb32c0")}, {'$push': {'service.shipstation.header_alt.0': 'test'}})
# print(Collection.find_one({"_id": ObjectId("605b48c443fb2bf66ebb32c0")}))
t2 = datetime.now()
print(t2-t1)
# Loading or Opening the json file
# with open(r'dependencies/services/sv_to_code.json', 'r') as f:
#     sv_to_code = json.load(f)
# df = pd.read_excel(r'manifests\sellercloud_shipbridge ShippingRep.xlsx')
# print(df.head(5))
# Inserting the loaded data in the Collection
# if JSON contains data more than one entry
# insert_many is used else inser_one is used
MFRawNameCollection = db.manifest_names
# name = 'dttest00'
_id = ObjectId('605cad672e94c0a5599f751b')
name = MFRawNameCollection.find_one({'_id': ObjectId(_id)}, {'name': 1, '_id': 0})['name']
print(MFRawNameCollection.find_one({'_id': ObjectId(_id)}, {'name': 1, '_id': 0}))
# x = db[name].find({}, {'_id': 0})
df = pd.DataFrame(list(db[name].find({}, {'_id': 0})))
# df = pd.DataFrame.from_dict(x)
print(df.head(10))


# for s in x:
#     print(s)
# db[name].drop()
# test = MFRawNameCollection.find_one_and_delete({'_id': _id}, {'name': 1, '_id': 0})
# print(test)
# manifest_name = MFRawNameCollection.find({'name': name})
# manifest = ManifestData.find_by_name(name=name)
# if manifest:
#     print(f'name already taken.')
# if isinstance(df, list):
#     MFRawShipmentCollection.insert_many(df)
# elif isinstance(df, pd.core.frame.DataFrame):
#     # Collection.insert_many({'name': 'test', 'init_time': datetime.now(), 'raw_shipments': df.to_dict('records')})
#     # Collection.insert_many(df.to_dict('records'))
#     id_test = MFRawShipmentCollection.insert_many(df.to_dict('records'))
# else:
#     MFRawShipmentCollection.insert(df)

# print(x['_id'], str(x['_id']), type(x['_id']))

# Collection.update({"_id": id_test}, {"$set": {"raw_shipments": df.to_dict('list')}})
# x = Collection.find_one({'_id': id_test})
# print(x)
# fs = gridfs.GridFS(db)
# fs.put(str.encode(df.to_json(orient='records')), name="test_1")
# # print(fs.get(ObjectId('605ba31a4f252612aa9262ac')).read())
df = pd.read_excel(r'Manifests\shipstation Shippping details - test.xlsx')
print(df.head(5))
# x = RawMFCollection.find_one({'filename': 'test_4'})
# print(x)

x = {'NY': 3}


def func(row, number):
    row['State'] = 'New YYYork'
    return row


# df.loc[df['State'] == 'NY'] = 'xyz'
df.loc[df['State'] == 'NY'] = df.loc[df['State'] == 'NY'].apply(lambda row: func(row, x[row['State']]), axis=1)
pd.set_option('display.max_columns', None)
print(df.head(30))
# name = 'coll_test'
# eval(f"db.{name}.insert_many(df.to_dict('records'))")
# Issue the serverStatus command and print the results
# serverStatusResult = db.command("serverStatus")
# pprint(serverStatusResult)
# redis_client = redis.Redis(host='ec2-54-198-194-83.compute-1.amazonaws.com',
#     password='p62ec9530586b558b8a416ac999122f565069ba2bc5658de5ff6ca065aa7889b5',
#     port=15419)
# r.set('jti number', 'access', 900)
# print(redis_client.ttl('jti number'))

lst = [
    ["2021-03-15", "mstest-7"],
    ["2021-03-15", "smtest"],
    ["2021-03-15", "smtest-1"],
    ["2021-03-15", "sb"],
    ["2021-03-15", "spf"],
    ["2021-03-15", "mstest-2"],
    ["2021-03-15", "111"],
    ["2021-03-15", "111111a"],
    ["2021-03-15", "121121"],
    ["2021-03-15", "mstest-3"],
    ["2021-03-15", "mstest-4"],
    ["2021-03-15", "mstest-5"],
    ["2021-03-15", "mstest-6"],
    ["2021-03-12", "saraleisacutiepie"],
    ["2021-03-12", "fjsl;dkfja;slifgheor"],
    ["2021-03-11", "984largefile"],
    ["2021-03-11", "985large"],
    ["2021-03-11", "986large"],
    ["2021-03-11", "496large"],
    ["2021-03-11", "497large"],
    ["2021-03-11", "filelarge987"],
    ["2021-03-11", "988large"],
    ["2021-03-11", "test-4"],
    ["2021-03-11", "989large"],
    ["2021-03-11", "990 large"],
    ["2021-03-11", "991 large"],
    ["2021-03-11", "large 992"],
    ["2021-03-11", "large 993"],
    ["2021-03-11", "large 994"],
    ["2021-03-11", "large 995"],
    ["2021-03-11", "alrge 990"],
    ["2021-03-11", "lalalala1111"],
    ["2021-03-10", "asdfasdfe e e"],
    ["2021-03-10", "file ship 21"],
    ["2021-03-10", "13shi"],
    ["2021-03-10", "114ship"],
    ["2021-03-10", "test -1"],
    ["2021-03-10", "test-"],
    ["2021-03-10", "test-2"],
    ["2021-03-10", "Knicky"],
    ["2021-03-10", "knicky - zack 1"],
    ["2021-03-10", "Zack 123321"],
    ["2021-03-10", "zuchc micky"],
    ["2021-03-10", "nickey teswt6"],
    ["2021-03-09", "shp1"],
    ["2021-03-09", "1shi"],
    ["2021-03-09", "2shi"],
    ["2021-03-09", "3shi"],
    ["2021-03-09", "12shi"],
    ["2021-03-09", "4shi"],
    ["2021-03-09", "knickey zach test1255"],
    ["2021-03-09", "knickey zach test1254"],
    ["2021-03-09", "knickey zach test1253"],
    ["2021-03-09", "knickey zach test1252"],
    ["2021-03-09", "5ship"],
    ["2021-03-09", "shipst_details"],
    ["2021-03-09", "knickey zach test2"],
    ["2021-03-09", "knickey zach test4"],
    ["2021-03-09", "knickey zach test5"],
    ["2021-03-09", "knickey zach test6"],
    ["2021-03-09", "knickey zach test7"],
    ["2021-03-09", "knickey zach test12"],
    ["2021-03-09", "knickey zach test13"],
    ["2021-03-09", "knickey zach test15"],
    ["2021-03-09", "knickey zach test16"],
    ["2021-03-09", "knickey zach test17"],
    ["2021-03-09", "knickey zach test20"],
    ["2021-03-09", "knickey zach test21"],
    ["2021-03-09", "knickey zach test22"],
    ["2021-03-09", "knickey zach test23"],
    ["2021-03-09", "knickey zach test24"],
    ["2021-03-09", "knickey zach test26"],
    ["2021-03-09", "knickey zach test28"],
    ["2021-03-09", "knickey zach test29"],
    ["2021-03-09", "knickey zach test31"],
    ["2021-03-09", "knickey zach test32"],
    ["2021-03-09", "knickey zach test34"],
    ["2021-03-09", "knickey zach test35"],
    ["2021-03-09", "knickey zach test36"],
    ["2021-03-09", "knickey zach test37"],
    ["2021-03-09", "asdaa22"],
    ["2021-03-09", "1shiq"],
    ["2021-03-09", "ship6"],
    ["2021-03-09", "aa6655"],
    ["2021-03-09", "8shi"],
    ["2021-03-09", "9shi"],
    ["2021-03-09", "10shi"],
    ["2021-03-09", "asdf"],
    ["2021-03-09", "11shhi"],
    ["2021-03-09", "12shhi"],
    ["2021-03-09", "ship770"],
    ["2021-03-08", "test313"],
    ["2021-03-08", "sh ip 1"],
    ["2021-03-08", "stat1"],
    ["2021-03-08", "stat 2"],
    ["2021-03-08", "statt2"],
    ["2021-03-08", "stat3"],
    ["2021-03-08", "test314"],
    ["2021-03-08", "test315"],
    ["2021-03-08", "test316"],
    ["2021-03-08", "unique file name"],
    ["2021-03-08", "test 305"],
    ["2021-03-08", "xyz"],
    ["2021-03-08", "xyzw"],
    ["2021-03-08", "test317"],
    ["2021-03-08", "test318"],
    ["2021-03-08", "xyzwy"],
    ["2021-03-08", "xyzwz"],
    ["2021-03-08", "xyzwzw"],
    ["2021-03-08", "test300"],
    ["2021-03-08", "test299"],
    ["2021-03-08", "test312"],
    ["2021-03-08", "test319"],
    ["2021-03-08", "knickey zach test1241"],
    ["2021-03-08", "knickey zach test1242"],
    ["2021-03-08", "knickey zach test1243"],
    ["2021-03-08", "knickey zach test1244"],
    ["2021-03-08", "knickey zach test1245"],
    ["2021-03-08", "knickey zach test1246"],
    ["2021-03-08", "zs sdf"],
    ["2021-03-08", "knickey zach test1247"],
    ["2021-03-08", "knickey zach test1249"],
    ["2021-03-08", "knickey zach test1250"],
    ["2021-03-08", "aass ac"],
    ["2021-03-08", "stat4"],
    ["2021-03-08", "sh sh h"],
    ["2021-03-08", "a b c "],
    ["2021-03-08", "ab df"],
    ["2021-03-08", "a b c 1"],
    ["2021-03-08", "a 11"],
    ["2021-03-08", "stat56"],
    ["2021-03-08", "a astsar a1"],
    ["2021-03-08", "chip1"],
    ["2021-03-08", "chip2"],
    ["2021-03-08", "aship11"],
    ["2021-03-08", "aship22"],
    ["2021-03-04", "ship large a"],
    ["2021-03-04", "ship new a "],
    ["2021-03-04", "ship new c"],
    ["2021-03-04", "ship small a"],
    ["2021-03-04", "ship new d"],
    ["2021-03-04", "test288"],
    ["2021-03-04", "dfasd"],
    ["2021-03-04", "knickey zach test1234"],
    ["2021-03-04", "aaaasdf"],
    ["2021-03-04", "new1123"],
    ["2021-03-04", "knickey zach test1233"],
    ["2021-03-04", "ship new e"],
    ["2021-03-04", "test289"],
    ["2021-03-04", "test290"],
    ["2021-03-04", "test291"],
    ["2021-03-04", "test292"],
    ["2021-03-04", "test293"],
    ["2021-03-04", "test294"],
    ["2021-03-04", "test295"],
    ["2021-03-04", "test297"],
    ["2021-03-04", "test298"],
    ["2021-03-04", "test301"],
    ["2021-03-04", "test302"],
    ["2021-03-04", "test303"],
    ["2021-03-04", "test304"],
    ["2021-03-04", "test305"],
    ["2021-03-04", "test306"],
    ["2021-03-04", "test307"],
    ["2021-03-04", "test308"],
    ["2021-03-04", "fsdfa"],
    ["2021-03-04", "sfdfsa  "],
    ["2021-03-04", "sss  ss"],
    ["2021-03-04", "aa ss sssd"],
    ["2021-03-04", "shi aa"],
    ["2021-03-04", "knickey zach test1235"],
    ["2021-03-04", "shi ab"],
    ["2021-03-04", "xxcv"],
    ["2021-03-04", "ship ac"],
    ["2021-03-04", "knickey zach test1236"],
    ["2021-03-04", "knickey zach test1237"],
    ["2021-03-04", "shippy aa"],
    ["2021-03-04", "knickey zach test1238"],
    ["2021-03-04", "zaxk a"],
    ["2021-03-04", "zaxk b"],
    ["2021-03-04", "zaxk c"],
    ["2021-03-04", "q1"],
    ["2021-03-04", "knickey zach test1240"],
    ["2021-03-04", "q2"],
    ["2021-03-04", "Q3"],
    ["2021-03-04", "q4"],
    ["2021-03-04", "q5"],
    ["2021-03-04", "q6"],
    ["2021-03-04", "q7"],
    ["2021-03-04", "q8"],
    ["2021-03-04", "q9"],
    ["2021-03-04", "q10"],
    ["2021-03-04", "q11"],
    ["2021-03-03", "ship1212"],
    ["2021-03-03", "s hip a"],
    ["2021-03-03", "s hip b"],
    ["2021-03-03", "s hipb"],
    ["2021-03-03", "shi pa"],
    ["2021-03-03", "knicky-zach test"],
    ["2021-03-03", "nicky test zach"],
    ["2021-03-03", "knickey zach test123"],
    ["2021-03-03", "test knicky b"],
    ["2021-03-03", "test knicky c"],
    ["2021-03-03", "test285"],
    ["2021-03-03", "test287"],
    ["2021-03-02", "test250"],
    ["2021-03-02", "test251"],
    ["2021-03-02", "test252"],
    ["2021-03-02", "test253"],
    ["2021-03-02", "test254"],
    ["2021-03-02", "test231"],
    ["2021-03-02", "test232"],
    ["2021-03-02", "shia"],
    ["2021-03-02", "shipsaa"],
    ["2021-03-02", "shipsac"],
    ["2021-03-02", "shipsad"],
    ["2021-03-02", "shipsae"],
    ["2021-03-02", "shipsaf"],
    ["2021-03-02", "shipsag"],
    ["2021-03-02", "shipaj"],
    ["2021-03-02", "shipah"],
    ["2021-03-02", "shipajk"],
    ["2021-03-02", "shipal"],
    ["2021-03-02", "test255"],
    ["2021-03-02", "test256"],
    ["2021-03-02", "test257"],
    ["2021-03-02", "shipalk"],
    ["2021-03-02", "shipaq"],
    ["2021-03-02", "shipaw"],
    ["2021-03-02", "shipaer"],
    ["2021-03-02", "test262"],
    ["2021-03-02", "test263"],
    ["2021-03-02", "t1"],
    ["2021-03-02", "shipawe"],
    ["2021-03-02", "zonear"],
    ["2021-03-02", "zoneat"],
    ["2021-03-02", "shipate"],
    ["2021-03-02", "test267"],
    ["2021-03-02", "test268"],
    ["2021-03-02", "test269"],
    ["2021-03-02", "test270"],
    ["2021-03-02", "test271"],
    ["2021-03-02", "test272"],
    ["2021-03-02", "test273"],
    ["2021-03-02", "shipart"],
    ["2021-03-02", "shipaza"],
    ["2021-03-02", "shipace"],
    ["2021-03-02", "shipazzz"],
    ["2021-03-01", "shipa"],
    ["2021-03-01", "shipb"],
    ["2021-03-01", "shipc"],
    ["2021-03-01", "shipd"],
    ["2021-03-01", "shipe"],
    ["2021-03-01", "shipf"],
    ["2021-03-01", "shipg"],
    ["2021-03-01", "shiph"],
    ["2021-03-01", "shipi"],
    ["2021-03-01", "shipj"],
    ["2021-03-01", "shipstation1"],
    ["2021-03-01", "shipk"],
    ["2021-03-01", "shipl"],
    ["2021-03-01", "shipm"],
    ["2021-03-01", "shipn"],
    ["2021-03-01", "shipo"],
    ["2021-03-01", "shipp"],
    ["2021-03-01", "shipq"],
    ["2021-03-01", "shipss"],
    ["2021-03-01", "shipt"],
    ["2021-03-01", "shipw"],
    ["2021-03-01", "shipx"],
    ["2021-03-01", "shipz"],
    ["2021-03-01", "shipaz"],
    ["2021-03-01", "sshipaza"],
    ["2021-03-01", "shipab"],
    ["2021-03-01", "shipac"],
    ["2021-03-01", "shipad"],
    ["2021-02-25", "test223"],
    ["2021-02-25", "test224"],
    ["2021-02-25", "test225"],
    ["2021-02-25", "test226"],
    ["2021-02-24", "test93"],
    ["2021-02-24", "test41"],
    ["2021-02-24", "test40"],
    ["2021-02-24", "test39"],
    ["2021-02-24", "shipstation1921"],
    ["2021-02-24", "shipstation19211"],
    ["2021-02-24", "shipstation192111"],
    ["2021-02-24", "edge11"],
    ["2021-02-24", "test38"],
    ["2021-02-24", "test37"],
    ["2021-02-24", "test931"],
    ["2021-02-24", "test120"],
    ["2021-02-24", "asdfsdf"],
    ["2021-02-24", "dfgsfgsdf"],
    ["2021-02-24", "dffgsdf"],
    ["2021-02-24", "dffgsd"],
    ["2021-02-24", "dffgs"],
    ["2021-02-24", "shopify smple"],
    ["2021-02-24", "shopify smpl"],
    ["2021-02-24", "shopify smp"],
    ["2021-02-24", "edge111"],
    ["2021-02-24", "shopify sm"],
    ["2021-02-24", "aaass"],
    ["2021-02-24", "shipstation12123"],
    ["2021-02-24", "axai"],
    ["2021-02-24", "shhhh"],
    ["2021-02-24", "test30"],
    ["2021-02-24", "sdfasdfa"],
    ["2021-02-24", "ssssdddd"],
    ["2021-02-24", "test29"],
    ["2021-02-24", "test28"],
    ["2021-02-24", "aaaddd"],
    ["2021-02-24", "asasd"],
    ["2021-02-24", "shipstation21799"],
    ["2021-02-24", "shipstation12345"],
    ["2021-02-24", "shipstation1234"],
    ["2021-02-24", "fsgsgfhjlll"],
    ["2021-02-24", "test200"],
    ["2021-02-24", "test201"],
    ["2021-02-24", "test202"],
    ["2021-02-24", "fsgsgfhjl"],
    ["2021-02-24", "efasd"],
    ["2021-02-24", "adbbbmm"],
    ["2021-02-24", "shipstation19999"],
    ["2021-02-24", "test203"],
    ["2021-02-24", "test204"],
    ["2021-02-24", "test205"],
    ["2021-02-24", "test206"],
    ["2021-02-24", "test207"],
    ["2021-02-24", "test209"],
    ["2021-02-24", "test210"],
    ["2021-02-24", "test211"],
    ["2021-02-24", "test212"],
    ["2021-02-24", "test213"],
    ["2021-02-24", "test214"],
    ["2021-02-24", "test215"],
    ["2021-02-24", "test216"],
    ["2021-02-24", "test217"],
    ["2021-02-24", "test218"],
    ["2021-02-24", "test219"],
    ["2021-02-24", "test220"],
    ["2021-02-24", "test221"],
    ["2021-02-24", "sfasdnmew"],
    ["2021-02-24", "shipstation234321"],
    ["2021-02-24", "xcghgnn"],
    ["2021-02-24", "test222"],
    ["2021-02-24", "shipstation2000"],
    ["2021-02-24", "goooghf"],
    ["2021-02-24", "fielnsme"],
    ["2021-02-24", "shipstation2021"],
    ["2021-02-24", "shippy1"],
    ["2021-02-24", "shippy12"],
    ["2021-02-24", "shipstation20211"],
    ["2021-02-24", "shippy2"],
    ["2021-02-24", "test send filters file name"],
    ["2021-02-24", "filter send example"],
    ["2021-02-24", "shippy3"],
    ["2021-02-24", "shippy1243"],
    ["2021-02-24", "shippy4"],
    ["2021-02-24", "shipstation255"],
    ["2021-02-24", "SHIPPY POST EXample"],
    ["2021-02-24", "test post filters"],
    ["2021-02-24", "asdasd"],
    ["2021-02-24", "shippy test filtersd"],
    ["2021-02-24", "shippy testt sesnts"],
    ["2021-02-24", "SHIPY"],
    ["2021-02-24", "ship test send filteres"],
    ["2021-02-24", "shipstation2025"],
    ["2021-02-24", "shippy5"],
    ["2021-02-24", "shipstation12121"],
    ["2021-02-24", "shipp1"],
    ["2021-02-24", "shippy12312"],
    ["2021-02-23", "test83"],
    ["2021-02-23", "test84"],
    ["2021-02-23", "test85"],
    ["2021-02-23", "test86"],
    ["2021-02-23", "shipstation1133"],
    ["2021-02-23", "shipstation1137"],
    ["2021-02-23", "shipstation1134"],
    ["2021-02-23", "shipstation1142 big"],
    ["2021-02-23", "test87"],
    ["2021-02-23", "test88"],
    ["2021-02-23", "test89"],
    ["2021-02-23", "test90"],
    ["2021-02-23", "test91"],
    ["2021-02-23", "test92"],
    ["2021-02-23", "test94"],
    ["2021-02-23", "test95"],
    ["2021-02-23", "test96"],
    ["2021-02-23", "shipstation1258"],
    ["2021-02-23", "shipstation321"],
    ["2021-02-23", "shipstation233"],
    ["2021-02-23", "shipstation14342"],
    ["2021-02-23", "shipstation1356"],
    ["2021-02-23", "shipstation1357"],
    ["2021-02-23", "sdfsdf"],
    ["2021-02-23", "shipstationtestedge"],
    ["2021-02-23", "shipstation14366"],
    ["2021-02-23", "shipstationtestexplore"],
    ["2021-02-23", "shipstation1441"],
    ["2021-02-23", "shipstation15555"],
    ["2021-02-23", "test97"],
    ["2021-02-23", "test98"],
    ["2021-02-23", "test99"],
    ["2021-02-23", "test50"],
    ["2021-02-23", "test51"],
    ["2021-02-23", "test52"],
    ["2021-02-23", "test53"],
    ["2021-02-23", "test54"],
    ["2021-02-23", "test49"],
    ["2021-02-23", "test48"],
    ["2021-02-23", "test47"],
    ["2021-02-23", "test46"],
    ["2021-02-23", "test45"],
    ["2021-02-23", "test44"],
    ["2021-02-23", "test43"],
    ["2021-02-23", "test42"],
    ["2021-02-23", "test0"],
    ["2021-02-23", "test106"],
    ["2021-02-23", "test109"],
    ["2021-02-23", "test110"],
    ["2021-02-22", "test1000"],
    ["2021-02-22", "test5"],
    ["2021-02-22", "test6"],
    ["2021-02-22", "test7"],
    ["2021-02-22", "test8"],
    ["2021-02-22", "shipstation2"],
    ["2021-02-22", "test9"],
    ["2021-02-22", "test10"],
    ["2021-02-22", "testing"],
    ["2021-02-22", "shipstation a"],
    ["2021-02-22", "sellercloudB"],
    ["2021-02-22", "shipstation217"],
    ["2021-02-22", "shipstation222"],
    ["2021-02-22", "shipstationaa"],
    ["2021-02-22", "test80"],
    ["2021-02-22", "test81"],
    ["2021-02-22", "test100"],
    ["2021-02-22", "test101"],
    ["2021-02-22", "shipstation529"],
    ["2021-02-22", "shipstation532"],
    ["2021-02-22", "test82"],
    ["2021-02-22", "shipstation536"],
    ["2021-02-18", "abc"],
    ["2021-02-17", "shopify shipments manifest example"],
    ["None", "test4"]
]

print(sorted(lst, reverse=True))
query_test = "SELECT manifest_data_test.id, manifest_data_test.orderno, manifest_data_test.shipdate, manifest_data_test.weight, manifest_data_test.service, manifest_data_test.zip, manifest_data_test.country, manifest_data_test.insured, manifest_data_test.dim1, manifest_data_test.dim2, manifest_data_test.dim3, manifest_data_test.price, manifest_data_test.zone, manifest_data_test.weight_threshold, manifest_data_test.sugg_service, manifest_data_test.dhl_tier_1_2021, manifest_data_test.dhl_tier_2_2021, manifest_data_test.dhl_tier_3_2021, manifest_data_test.dhl_tier_4_2021, manifest_data_test.dhl_tier_5_2021, manifest_data_test.dhl_cost_2021, manifest_data_test.usps_2021, manifest_data_test.dhl_cost_shipdate, manifest_data_test.usps_shipdate \
FROM manifest_data_test \
WHERE manifest_data_test.id = :id_1 AND manifest_data_test.shipdate BETWEEN :shipdate_1 AND :shipdate_2 AND (manifest_data_test.weight BETWEEN :weight_1 AND :weight_2 AND (manifest_data_test.zone IN (:zone_1, :zone_2, :zone_3, :zone_4, :zone_5, :zone_6, :zone_7) OR manifest_data_test.country != :country_1) OR manifest_data_test.weight BETWEEN :weight_3 AND :weight_4 AND manifest_data_test.zone IN (:zone_8, :zone_9, :zone_10)) ORDER BY manifest_data_test.shipdate"
regex_pattern = r'(\w+\.(id, |orderno, |weight_threshold, |weight, |service, |zip, |dim1, |dim2, |dim3, |zone, |sugg_service, |))'
query_sub = re.sub(regex_pattern, '', query_test)
print(query_sub)

# import matplotlib.pyplot as plt

# y = [
#             [
#                 "2020-12-12",
#                 1
#             ],
#             [
#                 "2020-12-25",
#                 1
#             ],
#             [
#                 "2020-12-28",
#                 21
#             ],
#             [
#                 "2020-12-29",
#                 28
#             ],
#             [
#                 "2020-12-30",
#                 24
#             ],
#             [
#                 "2020-12-31",
#                 25
#             ],
#             [
#                 "2021-01-01",
#                 22
#             ],
#             [
#                 "2021-01-02",
#                 4
#             ],
#             [
#                 "2021-01-03",
#                 44
#             ],
#             [
#                 "2021-01-04",
#                 25
#             ],
#             [
#                 "2021-01-05",
#                 42
#             ],
#             [
#                 "2021-01-06",
#                 35
#             ],
#             [
#                 "2021-01-07",
#                 33
#             ],
#             [
#                 "2021-01-08",
#                 16
#             ],
#             [
#                 "2021-01-09",
#                 3
#             ],
#             [
#                 "2021-01-10",
#                 13
#             ],
#             [
#                 "2021-01-11",
#                 31
#             ],
#             [
#                 "2021-01-12",
#                 29
#             ],
#             [
#                 "2021-01-13",
#                 23
#             ],
#             [
#                 "2021-01-14",
#                 31
#             ],
#             [
#                 "2021-01-15",
#                 11
#             ],
#             [
#                 "2021-01-16",
#                 2
#             ],
#             [
#                 "2021-01-17",
#                 38
#             ],
#             [
#                 "2021-01-18",
#                 32
#             ],
#             [
#                 "2021-01-19",
#                 29
#             ],
#             [
#                 "2021-01-20",
#                 26
#             ],
#             [
#                 "2021-01-21",
#                 28
#             ],
#             [
#                 "2021-01-22",
#                 11
#             ],
#             [
#                 "2021-01-23",
#                 9
#             ],
#             [
#                 "2021-01-24",
#                 32
#             ],
#             [
#                 "2021-01-25",
#                 30
#             ],
#             [
#                 "2021-01-26",
#                 38
#             ],
#             [
#                 "2021-01-27",
#                 8
#             ]
#         ]
# x = [_[0] for _ in y]
# plt.xlabel("X-axis")
# plt.ylabel("Y-axis")
# plt.title("A test graph")
# for i in range(len(y[0])):
#     plt.plot(x, [pt[i] for pt in y], label='id%s' % i)
# plt.legend()
# plt.show()
print(max(1, inf))
print(type(inf))
print(float('inf') is inf)
