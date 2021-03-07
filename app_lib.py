import os
import csv
import mysql.connector
import pickle
import json
import pandas as pd
import math
from datetime import datetime, date
from math import ceil
import bisect
import re
import string
# with open(r'dependencies\overlabeled\overlabeled.pkl', 'rb') as f:
#     overlabeled = pickle.load(f)
# with open(r'dependencies\overlabeled\overlabeled.json', 'w') as f:
#     json.dump(overlabeled, f, indent=4)
# with open(r'dependencies\sheets\customers.pkl', 'rb') as f:
#     customers = pickle.load(f)
try:
    with open(r'dependencies/overlabeled/overlabeled.json', 'r') as f:
        overlabeled = json.load(f)
except FileNotFoundError:
    print('file not found')
try:
    with open(r'dependencies/Shipping Variance/amp.pkl', 'rb') as f:
        amp = pickle.load(f)
except FileNotFoundError:
    print('file not found')
try:
    with open(r'dependencies/services/dhl_service_hash.json', 'r') as f:
        service = json.load(f)
except FileNotFoundError:
    print('file not found')
try:
    with open(r'dependencies/services/ai1s_service_names.json', 'r') as f:
        service_names = json.load(f)
except FileNotFoundError:
    print('file not found')
try:
    with open(r'dependencies/zones/USPS 2019 zip to zone.pkl', 'rb') as f:
        usps_zip_zone_2019 = pickle.load(f)
except FileNotFoundError:
    print('file not found')
try:
    with open(r'dependencies/zones/USPS 2020 zip to zone.pkl', 'rb') as f:
        usps_zip_zone_2020 = pickle.load(f)
except FileNotFoundError:
    print('file not found')
try:
    with open(r'dependencies/zones/DHL 2019 zip to zone.pkl', 'rb') as f:
        dhl_zip_zone_2019 = pickle.load(f)
except FileNotFoundError:
    print('file not found')
try:
    with open(r'dependencies/zones/DHL 2020 zip to zone.pkl', 'rb') as f:
        dhl_zip_zone_2020 = pickle.load(f)
except FileNotFoundError:
    print('file not found')
try:
    with open(r'dependencies/zones/country_to_code.json', 'r') as f:
        country_to_code = json.load(f)
except FileNotFoundError:
    print('file not found')
try:
    with open(r'dependencies/zones/CA_zip_to_zone.json', 'r') as f:
        ca_zip_zone = json.load(f)
except FileNotFoundError:
    print('file not found')
try:
    with open(r'dependencies/zones/intl_zone_names.pkl', 'rb') as f:
        intl_names = pickle.load(f)
except FileNotFoundError:
    print('file not found')
try:
    with open(r'flag_map.pkl', 'rb') as f:
        flag_map = pickle.load(f)
except FileNotFoundError:
    print('file not found')
try:
    with open(r'customer names.pkl', 'rb') as f:
        cust_names = pickle.load(f)
except FileNotFoundError:
    print('file not found')
try:
    with open(r'dependencies/Marked Up Items/del_to_dim.json', 'r') as f:
        del_to_dim = json.load(f)
except FileNotFoundError:
    print('file not found')
try:
    with open(r'invoices/invoices.json', 'rb') as f:
        invoices = json.load(f)
except FileNotFoundError:
    print('file not found')
try:
    with open(r'dependencies/sheets/tracking to account.pkl', 'rb') as f:
        tr_to_acc = pickle.load(f)
except FileNotFoundError:
    print('file not found')
try:
    with open(r'dependencies/services/sv_to_code.json', 'r') as f:
        sv_to_code = json.load(f)
except FileNotFoundError:
    print('file not found')

def acc_to_name(file):
    with open(file, 'r') as f:
        reader = csv.reader(f)
        next(reader)
        acc_names = {int(rows[3]): rows[0] for rows in reader}
        with open(r'customer names.pkl', 'wb') as fw:
            pickle.dump(acc_names, fw, pickle.HIGHEST_PROTOCOL)


# for x in list(tr_to_acc.items()):
#     if len(x[0][0]) == 22 and x[0][0].isnumeric() and x[0][0][0] == '9':
#         # tr_to_acc[(x[0][0], int(x[0][1][:5]))] = tr_to_acc.pop(x[0])
#         if x[0][0].isnumeric():
#             tr_to_acc[(int(x[0][0]), x[0][1])] = tr_to_acc.pop(x[0])
#             # print((int(x[0][0]), x[0][1]), tr_to_acc[(int(x[0][0]), x[0][1])])



# overlabeled_report_update(r'dependencies/overlabeled/20201228_Over_Label_Items_40340_4440844.csv')
# print(overlabeled)
#
# for dc in ('9361289936900293593967',
#            '9361289936900293593851',
#            '9361289936900293593929',
#            '9361289936900293593899',
#            '9361289936900293593820',
#            '9361289936900293593837',
#            '9361289936900293593783'):
#     print(overlabeled.get(dc), overlabeled.get("/'"+dc))
# overlabeled_report_update(r'C:/Users/Roy Solmonovich/Desktop/20201213_Over_Label_Items_40340_4409366.csv')

# del_con_to_dims('20201129_Marked_Up_Items_40340_4375552.csv')

# print(cust_names[5355340])
# acc_to_name('Tiers FINAL.csv')
# with open(r'customer names.pkl', 'rb') as f:
#     cust_names = pickle.load(f)
# print(cust_names)

class CarrierCharge:
    if os.path.exists(r'dependencies/charges_by_zone/carrier_charges111.pkl'):
        with open(r'dependencies/charges_by_zone/carrier_charges111.pkl', 'rb') as f:
            map = pickle.load(f)
    else:
        map = {}

    def __init__(self, carrier, location, date, service_code, ship_zone, weight, charge):
        self.carrier = carrier
        self.location = location
        self.date = date
        self.service_code = service_code
        self.ship_zone = ship_zone
        self.weight = weight
        self.charge = charge
        if CarrierCharge.map.get(carrier) is None:
            CarrierCharge.map[carrier] = {}
        if CarrierCharge.map[carrier].get(location) is None:
            CarrierCharge.map[carrier][location] = {}
        if CarrierCharge.map[carrier][location].get(date) is None:
            CarrierCharge.map[carrier][location][date] = {}
        if CarrierCharge.map[carrier][location][date].get(service_code) is None:
            CarrierCharge.map[carrier][location][date][service_code] = {}
        if CarrierCharge.map[carrier][location][date][service_code].get(ship_zone) is None:
            CarrierCharge.map[carrier][location][date][service_code][ship_zone] = {}
        # if CarrierCharge.map[carrier][location][date][service_code][ship_zone].get(weight) is None:
        CarrierCharge.map[carrier][location][date][service_code][ship_zone][weight] = charge

    def __str__(self):
        return (f"Carrier: {self.carrier}")

    def carrier(self):
        return self.carrier

    def location(self):
        return self.location

    def date(self):
        return self.date

    def service_code(self):
        return self.service_code

    def ship_zone(self):
        return self.ship_zone

    def weight(self):
        return self.weight

    def charge(self):
        return self.charge

    def update(carrier, file):
        with open(file, mode='r') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                if row[2] == '83':
                    row[2] = 82
                elif row[2] == '36':
                    row[2] = 81
                elif row[2] == '382':
                    row[2] = 383
                if row[1] == '15.99':
                    row[1] = 15.999
                if not row[4].replace('.', '', 1).isdigit():
                    continue
                d = row[0]
                if d.count('-') == 2:
                    d = str(datetime.strptime(d, '%Y-%m-%d').date())
                elif d.count('/') == 2:
                    d = str(datetime.strptime(d, '%m/%d/%Y').date())
                if d == date(2021, 7, 24):
                    d = date(2021, 1, 24)
                zone = row[3]
                loc = 'domestic'
                if zone[:4] != 'USPS':
                    loc = 'international'
                    if not (row[2] == '60' and zone[:2] == 'CA'):
                        zone = zone[:2]
                    else:
                        zone = zone[:2]+zone[3]
                CarrierCharge(carrier, loc,
                              d, int(row[2]), zone, float(row[1]),
                              float(row[4]))

                # if row[2] == 70:
                #     if float(row[1]) > 400:
                #         CarrierCharge(carrier, "domestic" if row[3][:4] == "USPS" else "international",
                #                       datetime.strptime(row[0], '%m/%d/%Y').date(), 81, row[3], float(row[1]),
                #                       float(row[4]) if row[4].replace('.', '', 1).isdigit() else -2)
                #         CarrierCharge(carrier, "domestic" if row[3][:4] == "USPS" else "international",
                #                       datetime.strptime(row[0], '%m/%d/%Y').date(), 82, row[3], float(row[1]),
                #                       float(row[4]) if row[4].replace('.', '', 1).isdigit() else -2)
                #         if int(row[3][-2:]) <= 8:
                #             CarrierCharge(carrier, "domestic" if row[3][:4] == "USPS" else "international",
                #                           datetime.strptime(row[0], '%m/%d/%Y').date(), 631, row[3], float(row[1]),
                #                           float(row[4]) if row[4].replace('.', '', 1).isdigit() else -2)
        with open(r'dependencies/charges by zone/carrier_charges111.pkl', 'wb') as f:
            pickle.dump(CarrierCharge.map, f, pickle.HIGHEST_PROTOCOL)

    def last_active_date(carrier, location, date):
        dates = sorted(list(CarrierCharge.map[carrier][location]))
        i_l = 0
        i_r = len(dates)-1
        if date >= dates[-1]:
            return dates[-1]
        elif date < dates[0]:
            return None
        while i_l <= i_r:
            m = (i_r+i_l)//2
            if date > dates[m]:
                i_l = m+1
            elif date < dates[m]:
                i_r = m-1
            else:
                return dates[m]
        if date >= dates[m]:
            return dates[m]
        else:
            return dates[m-1]

    def charge_validate(carrier, location, date, service_code, ship_zone, weight):
        if CarrierCharge.map.get(carrier) is None:
            # print(f"Carrier '{carrier}' not found in record.")
            return None
        if CarrierCharge.map[carrier].get(location) is None:
            print(f"Location '{location}' not found in record.")
            return None
        last_active_date = CarrierCharge.last_active_date(carrier, location, date)
        if last_active_date is None:
            print(f"Date '{date}' is earlier than our earliest active date.")
            return None
        if CarrierCharge.map[carrier][location][last_active_date].get(service_code) is None:
            print(f"Service code '{service_code}' not found.", carrier, location, date, service_code, ship_zone, weight)
            return None
        if CarrierCharge.map[carrier][location][last_active_date][service_code].get(ship_zone) is None:
            print(f"Ship zone '{ship_zone}' not found.")
            return None
        return last_active_date

    def charge_rate(carrier, location, date, service_code, ship_zone, weight):
        date = str(date)
        last_active_date = CarrierCharge.charge_validate(carrier, location, date, service_code, ship_zone, weight)
        if last_active_date is None:
            print(f"Active date prior to {date} could not be found for - {carrier} - {location}")
            return
        weight_charge = CarrierCharge.map[carrier][location][last_active_date][service_code][ship_zone]
        weights = sorted(list(weight_charge.keys()))
        i_l = 0
        i_r = len(weights)-1
        if weights == []:
            print(
                f"Charge not found based on given information: {carrier, location, date, service_code, ship_zone, weight}.")
            return
        if weight > weights[-1]:
            print(f"Weight larger than max weight bracket: {weights[-1]}.")
            return
        while i_l <= i_r:
            m = (i_r+i_l)//2
            if weight > weights[m]:
                i_l = m+1
            elif weight < weights[m]:
                i_r = m-1
            else:
                return weight_charge[weights[m]]
        if weight <= weights[m]:
            return weight_charge[weights[m]]
        else:
            return weight_charge[weights[m+1]]

    def charge_weight(carrier, location, date, service_code, ship_zone, weight):
        last_active_date = CarrierCharge.charge_validate(carrier, location, date, service_code, ship_zone, weight)
        if last_active_date is None:
            print(f"Active date prior to {date} could not be found for - {carrier} - {location}")
            return -1
        weight_charge = CarrierCharge.map[carrier][location][last_active_date][service_code][ship_zone]
        weights = sorted(list(weight_charge.keys()))
        i_l = 0
        i_r = len(weights)-1
        if weights == []:
            print(
                f"Charge not found based on given information: {carrier, location, date, service_code, ship_zone, weight}.")
            return -2
        if weight > weights[-1]:
            print(f"Weight larger than max weight bracket: {weights[-1]}.")
            return -3
        while i_l <= i_r:
            m = (i_r+i_l)//2
            if weight > weights[m]:
                i_l = m+1
            elif weight < weights[m]:
                i_r = m-1
            else:
                return weights[m]
        if weight <= weights[m]:
            return weights[m]
        else:
            return weights[m+1]
