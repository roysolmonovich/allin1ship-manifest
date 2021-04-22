import os
import csv
import mysql.connector
import pickle
import json
from datetime import datetime, date
from math import ceil
import bisect
from c import db_cred
import pandas as pd
try:
    with open(r'dependencies\overlabeled\overlabeled.json', 'r') as f:
        overlabeled = json.load(f)
except FileNotFoundError:
    pass
try:
    with open(r'dependencies\Shipping Variance\amp.pkl', 'rb') as f:
        amp = pickle.load(f)
except FileNotFoundError:
    pass
try:
    with open(r'dependencies\services\dhl_service_hash.json', 'r') as f:
        service = json.load(f)
except FileNotFoundError:
    pass
with open(r'dependencies\services\ai1s_service_names.json', 'r') as f:
    service_names = json.load(f)
try:
    with open(r'dependencies\zones\USPS 2019 zip to zone.pkl', 'rb') as f:
        usps_zip_zone_2019 = pickle.load(f)
except FileNotFoundError:
    pass
try:
    with open(r'dependencies\zones\USPS 2020 zip to zone.pkl', 'rb') as f:
        usps_zip_zone_2020 = pickle.load(f)
except FileNotFoundError:
    pass
try:
    with open(r'dependencies\zones\DHL 2019 zip to zone.pkl', 'rb') as f:
        dhl_zip_zone_2019 = pickle.load(f)
except FileNotFoundError:
    pass
try:
    with open(r'dependencies\zones\DHL 2020 zip to zone.pkl', 'rb') as f:
        dhl_zip_zone_2020 = pickle.load(f)
except FileNotFoundError:
    pass
with open(r'dependencies\zones\country_to_code.json', 'r') as f:
    country_to_code = json.load(f)
with open(r'dependencies\zones\CA_zip_to_zone.json', 'r') as f:
    ca_zip_zone = json.load(f)
with open(r'dependencies\zones\intl_zone_names.pkl', 'rb') as f:
    intl_names = pickle.load(f)
try:
    with open(r'flag_map.pkl', 'rb') as f:
        flag_map = pickle.load(f)
except FileNotFoundError:
    pass
try:
    with open(r'customer names.pkl', 'rb') as f:
        cust_names = pickle.load(f)
except FileNotFoundError:
    pass
try:
    with open(r'dependencies\Marked Up Items\del_to_dim.json', 'r') as f:
        del_to_dim = json.load(f)
except FileNotFoundError:
    pass
try:
    with open(r'invoices\invoices.json', 'rb') as f:
        invoices = json.load(f)
except FileNotFoundError:
    pass
try:
    with open(r'dependencies\sheets\tracking to account.pkl', 'rb') as f:
        tr_to_acc = pickle.load(f)
except FileNotFoundError:
    pass


def del_con_to_dims(file):
    # Adds Marked up Items report (shipment to dimensions) to an existing map.
    df = pd.read_csv(file)
    df = df[['Delivery Confirmation Number', 'DIM Height', 'DIM Length', 'DIM Width']]

    # Some rows have no dimensions. They need to be dropped to prevent errors.
    df.dropna(subset=['DIM Height'], inplace=True)
    dims = {rows[1][1:]: (float(rows[2]), float(rows[3]), float(rows[4])) for rows in df.itertuples() if rows[2] != ''}
    del_to_dim.update(dims)
    with open(r'dependencies\Marked Up Items\del_to_dim.json', 'w') as fw:
        json.dump(del_to_dim, fw, indent=4)


def tracking_to_acc(file):
    # Adds Customer Tracking file (delivery_confirm + zipcode to account #) to an existing map.
    with open(file, 'r') as f:
        reader = csv.reader(f)
        next(reader)
        tr_to_acc_new = {}
        for rows in reader:
            if rows[2]:
                # Numeric delivery_confirms of length 22 beginning with 9 belong to domestic shipments.
                # For domestic shipments, delivery_confirm + zip code is mapped to account #.
                if rows[0].isnumeric() and len(rows[0]) == 22 and rows[0][0] == '9':
                    tr_to_acc_new[(int(rows[0]), int(rows[1].replace('-', '')[:5]))] = int(rows[2])
                else:
                    # For international shipments, delivery_confirm by itself is mapped to account #.
                    tr_to_acc_new[rows[0]] = int(rows[2])
        tr_to_acc.update(tr_to_acc_new)
        with open(r'dependencies\sheets\tracking to account.pkl', 'wb') as fw:
            pickle.dump(tr_to_acc, fw, pickle.HIGHEST_PROTOCOL)


def overlabeled_report_update(file):
    cols = ['Delivery Confirmation Number', 'Data Capture Method']
    df = pd.read_csv(file, usecols=cols)
    df = df.loc[df['Data Capture Method'] == 'ENCODE']
    over_l = {row[1][1:]: row[2] for row in df.itertuples()}
    overlabeled.update(over_l)
    with open(r'dependencies\overlabeled\overlabeled.json', 'w') as fw:
        json.dump(overlabeled, fw, indent=4)


# Stopped using it
# def acc_to_name(file):
#     with open(file, 'r') as f:
#         reader = csv.reader(f)
#         next(reader)
#         acc_names = {int(rows[3]): rows[0] for rows in reader}
#         with open(r'customer names.pkl', 'wb') as fw:
#             pickle.dump(acc_names, fw, pickle.HIGHEST_PROTOCOL)


class CarrierCharge:
    if os.path.exists(r'dependencies\charges_by_zone\carrier_charges111.pkl'):
        with open(r'dependencies\charges_by_zone\carrier_charges111.pkl', 'rb') as f:
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
        with open(r'dependencies\charges_by_zone\carrier_charges111.pkl', 'wb') as f:
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
                return weight_charge[weights[m]]
        if weight <= weights[m]:
            return weight_charge[weights[m]]
        else:
            return weight_charge[weights[m+1]]

    def charge_weight(carrier, location, date, service_code, ship_zone, weight):
        if not weight:
            return weight
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


class Customer:
    if os.path.exists(r'customers.json'):
        with open(r'customers.json', 'r') as f:
            crm = json.load(f)
    else:
        crm = {}
    # mydb = mysql.connector.connect(
    #     **db_cred
    # )
    # mycursor = mydb.cursor()

    def __init__(self, account_no, mailer_id=None, location=None, date=None, tier=None, location_2=None, date_2=None, tier_2=None, payment_method=None, fee=None, name=None, qb_client=None):
        account_no = str(account_no)
        self.account_no = account_no
        self.name = name
        self.mailer_id = mailer_id
        self.location = location
        self.location_2 = location_2
        date = str(date) if date else None
        if date:
            if date.count('-') == 2:
                '2021-1-4' '2021-02-04'
                date = datetime.strptime(date, '%Y-%m-%d').strftime('%Y-%m-%d')
            elif date.count('/') == 2:
                date = datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
        self.date = date
        date_2 = str(date_2) if date_2 else None
        if date_2:
            if date_2.count('-') == 2:
                date_2 = datetime.strptime(date_2, '%Y-%m-%d').strftime('%Y-%m-%d')
            elif date_2.count('/') == 2:
                date_2 = datetime.strptime(date_2, '%m/%d/%Y').strftime('%Y-%m-%d')
        self.date_2 = date_2
        self.tier = tier
        self.tier_2 = tier_2
        self.payment_method = payment_method
        self.fee = fee
        self.qb_client = qb_client
        if account_no not in Customer.crm:
            Customer.crm[account_no] = {"mailer id": mailer_id, "tiers": {
                "domestic": [], "international": []}, 'payment method': payment_method, 'fee': fee}
        else:
            print(f'{name} already in system.')
            return
        if location and date and tier:
            bisect.insort(Customer.crm[account_no]["tiers"][location], [date, tier])
        if location_2 and date_2 and tier_2:
            bisect.insort(Customer.crm[account_no]["tiers"][location_2], [date_2, tier_2])
        if name:
            Customer.crm[account_no]['name'] = name
        if payment_method:
            Customer.crm[account_no]['payment method'] = payment_method
        if fee or fee == 0:
            Customer.crm[account_no]['fee'] = fee
        if qb_client:
            Customer.crm[account_no]['qb client'] = qb_client
        with open(r'customers.json', 'w') as f:
            json.dump(Customer.crm, f, indent=4)
        sql_insert = 'INSERT INTO customer (acc, mailer_id, payment_method, fee, name, qb_client) \
            VALUES (%s, %s, %s, %s, %s, %s)'

        mydb = mysql.connector.connect(**db_cred)
        mycursor = mydb.cursor()
        mycursor.execute(sql_insert, (account_no, mailer_id, payment_method, fee, name, qb_client))
        mydb.commit()
        mycursor.close()
        mydb.close()
        # Customer.mycursor.execute(sql_insert, (account_no, mailer_id, payment_method, fee, name, qb_client))
        # Customer.mydb.commit()

    def __str__(self):
        return f'Account #: {self.account_no}:\nmailer ID: {self.mailer_id}\n' +\
            f'Tiers: {Customer.crm[self.account_no]["tiers"]}\n' +\
            f'Fee:  {self.fee}\nPayment Method: {self.payment_method}\nName: {self.name}'

    def account_no(self):
        return self.account_no

    def mailer_id(self):
        return self.mailer_id

    def tiers(self):
        return Customer.crm[self.account_no]["tiers"]

    def update(account_no, location=None, rate_date=None, tier=None, mailer_id=None, fee=None, payment_method=None, name=None, qb_client=None, new_account_no=None):
        account_no = str(account_no)
        if account_no not in Customer.crm:
            print(f'Account # {account_no} not found.')
            return
        rate_date = str(rate_date) if rate_date else rate_date
        if rate_date and rate_date.count('-') == 2:
            rate_date = datetime.strptime(rate_date, '%Y-%m-%d').strftime('%Y-%m-%d')
        elif rate_date and rate_date.count('/') == 2:
            rate_date = datetime.strptime(rate_date, '%m/%d/%Y').strftime('%Y-%m-%d')
        if tier and location and rate_date:
            bisect.insort(Customer.crm[account_no]["tiers"][location], [rate_date, tier])
        if mailer_id:
            Customer.crm[account_no]['mailer id'] = mailer_id
        if fee or fee == 0:
            Customer.crm[account_no]['fee'] = fee
        if payment_method:
            Customer.crm[account_no]['payment method'] = payment_method
        if name:
            Customer.crm[account_no]['name'] = name
        if qb_client:
            Customer.crm[account_no]['qb client'] = qb_client
        if new_account_no:
            new_account_no = str(new_account_no)
            if new_account_no in Customer.crm:
                print(f'Account # {new_account_no} is already taken.')
                return
            Customer.crm[new_account_no] = Customer.crm.pop(account_no)
        with open(r'customers.json', 'w') as f:
            json.dump(Customer.crm, f, indent=4)

    def update_bulk(file):
        with open(file, 'r', encoding='unicode_escape') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                for i, v in enumerate(row):
                    if not v:
                        row[i] = None
                    elif v.isnumeric():
                        row[i] = int(row[i])
                    elif v.replace('.', '', 1).isnumeric():
                        row[i] = float(row[i])
                name, mailer_id, account_no, fee, payment_method = row[:2]+row[3:6]
                account_no = str(account_no)
                if account_no not in Customer.crm:
                    print(f'Customer {row[0]} not in system. Adding customer to system.')
                    Customer(account_no)
                if row[6]:
                    Customer.update(account_no=account_no, mailer_id=mailer_id,
                                    fee=fee, payment_method=payment_method, name=name)
                    for i, v in enumerate(row[6:-2:3]):
                        if v:
                            if v.count('-') == 2:
                                rate_date = datetime.strptime(v, '%Y-%m-%d').strftime('%Y-%m-%d')
                            elif v.count('/') == 2:
                                rate_date = datetime.strptime(v, '%m/%d/%Y').strftime('%Y-%m-%d')
                            else:
                                continue
                        if row[i*3+7]:
                            # recent_tier(account_no, 'domestic')
                            recent_tier_date = Customer.crm[account_no]['tiers']['domestic'][-1][0] if Customer.crm[account_no]['tiers']['domestic'] else None
                            print(recent_tier_date, rate_date)
                            if recent_tier_date and rate_date <= recent_tier_date:
                                print(
                                    f'Trying to add domestic {rate_date} update to {account_no} where {recent_tier_date} exists. Skipping update.')
                            else:
                                tier = row[i*3+7]
                                Customer.update(account_no=account_no,
                                                location='domestic', rate_date=rate_date, tier=tier)
                                print(account_no, 'domestic', rate_date, tier, 'updated successfully.')
                        if row[i*3+8]:
                            recent_tier_date = Customer.crm[account_no]['tiers']['international'][-1][0] if Customer.crm[account_no]['tiers']['international'] else None
                            if recent_tier_date and rate_date <= recent_tier_date:
                                print(
                                    f'Trying to add international {rate_date} update to {account_no} where {recent_tier_date} exists. Skipping update.')
                            else:
                                tier = row[i*3+8]
                                Customer.update(account_no=account_no,
                                                location='international', rate_date=rate_date, tier=tier)
                                print(account_no, 'international', rate_date, tier, 'updated successfully.')

    def delete(account_no):
        account_no = str(account_no)
        c = Customer.crm.pop(account_no, None)
        if not c:
            print(f'Customer # {account_no} not found in our system.')
        else:
            with open(r'customers.json', 'w') as f:
                json.dump(Customer.crm, f, indent=4)
            mydb = mysql.connector.connect(**db_cred)
            mycursor = mydb.cursor()
            mycursor.execute('DELETE FROM customer WHERE acc = %s', (account_no, ))
            mydb.commit()
            mycursor.close()
            mydb.close()
            # Customer.mycursor.execute('DELETE FROM customer WHERE acc = %s', (account_no, ))
            # Customer.mydb.commit()
            print(f'Customer # {account_no} successfully deleted.')

    def recent_tier(account_no, location, ship_date=str(date.today())):
        account_no = str(account_no)
        if ship_date.count('-') == 2:
            ship_date = datetime.strptime(ship_date, '%Y-%m-%d').strftime('%Y-%m-%d')
        elif ship_date.count('/') == 2:
            ship_date = datetime.strptime(ship_date, '%m/%d/%Y').strftime('%Y-%m-%d')
        dates_to_rates = Customer.crm[account_no]["tiers"][location]
        # dates = list(dates_to_rates.keys())
        i_l = 0
        i_r = len(dates_to_rates)-1
        if not dates_to_rates or ship_date < dates_to_rates[0][0]:
            return None
        if ship_date >= dates_to_rates[-1][0]:
            return dates_to_rates[-1][1]
        while i_l <= i_r:
            m = (i_r+i_l)//2
            if ship_date > dates_to_rates[m][0]:
                i_l = m+1
            elif ship_date < dates_to_rates[m][0]:
                i_r = m-1
            else:
                return dates_to_rates[m][1]
        if ship_date >= dates_to_rates[m][0]:
            return dates_to_rates[m][1]
        else:
            return dates_to_rates[m-1][1]

    # Might need to be fixed since other funcs of class were updated

    def outdated_recent_tier_accounts(date):
        outdated = []
        for c in Customer.crm:
            if 'domestic' in Customer.crm[c]['tiers'] and Customer.crm[c]['tiers']['domestic'] and Customer.recent_tier(c, 'domestic')[0] < date:
                recent_tier = Customer.recent_tier(c, 'domestic')
                outdated.append((c, Customer.crm[c]['name'], 'domestic', recent_tier[0], recent_tier[1]))
            if 'international' in Customer.crm[c]['tiers'] and Customer.crm[c]['tiers']['international'] and Customer.recent_tier(c, 'international')[0] < date:
                recent_tier = Customer.recent_tier(c, 'international')
                outdated.append((c, Customer.crm[c]['name'], 'international', recent_tier[0], recent_tier[1]))
        if outdated:
            return outdated
        return None


def weight(actual_weight, zone, lg=0, wd=0, ht=0):
    # weight function returns the 'final weight' (max between actual and dimensional weight)
    # and True/False whether final weight is actual/dimensional based on following rule:
    # if shipping is domestic:
    #   if volume is less than or equal to 1728 cu. in., return actual weight
    #   otherwise return max between actual weight and dimensional weight (= volume/166*16 [oz])
    # if shipping is international:
    #   return max between actual weight and dimensional weight
    dim_weight = 0
    weight = actual_weight
    # If domestic (domestic zones start with 'USPS')
    if zone[: 4] == "USPS":
        dim_cutoff = 1728
        # Check if dimensions less than cutoff = 60
        # If less, weight is actual weight
        # Otherwise, weight is greater of actual weight and dimensional weight
        if lg*wd*ht <= dim_cutoff:
            weight = actual_weight
        else:
            dim_weight = lg*wd*ht/166*16
            weight = max(dim_weight, actual_weight)
    # If international, convert actual wight from lb to oz
    else:
        dim_weight = lg*wd*ht/166*16
        actual_weight *= 16
        weight = max(dim_weight, actual_weight)
    return (round(weight, 3), True if weight == actual_weight else False)


def acc_num(bol, deliv_confirm, zone, ol, cust_conf, int_tr_no, zip):
    account_no = None
    flag = 0
    # if len(cust_conf) >= 22:
    #     account_no = amp[1].get((cust_conf[-22:], zip.replace(' ', '').replace('-', '').upper()))
    # right 22 chars in invoice against full delivery confirmation of amp
    # if not in amp, check in customer tracking
    if zone[: 4] == "USPS":  # If domestic
        if overlabeled.get(deliv_confirm, None) == 'ENCODE':
            # return (account_no, -1)
            flag = -1
        else:
            bol = bol.lstrip('0')[: 7]
            mailer_id = Customer.crm[bol]['mailer id'] if bol in Customer.crm else None
            if mailer_id is None:
                # print("Flag - No account #", bol, "found")
                flag = -2
            else:
                deliv_confirm = ol if ol != "" else deliv_confirm
                deliv_confirm_mailer_id = deliv_confirm.lstrip('0')[5: 14]
                if str(mailer_id) != deliv_confirm_mailer_id and deliv_confirm_mailer_id[: 6] != "699035":
                    # print("delivery confirmaion mailer id doesn't match.", mailer_id, deliv_confirm_mailer_id, bol, row[2])
                    flag = -3
                else:
                    amp_pickup_no = amp[1].get((int(deliv_confirm), int(zip[:5])))
                    if amp_pickup_no and int(amp_pickup_no) != int(bol):
                        # print("match in amp corresponds to different pickup #")
                        flag = -4
                    elif len(cust_conf) >= 30 and cust_conf[: 2] == "42":
                        # print("length of customer conf. # greater than 30 and first two characters are 42")
                        flag = -5
                    elif int_tr_no[0] not in ('2', '3', '4'):
                        # print("Internal tracking # not starting with 2, 3, or 4:", int_tr_no)
                        flag = -6
        if not flag:
            return (int(bol), flag)
        elif len(cust_conf) >= 22 and cust_conf[-22:].isnumeric():
            account_no = amp[1].get((int(cust_conf[-22:]), int(zip[:5]))
                                    ) or tr_to_acc.get((int(cust_conf[-22:]), int(zip[:5])))
            if account_no:
                flag = -9
        return(account_no, flag)
    else:
        # International
        if len(cust_conf) < 12:
            # Length of cust. conf. # less than 20:"
            return (account_no, -7)
        if cust_conf[: 2] == 'GM' and len(cust_conf) == 20:
            return (int(cust_conf[4: 11]), 0)
        account_no = amp[0].get(cust_conf)
        # look up tracking # and zipcode, return third column
        # return amp[1].get(cust_conf, None)
        if not account_no:
            # print("Cust. conf. #", cust_conf, "not found in AMP")
            account_no = tr_to_acc.get(cust_conf)
            if account_no:
                return (account_no, 0)
            return (None, -8)
        return (account_no, 0)
        # domestic - delivery, intl. - customer


def route_acc(acc, ship_date):
    if acc[0] == 5350903 and ship_date >= date(2020, 11, 22):
        return (5355340, acc[1])
    elif acc[0] == 5354264 and ship_date >= date(2020, 11, 15):
        return (5361372, acc[1])
    elif acc[0] == 5344480:
        return (5348587, acc[1])
    elif acc[0] == 5359915:
        if ship_date >= date(2021, 1, 22) and ship_date <= date(2021, 1, 27):
            return (5367056, acc[1])
        elif ship_date > date(2021, 1, 27):
            return (5367469, acc[1])
    # elif acc[0] ==  and ship_date >= date(2021, 1, 28):
    #     return (, acc[1])
    return acc


def surcharge(service_code, domestic, ship_date, weight, lg=0, wd=0, ht=0):
    total_our = {'per_lb': 0, 'temp': 0, '70_261': 0}
    total_dhl = {'per_lb': 0, 'temp': 0}

    if not domestic:
        return (total_our, total_dhl)
    nqd = {76, 77, 81, 82, 383, 384, 631}  # non-qualified dimensions
    if service_code == 70:
        total_our['70_261'] = 2
    elif service_code == 261:
        total_our['70_261'] += 1
    dim_sorted = sorted([lg, wd, ht])
    weight /= 16
    if service_code in nqd and (dim_sorted[2] > 27 or dim_sorted[2] + 2*(dim_sorted[0]+dim_sorted[1]) > 50):
        total_our['per_lb'] = 1.5*ceil(weight)
        total_dhl['per_lb'] = 1.5*ceil(weight)
    if date(2020, 10, 18) <= ship_date <= date(2020, 12, 27):
        if service_code == 261:
            total_our['temp'] = 0.25
            total_dhl['temp'] = 0.25
        elif service_code == 70:
            total_our['temp'] = 0.4
            total_dhl['temp'] = 0.4
        elif service_code in (81, 82, 631):
            total_dhl['temp'] = 0.29
            if weight < 1:
                total_our['temp'] = 0.29
            else:
                total_our['temp'] = 0.34
    return (total_our, total_dhl)


def update_amp(file, amp_update0={}, amp_update1={}):
    if os.path.exists(r'dependencies\Shipping Variance\amp.pkl'):
        with open(r'dependencies\Shipping Variance\amp.pkl', 'rb') as f:
            amp_curr = pickle.load(f)
    else:
        amp_curr = [{}, {}]
    with open(file, 'r', encoding='unicode_escape') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            if len(row) > 4:
                if row[-3].isdigit():
                    if len(row[1]) > 1:
                        amp_update1[(int(row[1][1:]), int(row[8][1:6]))] = int(row[-3])
                    elif len(row[0]) > 1:
                        amp_update0[row[0][1:]] = int(row[-3])
            else:
                if row[3].isdigit():
                    if len(row[1]) > 1:
                        amp_update1[(int(row[1][1:]), int(row[2][1:6]))] = int(row[3])
                    elif len(row[0]) > 1:
                        amp_update0[row[0][1:]] = int(row[3])
        amp_curr[0].update(amp_update0)
        amp_curr[1].update(amp_update1)
        with open(r'dependencies\Shipping Variance\amp.pkl', 'wb') as f:
            pickle.dump(amp_curr, f, pickle.HIGHEST_PROTOCOL)


# add location (domestic/international to file and adjust code to add location)
def update_service(file):
    with open(file, 'r') as f:
        reader = csv.reader(f)
        dhl_to_ai1s = {int(rows[0]): (int(rows[1]), int(rows[2]), int(rows[3]), rows[4]) for rows in reader}
        # with open(r'dependencies\services\dhl_service_hash.pkl', 'wb') as fw:
        #     pickle.dump(dhl_to_ai1s, fw, pickle.HIGHEST_PROTOCOL)
        with open(r'dependencies\services\dhl_service_hash.json', 'w') as fw:
            pickle.dump(dhl_to_ai1s, fw, indent=4)


def intl_zone_to_name(file):
    with open(file, 'r') as f:
        reader = csv.reader(f)
        intl_names = {rows[0]: rows[1] for rows in reader}
        with open(r'dependencies\zones\intl zone names.pkl', 'wb') as fw:
            pickle.dump(intl_names, fw, pickle.HIGHEST_PROTOCOL)


def create_profit_report():
    sql = "SELECT account, bill_date, SUM(IFNULL(charge+surcharges, 0)-IFNULL(total_charges_dhl, 0)) profit FROM dhl_master_invoice \
    WHERE account > 0 AND bill_date IS NOT NULL GROUP BY account, bill_date ORDER BY bill_date DESC, account;"
    mydb = mysql.connector.connect(**db_cred)
    mycursor = mydb.cursor()
    mycursor.execute(sql)
    res = mycursor.fetchall()
    mycursor.close()
    mydb.close()
    # columns = [desc[0] for desc in mycursor.description]
    columns = ['account', 'bill_date', 'profit']
    print(columns)
    # res = Customer.mycursor.fetchall()
    # columns = [desc[0] for desc in Customer.mycursor.description]

    # Customer.mycursor.execute(sql)
    # res = Customer.mycursor.fetchall()
    # columns = [desc[0] for desc in Customer.mycursor.description]
    df = pd.DataFrame([tuple(t) for t in res], columns=columns)
    df['account'] = df.apply(lambda row: Customer.crm[str(row['account'])].get('name')
                             if str(row['account']) in Customer.crm else None, axis=1)
    df['profit'] = df['profit'].astype(float)
    df = pd.pivot_table(df, values='profit', index=['account'], columns='bill_date')
    cols = sorted(df.columns.tolist(), reverse=True)
    df = df[cols]
    # print(df.head(10))
    file = r'Customer Profit by Week\Customer Profit by Week.xlsx'
    writer = pd.ExcelWriter(file, engine='openpyxl')
    df.to_excel(writer, sheet_name='Sheet1', freeze_panes=(1, 0))
    writer.save()
    writer.close()


def duplicate_check(potential_dupl, del_confirm_to_shipment=(),
                    cust_confirm_to_shipment=(), ol_to_shipment=()):
    log = {}
    sql_dupl_deliv = 'SELECT delivery_confirm, ship_date, material_or_vas_num, invoice FROM dhl_master_invoice \
        WHERE delivery_confirm = %s AND (delivery_confirm, ship_date, material_or_vas_num) != (%s, %s, %s) AND material_or_vas_num NOT IN (154, 192)'
    sql_dupl_cust = 'SELECT customer_confirm, delivery_confirm, ship_date, material_or_vas_num, invoice FROM dhl_master_invoice \
        WHERE RIGHT(customer_confirm, 22) = %s AND (delivery_confirm, ship_date, material_or_vas_num) != (%s, %s, %s) AND material_or_vas_num NOT IN (154, 192)'
    sql_dupl_ol = 'SELECT overlabeled_value, delivery_confirm, ship_date, material_or_vas_num, invoice FROM dhl_master_invoice \
        WHERE overlabeled_value = %s AND (delivery_confirm, ship_date, material_or_vas_num) != (%s, %s, %s) AND material_or_vas_num NOT IN (154, 192)'
    sql_update_history_duplicate = 'UPDATE dhl_master_invoice SET account = -account, bill_date = NULL WHERE account > 0 AND (delivery_confirm, ship_date, material_or_vas_num) = (%s, %s, %s)'
    sql_update_invoice_duplicate = 'UPDATE dhl_master_invoice SET account = -account, bill_date = NULL WHERE account > 0 AND (delivery_confirm, ship_date, material_or_vas_num) IN '
    mydb = mysql.connector.connect(**db_cred)
    mycursor = mydb.cursor()
    for rec in potential_dupl:
        if rec[-1][1] == -9:  # account was flagged and then found in amp or customer tracking - checking for duplicates against history
            mycursor.execute(sql_dupl_deliv, [rec[0]]+rec[:3])
            # Customer.mycursor.execute(sql_dupl_deliv, [rec[0]]+rec[:3])
            duplicate = mycursor.fetchall()
            # duplicate = Customer.mycursor.fetchall()
            if duplicate:
                for d in duplicate:
                    print(d)
                    log[(
                        rec[-2], 'flagged duplicate found (delivery_confirm)')] = f'Shipment {tuple(rec[:3])} delivery_confirm - {rec[0]} - found in shipment {tuple(d[:3])} delivery_confirm - {d[0]}'
                mycursor.execute(sql_update_history_duplicate, rec[:3])
                # Customer.mycursor.execute(sql_update_history_duplicate, rec[:3])
            mycursor.execute(sql_dupl_ol, [rec[0]]+rec[:3])
            # Customer.mycursor.execute(sql_dupl_ol, [rec[0]]+rec[:3])
            # duplicate = Customer.mycursor.fetchall()
            duplicate = mycursor.fetchall()
            if duplicate:
                for d in duplicate:
                    print(d)
                    log[(
                        rec[-2], 'flagged duplicate found (overlabeled)')] = f'Shipment {tuple(rec[:3])} delivery_confirm - {rec[0]} - found in shipment {tuple(d[1:4])} overlabeled - {d[0]}'
                mycursor.execute(sql_update_history_duplicate, rec[:3])
            mycursor.execute(sql_dupl_cust, [rec[0]]+rec[:3])
            duplicate = mycursor.fetchall()
            #     Customer.mycursor.execute(sql_update_history_duplicate, rec[:3])
            # Customer.mycursor.execute(sql_dupl_cust, [rec[0]]+rec[:3])
            # duplicate = Customer.mycursor.fetchall()
            if duplicate:
                for d in duplicate:
                    print(d)
                    log[(
                        rec[-2], 'flagged duplicate found (cust_confirm)')] = f'Shipment {tuple(rec[:3])} delivery_confirm - {rec[0]} - found in shipment {tuple(d[1:4])} cust_confirm - {d[0]}'
                mycursor.execute(sql_update_history_duplicate, rec[:3])
                # Customer.mycursor.execute(sql_update_history_duplicate, rec[:3])
        else:  # account passed all the flags - checking for duplicates against invoice
            if rec[0] in del_confirm_to_shipment:
                if len(del_confirm_to_shipment[rec[0]]) - (tuple(rec[:3]) in del_confirm_to_shipment[rec[0]]) > 0:
                    log[(
                        rec[-2], 'unflagged duplicate found (delivery_confirm)')] = f'Shipment {tuple(rec[:3])} delivery_confirm - {rec[0]} - found in shipment(s) {del_confirm_to_shipment[rec[0]]} delivery_confirm'
                    mycursor.execute(sql_update_invoice_duplicate +
                                     str(tuple(del_confirm_to_shipment[rec[0]]+[tuple(rec[:3])])))
                    # Customer.mycursor.execute(sql_update_invoice_duplicate +
                    #                           str(tuple(del_confirm_to_shipment[rec[0]]+[tuple(rec[:3])])))
            if rec[0] in cust_confirm_to_shipment:
                if len(cust_confirm_to_shipment[rec[0]]) - (tuple(rec[:3]) in cust_confirm_to_shipment[rec[0]]) > 0:
                    log[(
                        rec[-2], 'unflagged duplicate found (overlabeled)')] = f'Shipment {tuple(rec[:3])} delivery_confirm - {rec[0]} - found in shipment(s) {del_confirm_to_shipment[rec[0]]} overlabeled'
                    mycursor.execute(sql_update_invoice_duplicate +
                                     str(tuple(cust_confirm_to_shipment[rec[0]]+[tuple(rec[:3])])))

                    # Customer.mycursor.execute(sql_update_invoice_duplicate +
                    #                           str(tuple(cust_confirm_to_shipment[rec[0]]+[tuple(rec[:3])])))
            if rec[0] in ol_to_shipment:
                if len(ol_to_shipment[rec[0]]) - (tuple(rec[:3]) in ol_to_shipment[rec[0]]) > 0:
                    log[(
                        rec[-2], 'unflagged duplicate found (cust_confirm)')] = f'Shipment {tuple(rec[:3])} delivery_confirm - {rec[0]} - found in shipment(s) {del_confirm_to_shipment[rec[0]]} cust_confirm'
                    mycursor.execute(sql_update_invoice_duplicate +
                                     str(tuple(ol_to_shipment[rec[0]]+[tuple(rec[:3])])))
                    # Customer.mycursor.execute(sql_update_invoice_duplicate +
                    #                           str(tuple(ol_to_shipment[rec[0]]+[tuple(rec[:3])])))
    mycursor.close()
    mydb.close()
    return log

#bol_number, pricing_zone, internal_tracking, recipient_zip


def history_duplicates(records=None):
    print('Selecting all potential duplicates...')
    mydb = mysql.connector.connect(**db_cred)
    mycursor = mydb.cursor()
    if records:
        sql_select_existing_duplicates = 'SELECT ship_date, service, delivery_confirm FROM duplicates'
        mycursor.execute(sql_select_existing_duplicates)
        # Customer.mycursor.execute(sql_select_existing_duplicates)
        existing_duplicates = set()
        while True:
            existing_duplicate = mycursor.fetchone()
            # existing_duplicate = Customer.mycursor.fetchone()
            if not existing_duplicate:
                break
            existing_duplicates.add(existing_duplicate)
        shipments_to_check = []
        for rec in records:
            if rec[:3] not in existing_duplicates:
                shipments_to_check.append(rec)
        print(shipments_to_check)
        # dhl_master_invoice.ship_date, material_or_vas_num, dhl_master_invoice.delivery_confirm, RIGHT(customer_confirm, 22), \
        #     account, bol_number, pricing_zone, internal_tracking, recipient_zip
    else:
        sql_select = 'SELECT dhl_master_invoice.ship_date, material_or_vas_num, dhl_master_invoice.delivery_confirm, RIGHT(customer_confirm, 22), \
            account FROM dhl_master_invoice LEFT JOIN duplicates ON dhl_master_invoice.delivery_confirm = duplicates.delivery_confirm \
            AND dhl_master_invoice.ship_date = duplicates.ship_date AND material_or_vas_num = service \
            WHERE account < 0 AND account >= -9 AND LEFT(pricing_zone, 4) = "USPS" AND material_or_vas_num NOT IN (154, 192) \
            AND duplicates.delivery_confirm IS NULL'
        mycursor.execute(sql_select)
        # Customer.mycursor.execute(sql_select)
        shipments_to_check = []
        while True:
            current_shipment = mycursor.fetchone()
            # current_shipment = Customer.mycursor.fetchone()
            if current_shipment:
                shipments_to_check.append(current_shipment)
            else:
                break
    print('Done...')
    if not shipments_to_check:
        print('No potential duplicates to check...')
        return
    print('Starting history duplicates check...')
    print('Selecting all domestic shipments...')
    mycursor.execute('SELECT ship_date, material_or_vas_num, delivery_confirm, RIGHT(customer_confirm, 22), overlabeled_value, account \
        FROM dhl_master_invoice \
        WHERE LEFT(pricing_zone, 4) = "USPS" AND material_or_vas_num NOT IN (154, 192)')
    dom_shipments = mycursor.fetchall()
    # Customer.mycursor.execute('SELECT ship_date, material_or_vas_num, delivery_confirm, RIGHT(customer_confirm, 22), overlabeled_value, account \
    #     FROM dhl_master_invoice \
    #     WHERE LEFT(pricing_zone, 4) = "USPS" AND material_or_vas_num NOT IN (154, 192)')
    # dom_shipments = Customer.mycursor.fetchall()
    print('Done...')
    d_to_shipment = {}
    c_to_shipment = {}
    ol_to_shipment = {}
    duplicates = {}
    log = {}
    for row in dom_shipments:
        ship_date, service, del_conf, cust_conf, ol, account = row[0], row[1], row[2], row[3], str(
            row[4]) if row[4] else row[4], row[5]
        if del_conf not in d_to_shipment:
            d_to_shipment[del_conf] = []
        d_to_shipment[del_conf].append((ship_date, service, del_conf))
        if cust_conf:
            if cust_conf not in c_to_shipment:
                c_to_shipment[cust_conf] = []
            c_to_shipment[cust_conf].append((ship_date, service, del_conf))
        if ol:
            if ol not in ol_to_shipment:
                ol_to_shipment[ol] = []
            ol_to_shipment[ol].append((ship_date, service, del_conf))
    for row in shipments_to_check:
        ship_date, service, del_conf, cust_conf, account = row[0], row[1], row[2], row[3], row[4]
        shipment = (ship_date, service, del_conf)
        if cust_conf in d_to_shipment.keys():
            if len(d_to_shipment[cust_conf]) - (shipment in d_to_shipment[cust_conf]) > 0:
                log[shipment] = f'account: {account}, shipment {shipment} customer confirm - {cust_conf} matches the delivery confirm of the following shipments: {d_to_shipment[cust_conf]}'
                duplicates[shipment] = account
        if cust_conf in c_to_shipment.keys():
            if len(c_to_shipment[cust_conf]) - (shipment in c_to_shipment[cust_conf]) > 0:
                log[shipment] = f'account: {account}, shipment {shipment} customer confirm - {cust_conf} matches the customer confirm right 22 of the following shipments: {c_to_shipment[cust_conf]}'
                duplicates[shipment] = account
        if cust_conf in ol_to_shipment.keys():
            if len(ol_to_shipment[cust_conf]) - (shipment in ol_to_shipment[cust_conf]) > 0:
                log[shipment] = f'account: {account}, shipment {shipment} customer confirm - {cust_conf} matches the overlabeled of the following shipments: {ol_to_shipment[cust_conf]}'
                duplicates[shipment] = account
    if duplicates:
        print(log)
        sql_duplicates = list(duplicates.keys())
        print(f'{len(duplicates)} duplicates found - updating dB...')
        sql_add_duplicates = 'INSERT INTO duplicates (ship_date, service, delivery_confirm) VALUES (%s, %s, %s)'
        mycursor.executemany(sql_add_duplicates, sql_duplicates)
        # Customer.mycursor.executemany(sql_add_duplicates, sql_duplicates)
        sql_negate_duplicates = 'UPDATE dhl_master_invoice SET account = - account, bill_date = NULL WHERE (ship_date, material_or_vas_num, delivery_confirm) IN ((%s, %s, %s)) AND account > 0'
        mycursor.executemany(sql_negate_duplicates, sql_duplicates)
        mydb.commit()
        # Customer.mycursor.executemany(sql_negate_duplicates, sql_duplicates)
        # Customer.mydb.commit()
        print(f'{len(duplicates)} new customer confirm duplicates added to dB.')
    else:
        print('No new customer confirm duplicates found.')
    mycursor.close()
    mydb.close()
    if duplicates:
        return log
    # if uniques_account_found:


# continue invoice duplicates and history duplicates v2 tomorrow
# del_confirm, ship_date, inv_sc
def invoice_duplicates(invoice, records):
    print('Starting invoice duplicates check...')
    mydb = mysql.connector.connect(**db_cred)
    mycursor = mydb.cursor()
    sql_select_existing_duplicates = 'SELECT ship_date, service, delivery_confirm FROM duplicates'
    mycursor.execute(sql_select_existing_duplicates)
    # Customer.mycursor.execute(sql_select_existing_duplicates)
    existing_duplicates = set()
    while True:
        existing_duplicate = mycursor.fetchone()
        # existing_duplicate = Customer.mycursor.fetchone()
        if not existing_duplicate:
            break
        existing_duplicates.add(existing_duplicate)
    print('Selecting all potential duplicates...')
    shipments_to_check = []
    for current_shipment in records:
        if current_shipment[:3] not in existing_duplicates:
            shipments_to_check.append(current_shipment)
    print('Done...')
    if not shipments_to_check:
        print('No potential duplicates to check...')
        return
    print(f'Selecting all domestic shipments within invoice {invoice}...')
    mycursor.execute(f'SELECT ship_date, material_or_vas_num, delivery_confirm, RIGHT(customer_confirm, 22), overlabeled_value, account \
        FROM dhl_master_invoice \
        WHERE LEFT(pricing_zone, 4) = "USPS" AND material_or_vas_num NOT IN (154, 192) AND invoice = {invoice}')
    dom_shipments = mycursor.fetchall()
    # Customer.mycursor.execute(f'SELECT ship_date, material_or_vas_num, delivery_confirm, RIGHT(customer_confirm, 22), overlabeled_value, account \
    #     FROM dhl_master_invoice \
    #     WHERE LEFT(pricing_zone, 4) = "USPS" AND material_or_vas_num NOT IN (154, 192) AND invoice = {invoice}')
    # dom_shipments = Customer.mycursor.fetchall()
    print('Done...')
    d_to_shipment = {}
    c_to_shipment = {}
    ol_to_shipment = {}
    duplicates = {}
    log = {}
    for row in dom_shipments:
        ship_date, service, del_conf, cust_conf, ol, account = row[0], row[1], row[2], row[3], str(
            row[4]) if row[4] else row[4], row[5]
        if del_conf not in d_to_shipment:
            d_to_shipment[del_conf] = []
        d_to_shipment[del_conf].append((ship_date, service, del_conf))
        if cust_conf:
            if cust_conf not in c_to_shipment:
                c_to_shipment[cust_conf] = []
            c_to_shipment[cust_conf].append((ship_date, service, del_conf))
        if ol:
            if ol not in ol_to_shipment:
                ol_to_shipment[ol] = []
            ol_to_shipment[ol].append((ship_date, service, del_conf))
    for row in shipments_to_check:
        ship_date, service, del_conf, account = row[0], row[1], row[2], row[3]
        shipment = (ship_date, service, del_conf)
        if del_conf in d_to_shipment.keys():
            if len(d_to_shipment[del_conf]) - (shipment in d_to_shipment[del_conf]) > 0:
                log[shipment] = f'account: {account}, shipment {shipment} delivery confirm - {del_conf} matches the delivery confirm of the following shipments: {d_to_shipment[del_conf]}'
                duplicates[shipment] = account
        if del_conf in c_to_shipment.keys():
            if len(c_to_shipment[del_conf]) - (shipment in c_to_shipment[del_conf]) > 0:
                log[shipment] = f'account: {account}, shipment {shipment} delivery confirm - {del_conf} matches the customer confirm right 22 of the following shipments: {c_to_shipment[del_conf]}'
                duplicates[shipment] = account
        if del_conf in ol_to_shipment.keys():
            if len(ol_to_shipment[del_conf]) - (shipment in ol_to_shipment[del_conf]) > 0:
                log[shipment] = f'account: {account}, shipment {shipment} delivery confirm - {del_conf} matches the overlabeled of the following shipments: {ol_to_shipment[del_conf]}'
                duplicates[shipment] = account
    if duplicates:
        print(log)
        sql_duplicates = list(duplicates.keys())
        print(f'{len(duplicates)} duplicates found - updating dB...')
        sql_add_duplicates = 'INSERT INTO duplicates (ship_date, service, delivery_confirm) VALUES (%s, %s, %s)'
        mycursor.executemany(sql_add_duplicates, sql_duplicates)
        sql_negate_duplicates = 'UPDATE dhl_master_invoice SET account = - account WHERE (ship_date, material_or_vas_num, delivery_confirm) IN ((%s, %s, %s)) AND account > 0'
        mycursor.executemany(sql_negate_duplicates, sql_duplicates)
        mydb.commit()
        # Customer.mycursor.executemany(sql_add_duplicates, sql_duplicates)
        # sql_negate_duplicates = 'UPDATE dhl_master_invoice SET account = - account WHERE (ship_date, material_or_vas_num, delivery_confirm) IN ((%s, %s, %s)) AND account > 0'
        # Customer.mycursor.executemany(sql_negate_duplicates, sql_duplicates)
        # Customer.mydb.commit()
        print(f'{len(duplicates)} new delivery confirm duplicates added to dB.')
        # return log
    else:
        print('No new delivery confirm duplicates found.')
    mycursor.close()
    mydb.close()
    if duplicates:
        return log
