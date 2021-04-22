import json
import re
import csv
from datetime import date
import numpy as np
import pandas as pd
import lib
import os
from numpy import nan, random, int64
# from pd import DatetimeIndex
# with open(r'Manifests\format.json', 'r') as f:
#     format = json.load(f)
with open(r'dependencies\services\sv_to_code.json', 'r') as f:
    sv_to_code = json.load(f)


class ManifestFormat:
    if os.path.exists(r'Manifests\format.json'):
        with open(r'Manifests\format.json', 'r') as f:
            format = json.load(f)
    else:
        format = {}

    def __init__(self, sw, order_no=None, date=None, weight=None, service=None, current_price=None, insured_parcel=None, address=None, address1=None, dim1=None, dim2=None, dim3=None):
        self.sw = sw
        self.order_no = order_no
        self.date = date
        self.weight = weight
        self.service = service
        self.current_price = current_price
        self.insured_parcel = insured_parcel
        self.address = address
        self.address1 = address1
        self.dim1 = dim1
        self.dim2 = dim2
        self.dim3 = dim3
        if ManifestFormat.format.get('order_no') is None:
            ManifestFormat.format['order_no'] = {}
        if ManifestFormat.format['order_no'].get(sw) is None and order_no:
            ManifestFormat.format['order_no'][sw] = order_no

        if ManifestFormat.format.get('date') is None:
            ManifestFormat.format['date'] = {}
        if ManifestFormat.format['date'].get(sw) is None and date:
            ManifestFormat.format['date'][sw] = date

        if ManifestFormat.format.get('weight') is None:
            ManifestFormat.format['weight'] = {}
        if ManifestFormat.format['weight'].get(sw) is None and weight:
            ManifestFormat.format['weight'][sw] = weight

        if ManifestFormat.format.get('service') is None:
            ManifestFormat.format['service'] = {}
        if ManifestFormat.format['service'].get(sw) is None and service:
            ManifestFormat.format['service'][sw] = service

        if ManifestFormat.format.get('current_price') is None:
            ManifestFormat.format['current_price'] = {}
        if ManifestFormat.format['current_price'].get(sw) is None and current_price:
            ManifestFormat.format['current_price'][sw] = current_price

        if ManifestFormat.format.get('insured_parcel') is None:
            ManifestFormat.format['insured_parcel'] = {}
        if ManifestFormat.format['insured_parcel'].get(sw) is None and insured_parcel:
            ManifestFormat.format['insured_parcel'][sw] = insured_parcel

        if ManifestFormat.format.get('address') is None:
            ManifestFormat.format['address'] = {}
        if ManifestFormat.format['address'].get(sw) is None and address:
            ManifestFormat.format['address'][sw] = address

        if ManifestFormat.format.get('address1') is None:
            ManifestFormat.format['address1'] = {}
        if ManifestFormat.format['address1'].get(sw) is None and address1:
            ManifestFormat.format['address1'][sw] = address1

        if ManifestFormat.format.get('dim1') is None:
            ManifestFormat.format['dim1'] = {}
        if ManifestFormat.format['dim1'].get(sw) is None and dim1:
            ManifestFormat.format['dim1'][sw] = dim1

        if ManifestFormat.format.get('dim2') is None:
            ManifestFormat.format['dim2'] = {}
        if ManifestFormat.format['dim2'].get(sw) is None and dim2:
            ManifestFormat.format['dim2'][sw] = dim2

        if ManifestFormat.format.get('dim3') is None:
            ManifestFormat.format['dim3'] = {}
        if ManifestFormat.format['dim3'].get(sw) is None and dim3:
            ManifestFormat.format['dim3'][sw] = dim3

        # if CarrierCharge.map[carrier][location][date][service_code][ship_zone].get(weight) is None:
        with open(r'Manifests\format.json', 'w') as f:
            json.dump(ManifestFormat.format, f, indent=4)

    def __str__(self):
        return (f"Software: {self.sw}\nSoftware: {self.sw}\nOrder No: {self.order_no}\n\
        Date: {self.date}\nWeight: {self.weight}\nService: {self.service}\n\
        Price: {self.current_price}\nInsured Parcel: {self.insured_parcel}\nAddress: {self.address}\n\
        Address1: {self.address1}\nDim1: {self.dim1}\nDim2: {self.dim2}\nDim3: {self.dim3}\n")
        # self, sw, order_no, date, weight, service, current_price, insured_parcel, address

    def sw(self):
        return self.sw

    def order_no(self):
        return self.order_no

    def date(self):
        return self.date

    def weight(self):
        return self.weight

    def service(self):
        return self.service

    def current_price(self):
        return self.current_price

    def insured_parcel(self):
        return self.insured_parcel

    def address(self):
        return self.address

    def update_sw_format(sw, order_no=None, date=None, weight=None, service=None, current_price=None, insured_parcel=None, address=None, address1=None, dim1=None, dim2=None, dim3=None):
        if order_no:
            ManifestFormat.format['order_no'][sw] = order_no
        if date:
            ManifestFormat.format['date'][sw] = date
        if weight:
            ManifestFormat.format['weight'][sw] = weight
        if service:
            ManifestFormat.format['service'][sw] = service
        if current_price:
            ManifestFormat.format['current_price'][sw] = current_price
        if insured_parcel:
            ManifestFormat.format['insured_parcel'][sw] = insured_parcel
        if address:
            ManifestFormat.format['address'][sw] = address
        if address1:
            ManifestFormat.format['address1'][sw] = address
        if dim1:
            ManifestFormat.format['dim1'][sw] = dim1
        if dim2:
            ManifestFormat.format['dim2'][sw] = dim1
        if dim3:
            ManifestFormat.format['dim3'][sw] = dim1
        with open(r'Manifests\format.json', 'w') as f:
            json.dump(ManifestFormat.format, f, indent=4)
        # zip = format['zip']['sellercloud_shipbridge']
        # country = format['country']['sellercloud_shipbridge']
        # print(format.keys())
        # print(country)
        # print(zip)
        # format['address'] = {'sellercloud_shipbridge': {'header': 'Address', 'format': 'str',
        #                                                      'z_parse': ['split', ', ', '-1'], '+4': True,
        #                                                      'c_parse': ['split', ', ', '-2'], 'c_abbv': False}}
        # format.pop('zip')
        # format.pop('country')
        # for k in format:
        #     print(k, format[k])
        # with open(r'Manifests\format.json', 'w') as f:
        #     json.dump(format, f, indent=4)
        # print(format)


print(ManifestFormat.format)


def w_lbs_or_w_oz(string):
    string = string.rstrip(' ')
    (val, unit) = string.split(' ')
    val = float(val)
    if unit in ('lb', 'lbs'):
        val *= 16
    return val


def add_to_zip_ctry(add):
    add_split = add.split(', ')
    n = len(add_split)-2
    for i in range(n):
        if re.search(r'\d', add_split[n-i]) or not add_split[n-i]:
            zip = add_split[n-i]
            country = ' '.join(add_split[n-i+1:])
            return (zip, country)


def sv_to_code_import(file):
    with open(file, encoding="unicode_escape") as f:
        csv_reader = csv.reader(f, delimiter=',')
        next(csv_reader)
        for row in csv_reader:
            sv_name = re.sub(r'[^\w]', '', row[0]).lower()
            if sv_name not in sv_to_code:
                sv_to_code[sv_name] = {}
            dom_intl = row[1].lower().replace('us', 'domestic').replace('intl', 'international')
            if dom_intl not in sv_to_code[sv_name]:
                sv_to_code[sv_name][dom_intl] = {}
            over_under = row[2]
            if over_under not in sv_to_code[sv_name][dom_intl]:
                service_no = lib.service_names.get(row[3])
                # print()

                sv_to_code[sv_name][dom_intl][over_under] = int(service_no) if row[3] else 0
    with open(r'dependencies\services\sv_to_code.json', 'w') as f:
        json.dump(sv_to_code, f, indent=4)


def random_dates(start, end, n):
    start_u = start.value//10**9
    end_u = end.value//10**9
    return pd.DatetimeIndex((10**9*np.random.randint(start_u, end_u, n, dtype=np.int64)).view('M8[ns]'))
#
#
# def rand_date_str(row):
#     return str(row['shipdate (gen.)'])[:10]

# with open(r'manifests\mnl.csv', 'r', encoding='utf-8') as f:
#     reader = csv.reader(f)
#     service_override = {}
#     for row in reader:
#         sv_name_ovr = re.sub(r'[^\w]', '', row[0]).lower()
#         dom_intl_ovr = 'domestic' if row[2] in ('United States', 'US') else 'international'
#         if sv_name_ovr not in service_override:
#             service_override[sv_name_ovr] = {}
#         if dom_intl_ovr not in service_override[sv_name_ovr]:
#             service_override[sv_name_ovr][dom_intl_ovr] = {}
#         if row[1] not in service_override[sv_name_ovr]:
#             service_override[sv_name_ovr][dom_intl_ovr][row[1]] = {}
#         service_override[sv_name_ovr][dom_intl_ovr][row[1]] = int(row[3])
# print(service_override)


def service(sv_name, ctry_code, weight):
    # OVERRIDE:

    # sv_to_code = service_override
    dom_intl = 'domestic' if ctry_code == 'US' else 'international'
    if dom_intl == 'domestic':
        weight_thres = '<' if weight < 16 else '>='
    else:
        weight_thres = '<' if weight < 70.4 else '>='
    if sv_name in sv_to_code:
        if dom_intl in sv_to_code[sv_name]:
            if weight_thres in sv_to_code[sv_name][dom_intl]:
                return [sv_to_code[sv_name][dom_intl][weight_thres], lib.service[str(sv_to_code[sv_name][dom_intl][weight_thres])][3], weight_thres]
            else:
                print(f'Weight threshold not found for service name: {sv_name}.')
        else:
            print(f'{dom_intl} not found for service name: {sv_name}.')
    else:
        print(f'Service name {sv_name} not found')
    return [None, None, weight_thres]


def row_to_rate(row):
    shipdate = row.shipdate.date()
    if len(row.country) == 2 and row.country.isupper():
        ctry_code = lib.country_to_code.get(row.country)
    else:
        ctry_code = lib.country_to_code.get(re.sub(r'[^\w]', '', row.country).lower())
    domestic = True if ctry_code == 'US' else False
    sv_name = re.sub(r'[^\w]', '', row['service']).lower()
    sc = service(sv_name, ctry_code, row['weight'])
    if not ctry_code:
        print(f'{row.country} not in country to code hash table')
        return [None*2]+[sc[1]]+[None]*11
    if domestic:
        zip = row.zip.replace('-', '')
        if zip == 'N/A':
            zone = f"{int(row.zone[5:]):02d}"
            print(zone)
        else:
            if len(zip) <= 5:
                zip3 = int(zip[: len(zip)-2])
            else:
                zip3 = int(zip[: len(zip)-6])
            zone = lib.dhl_zip_zone_2020[(zip3)] if zip != 'N/A' else row['zone']
        # zone = lib.dhl_zip_zone_2020[(zip3)] if zip != 'N/A' else row['zone']
        if zone == '09':
            zone = '08'
        zone = "USPS" + zone
    else:
        if ctry_code == 'CA' and sc[0] == 60:
            zone = lib.ca_zip_zone[row.zip[:3]]
        else:
            zone = ctry_code
    zone_desc = 'Zone ' + zone[-2:] if domestic else zone
    if not sc[0]:
        return [ctry_code, zone_desc, sc[2], sc[1]]+[None]*10
    no_costs = []
    output = [ctry_code, zone_desc]+[sc[2], sc[1]]
    output.append(lib.CarrierCharge.charge_weight(
        'DHL', 'domestic' if domestic else 'international', str(shipdate), sc[0], zone, row['weight']))
    if domestic and int(zone[-2:]) in (11, 12, 13) and row['weight'] < 16:
        return output+[None]*9
    for tier in range(1, 6):
        charge = lib.CarrierCharge.charge_rate(
            tier, 'domestic' if domestic else 'international', str(date(2021, 2, 1)), sc[0], zone, row['weight'])
        charge = None if charge < 0 else charge
        output.append(charge)
    dhl_c_2021 = lib.CarrierCharge.charge_rate("DHL", "domestic" if domestic else "international",
                                               str(date(2021, 2, 1)), sc[0], zone, row['weight'])
    usps_c_2021 = lib.CarrierCharge.charge_rate("USPS", "domestic" if domestic else "international",
                                                str(date(2021, 2, 1)), sc[0], zone, row['weight'])
    dhl_c = lib.CarrierCharge.charge_rate("DHL", "domestic" if domestic else "international",
                                          str(shipdate), sc[0], zone, row['weight'])
    usps_c = lib.CarrierCharge.charge_rate("USPS", "domestic" if domestic else "international",
                                           str(shipdate), sc[0], zone, row['weight'])
    # rates.extend([lib.CarrierCharge.charge_rate('DHL', 'domestic' if domestic else 'international', date(2021, 2, 1), sc, zone, w), lib.CarrierCharge.charge_rate('USPS', 'domestic' if domestic else 'international', date(2021, 2, 1), sc, zone, w), our_c, dhl_c, usps_c, ship_date])

    output.extend([dhl_c_2021, usps_c_2021, dhl_c, usps_c])
    return output


cols_write = {'2021 tier 1': (1, False), '2021 tier 2': (2, False), '2021 tier 3': (3, False),
              '2021 tier 4': (4, False), '2021 tier 5': (5, False), '2021 DHL': ('DHL', False), '2021 USPS': ('USPS', False), 'shipdate DHL': ('DHL', True), 'shipdate USPS': ('USPS', True)}
cols_read = {'orderno': str, 'shipdate': str, 'weight': float, 'service': str,
             'zip': str, 'country': str, 'price': float, 'zone': str, 'weight threshold': str}
cols_service = {'service': str, 'weight threshold': str, 'country': str, 'sugg. service': str, 'Actual Service': str}

actual_service = {}


def update_services(row):
    if row['service'] is not nan:
        sv_name = re.sub(r'[^\w]', '', row['service']).lower()
        dom_intl = 'domestic' if row.country == 'US' else 'international'
        if row['sugg. service'] is not nan:
            if sv_name not in sv_to_code:
                sv_to_code[sv_name] = {}
            if dom_intl not in sv_to_code[sv_name]:
                sv_to_code[sv_name][dom_intl] = {}
            # Only updates if there is no existing suggested service - no overrides
            if row['weight threshold'] not in sv_to_code[sv_name][dom_intl]:
                sv_to_code[sv_name][dom_intl][row['weight threshold']] = int(lib.service_names[row['sugg. service']])
                with open(r'dependencies\services\sv_to_code.json', 'w') as f:
                    json.dump(sv_to_code, f, indent=4)
        if row['Actual Service'] is not nan:
            if sv_name not in actual_service:
                actual_service[sv_name] = {}
            if dom_intl not in actual_service[sv_name]:
                actual_service[sv_name][dom_intl] = {}
            if row['weight threshold'] not in actual_service[sv_name][dom_intl]:
                actual_service[sv_name][dom_intl][row['weight threshold']] = int(
                    lib.service_names[row['sugg. service']])


# continue here
def cost_correction(row):
    costs = []
    sv_name = re.sub(r'[^\w]', '', row['service']).lower()
    domestic = True if row.zone[:4] == 'Zone' else False
    zone = ('USPS' + row.zone[-2:]) if domestic else row.zone
    our_service = actual_service[sv_name]['domestic' if domestic else 'international'][row['weight threshold']
                                                                                       ] or sv_to_code[sv_name]['domestic' if domestic else 'international'][row['weight threshold']]
    our_service_name = lib.service[str(our_service)][3]
    if our_service == 0:
        costs += [None]*9
    else:
        for tier in cols_write.values():
            cost = lib.CarrierCharge.charge_rate(tier[0], "domestic" if domestic else "international",
                                                 str(row.shipdate) if tier[1] else '2021-02-01', our_service, zone, row['weight'])
            costs.append(cost if cost and cost >= 0 else None)
    return [our_service_name] + costs

    # (carrier, location, date, service_code, ship_zone, weight)


# print(lib.CarrierCharge.map['DHL']['domestic'][date(2021, 1, 24)][82].keys())


# Rouses Point NY, 12979, United States
# row = {'shipdate': date(2020, 1, 12), 'address': 'Carrigtohill Cork, , Ireland',
#        'weight': '1 oz', 'service': 'UPS SurePost 1 lb or greater'}
# print(row_to_rate(row))

# 'shipdate', columns[1]: 'weight', columns[2]: 'service', columns[3]: 'address'
# lib.CarrierCharge.charge_rate("DHL", "domestic" if domestic else "international",
#                                       ship_date, sc, zone, w[0])
# sv_name = 'USPS Priority'
# country = 'United States'
# weight = 13.2
# print(service(sv_name, country, weight))

# file = r'C:\Users\Roy Solmonovich\Downloads\Services2.csv'
# sv_to_code_import(file)

# sw, order_no=None, date=None, weight=None, service=None, current_price=None, insured_parcel=None, address=None, dimensions=None
order_no = {'header': 'OrderNumber', 'format': 'int'}
d = {'header': 'ShipDate', 'format': 'str', 'parse': '%m/%d/%Y %I:%M:%S %p'}
weight = {'header': 'WeightOz', 'format': 'float', 'parse': 'None'}
service0 = {'header': 'heading service', 'header_alt': (
    ('Carrier', 'ProviderName'), ('ServiceCode', 'ShippingServiceCode')), 'format': 'str'}
address = {'header0': 'PostalCode', 'header1': 'CountryCode', 'format': 'str', 'ctry_type': 'code'}
current_price = {'header': 'CarrierFee', 'format': 'float'}
insured_parcel = {'header': 'InsuranceFee', 'format': 'float'}
dimensions = {'header0': 'Length', 'header1': 'Width', 'header2': 'Height', 'format': 'float', 'unit': 'in'}

# x = ManifestFormat(sw='shipstation', address='xyz')
# Service = use column heading service if not found Join Carrier or ProviderName with ServiceCode or ShippingServiceCode
# ManifestFormat.update_sw_format('shipstation', order_no=order_no, date=d, weight=weight, service=service0,
#                                 current_price=current_price, insured_parcel=insured_parcel, address=address, dimensions=dimensions)
# with open(r'manifests\mnl.csv', 'r', encoding='utf-8') as f:
#     reader = csv.reader(f)
#     service_override = {}
#     for row in reader:
#         print(row)
#         sv_name_ovr = re.sub(r'[^\w]', '', row[0]).lower()
#         dom_intl_ovr = 'domestic' if row[2] in ('United States', 'US') else 'international'
#         if sv_name_ovr not in service_override:
#             service_override[sv_name_ovr] = {}
#         if row[1] not in service_override[sv_name_ovr]:
#             service_override[sv_name_ovr][row[1]] = {}
#         if dom_intl_ovr not in service_override[sv_name_ovr][row[1]]:
#             service_override[sv_name_ovr][row[1]][dom_intl_ovr] = {}
#         service_override[sv_name_ovr][row[1]][dom_intl_ovr] = int(row[3])
# ManifestFormat('shopify', order_no={'header': ['Shipment ID', 'Order ID', 'Carrier Transaction'], 'format': 'str'}, date={'header': ['Ship Date', 'Order Date'], 'format': 'str'}, weight={'header': [
#                'Weight Oz'], 'format': 'float'}, service={'header': ['Shipping Service'], 'format': 'float'}, current_price={'header': ['Carrier Fee'], 'format': 'float'}, address={'header': ['Ship Postal Code'], 'format': 'str'}, address1={'header': ['Ship Country'], 'format': 'str'})
headers = ('order_no', 'order_no', 'order_no', 'order_no', 'order_no', 'order_no', 'order_no', )
# for k, v in ManifestFormat.format.items():
#     print(k, v['shopify']['header'])
# print(ManifestFormat.format['address1']['teapplix'])
# sv_to_code_import(r'C:\Users\Roy Solmonovich\Downloads\sugg_services updated (1) (1).csv')
