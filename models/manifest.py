# from sqlalchemy import create_engine, select, MetaData, Table
from db import db
from datetime import datetime, date
from sqlalchemy.sql import func
# import mf_lib as mflib
import pandas as pd
from numpy import random, int64, nan
# from flask-sqlalchemy import
from app_lib import CarrierCharge, country_to_code, service as lib_service, dhl_zip_zone_2020, ca_zip_zone, service_names, sv_to_code
import re
import os
import json
from sqlalchemy.exc import ProgrammingError

# manifest_data.tier_1_2021


class ManifestMissingModel(db.Model):
    __tablename__ = 'manifest_missing_fields'
    manifest_id = db.Column(db.Integer, db.ForeignKey('manifest.id'), primary_key=True)
    field = db.Column(db.String(20), primary_key=True)
    manifest = db.relationship('ManifestModel')

    def __init__(self, manifest_id, field):
        self.manifest_id = manifest_id
        self.field = field

    def save_to_db(self):
        db.session.add(self)
        db.session.commit()

    @ classmethod
    def find_all_by_id(cls, _id):
        return cls.query.with_entities(cls.field).filter_by(manifest_id=_id).all()

    @ classmethod
    def json(cls, _id):
        return {missing_field[0] for missing_field in cls.find_all_by_id(_id)}


class ManifestDataModel(db.Model):
    __tablename__ = 'manifest_data_test'
    id = db.Column(db.Integer, db.ForeignKey('manifest.id'), primary_key=True)
    orderno = db.Column(db.String(45), primary_key=True)
    shipdate = db.Column(db.Date(), index=True)
    weight = db.Column(db.Float(precision=3))
    service = db.Column(db.String(45))
    zip = db.Column(db.String(45))
    country = db.Column(db.String(45))
    insured = db.Column(db.Boolean())
    dim1 = db.Column(db.Float(precision=3))
    dim2 = db.Column(db.Float(precision=3))
    dim3 = db.Column(db.Float(precision=3))
    price = db.Column(db.Float(precision=2))
    zone = db.Column(db.String(7))
    weight_threshold = db.Column(db.String(2))
    sugg_service = db.Column(db.String(45))
    dhl_tier_1_2021 = db.Column(db.Float(precision=2))
    dhl_tier_2_2021 = db.Column(db.Float(precision=2))
    dhl_tier_3_2021 = db.Column(db.Float(precision=2))
    dhl_tier_4_2021 = db.Column(db.Float(precision=2))
    dhl_tier_5_2021 = db.Column(db.Float(precision=2))
    dhl_cost_2021 = db.Column(db.Float(precision=2))
    usps_2021 = db.Column(db.Float(precision=2))
    dhl_cost_shipdate = db.Column(db.Float(precision=2))
    usps_shipdate = db.Column(db.Float(precision=2))
    manifest = db.relationship('ManifestModel')
    zone_areas = {
        'Non-contiguous US': [f'Zone {i}' for i in range(11, 14)], 'Contiguous US': [f'Zone {i:02}' for i in range(1, 9)]}
    carrier_fields = {}

    def __init__(self, id, orderno, shipdate, weight, service, zip, country, insured, dim1, dim2, dim3, price, zone, weight_threshold, sugg_service, dhl_tier_1_2021, dhl_tier_2_2021, dhl_tier_3_2021, dhl_tier_4_2021, dhl_tier_5_2021, dhl_cost_2021, usps_2021, dhl_cost_shipdate, usps_shipdate):
        self.id = id
        self.orderno = orderno
        self.shipdate = shipdate
        self.weight = weight
        self.service = service
        self.zip = zip
        self.country = country
        self.insured = insured
        self.dim1 = dim1
        self.dim2 = dim2
        self.dim3 = dim3
        self.price = price
        self.zone = zone
        self.weight_threshold = weight_threshold
        self.sugg_service = sugg_service
        self.dhl_tier_1_2021 = dhl_tier_1_2021
        self.dhl_tier_2_2021 = dhl_tier_2_2021
        self.dhl_tier_3_2021 = dhl_tier_3_2021
        self.dhl_tier_4_2021 = dhl_tier_4_2021
        self.dhl_tier_5_2021 = dhl_tier_5_2021
        self.dhl_cost_2021 = dhl_cost_2021
        self.usps_2021 = usps_2021
        self.dhl_cost_shipdate = dhl_cost_shipdate
        self.usps_shipdate = usps_shipdate

    # @ classmethod
    # def find_by_name(cls, name):
    #     return cls.query.filter_by(name=name).first()

    @ classmethod
    def find_by_id(cls, _id):
        return cls.query.filter_by(id=_id).first()

    @ classmethod
    def find_all_shipments(cls, _id, page=1, per_page=20):
        # userList = users.query\
        #     .join(friendships, users.id==friendships.user_id)\
        #     .add_columns(users.userId, users.name, users.email, friends.userId, friendId)\
        #     .filter(users.id == friendships.friend_id)\
        #     .filter(friendships.user_id == userID)\
        #     .paginate(page, 1, False)
        # return cls.query.join(ManifestModel, ManifestModel.id == cls.id).filter(cls.id == _id).all()
        return cls.query.filter(cls.id == _id).order_by(cls.shipdate).paginate(page, per_page, False)

    @ classmethod
    def find_distinct_services(cls, _id):
        return cls.query.distinct().with_entities(cls.service, cls.weight_threshold, cls.country, cls.sugg_service).filter(cls.id == _id).all()

    @classmethod
    def find_distinct_zones(cls, _id):
        all_zones = cls.query.distinct().with_entities(cls.zone).filter(cls.id == _id).order_by(cls.zone).all()
        domestic_zones, international_zones = [], []
        for zone in all_zones:
            if zone[0][:5] == 'Zone ':
                domestic_zones.append(zone[0])
            else:
                international_zones.append(zone[0])
        return domestic_zones, international_zones

    @classmethod
    def find_date_range(cls, _id):
        start_date = cls.query.with_entities(func.min(cls.shipdate)).filter(cls.id == _id).first()
        end_date = cls.query.with_entities(func.max(cls.shipdate)).filter(cls.id == _id).first()
        return str(start_date[0]), str(end_date[0])

    @ classmethod
    def filtered_query_builder(cls, _id, shipment_filter):
        query = []
        if 'shipdates' in shipment_filter:
            include, start, end = shipment_filter['shipdates']
            if None not in (start, end):
                query.append(f"{'' if include else '~'}cls.shipdate.between('{start}', '{end}')")
            else:
                if start:
                    query.append(f"{'' if include else '~'}cls.shipdate >= '{start}'")
                elif end:
                    query.append(f"{'' if include else '~'}cls.shipdate <= '{end}'")
        if 'weight_zone' in shipment_filter:
            weight_zone_query = []
            for weight_zone in shipment_filter['weight_zone']:
                weight_zone_sub = []
                if 'weight' in weight_zone:
                    min_weight, max_weight = weight_zone['weight']
                    weight_zone_sub.append(
                        f"cls.weight.between({min_weight or 0}, {max_weight or 999999})")
                if 'zone' in weight_zone:
                    zones = weight_zone['zone']
                    zones_list = []
                    include_international = False
                    for zone in zones:
                        if zone in cls.zone_areas:
                            zones_list += cls.zone_areas[zone]
                        else:
                            if zone == 'International':
                                include_international = True
                            else:
                                zones_list.append(zone)
                    if not include_international:
                        zones_list = [f"cls.zone.in_({zones_list})"]
                    else:
                        zones_list = [
                            f"(cls.zone.in_({zones_list[:-1]}) | ~cls.country.__eq__('US'))"]
                    weight_zone_sub += zones_list
                    print(weight_zone_sub)
                weight_zone_query.append('('+(' & ').join(weight_zone_sub)+')')
            query.append((' | ').join(weight_zone_query))
        if 'services' in shipment_filter:
            service_query = []
            for service in shipment_filter['services']:
                service_query.append(
                    f"(cls.service.__eq__('{service['service name']}') & cls.weight_threshold.__eq__('{'>=' if service['weight threshold'][:5] == 'Over ' else '<'}') & cls.country.{'__eq__' if service['location'] == 'US' else '__ne__'}('US'))")
            query.append((' | ').join(service_query))
        query_string = f"cls.query.filter(cls.id == {_id}, {(', ').join(query)}).order_by(cls.shipdate)"
        print(query_string)
        return query_string

    @ classmethod
    def find_filtered_shipments(cls, filter_query, page=1, per_page=20):
        paginated_result = eval(filter_query).paginate(page, per_page, False)
        return paginated_result

    @ classmethod
    def filter_based_report(cls, filter_query):
        # 	"lowest_cost": [cost, date],
        # 	"highest_cost": [cost, date],
        # 	"duration": number of days,
        # 	“pickups”; # of pickups,
        # 	“daily_packages”: # of daily packages,
        # 	"carrier_stats": [
        # 			{
        # 			"carrier_name": name,
        # 				“domestic_tier_stats”:
        # {
        # 					“tier_name”: name,
        # "current_cost": cost,
        # 					"tier_cost": cost,
        # 					"savings_$": savings $,
        # 					"savings_%": savings %,
        # 					"our_cost": cost,
        # 					"profit_$": profit $,
        # 					"profit_%": profit %,
        # 					"pickups": # of pickups,
        # 					"daily_packages": # of daily packages
        # 					},
        #
        #
        # 			“international_tier_stats”:
        #  {
        # 				“tier_name”: name,
        # "current_cost": cost,
        # 				"tier_cost": cost,
        # 				"savings_$": savings $,
        # 				"savings_%": savings %,
        # 				"our_cost": cost,
        # 				"profit_$": profit $,
        # 				"profit_%": profit %,
        # 				"pickups": # of pickups,
        # 				"daily_packages": # of daily packages
        # 				}
        # 			]
        # 			},
        # 			{... more carriers ...}
        # 			]
        # 	}
        query_eval = eval(filter_query)
        df = pd.read_sql(query_eval.statement, query_eval.session.bind)
        pd.set_option('display.max_columns', None)
        lowest_cost, highest_cost = df.price.min(), df.price.max()
        lowest_cost = None if lowest_cost is nan else lowest_cost
        highest_cost = None if highest_cost is nan else highest_cost
        unique_dates = pd.to_datetime(df.shipdate.drop_duplicates(inplace=False), errors='coerce')
        weekdays = unique_dates.dt.dayofweek
        pickup_days_count = len(weekdays[weekdays < 5])
        # Sat, Sun = 5, 6
        packages_count = len(df.index)
        daily_package = round(packages_count/pickup_days_count, 2)
        start_date, end_date = df.shipdate.min(), df.shipdate.max()
        year, month, day = end_date.year-start_date.year, end_date.month-start_date.month, end_date.day-start_date.day
        duration = []
        for time_field in 'year', 'month', 'day':
            val = eval(time_field)
            if val:
                duration.append(f'{val} {time_field}{"s" if val>1 else ""}')
        print(df.head)
        duration_str = (', ').join(duration)
        print(duration_str, start_date, end_date, lowest_cost, highest_cost, pickup_days_count, daily_package)
        return duration_str, start_date, end_date, lowest_cost, highest_cost, pickup_days_count, daily_package

    @ classmethod
    def find_raw_shipments(cls, _id, raw_input):
        try:
            return cls.query.from_statement(db.text(f"SELECT * FROM manifest_data_test WHERE id = {_id} AND {raw_input}")).all()
        except ProgrammingError:
            return [None]

    @ classmethod
    def find_filtered_shipments_v2(cls, _id, shipment_filter):
        query = [f'SELECT * FROM manifest_data_test WHERE id = {_id}']
        if 'shipdates' in shipment_filter:
            include, start, end = shipment_filter['shipdates']
            if None not in (start, end):
                query.append(f"shipdate {'' if include else 'NOT '}BETWEEN '{start}' AND '{end}'")
            else:
                if start:
                    query.append(f"shipdate {'>=' if include else '<'} '{start}'")
                elif end:
                    query.append(f"shipdate {'<=' if include else '>'} '{end}'")
        if 'weight_zone' in shipment_filter:
            weight_zone_query = []
            include_weight_zones, *weight_zones = shipment_filter['weight_zone']
            for weight_zone in weight_zones:
                weight_zone_sub = []
                if 'weight' in weight_zone:
                    include, min_weight, max_weight = weight_zone['weight']
                    print(include, min_weight, max_weight)
                    if None not in (min_weight, max_weight):
                        weight_zone_sub.append(
                            f"weight {'' if include else 'NOT '}BETWEEN {min_weight} AND {max_weight}")
                    else:
                        if min_weight:
                            weight_zone_sub.append(f"weight {'>=' if include else '<'} {min_weight}")
                        elif max_weight:
                            weight_zone_sub.append(f"weight {'<=' if include else '>'} {max_weight}")
                if 'zone' in weight_zone:
                    include, *zones = weight_zone['zone']
                    zones_list = []
                    include_international = False
                    for zone in zones:
                        if zone in cls.zone_areas:
                            zones_list += cls.zone_areas[zone]
                        else:
                            if zone == 'International':
                                include_international = True
                            else:
                                zones_list.append(zone)
                    if not include_international:
                        zones_list = [f"zone {'' if include else 'NOT '}IN {tuple(zones_list)}"]
                    else:
                        zones_list = [
                            f"{'' if include else 'NOT '}(zone IN {tuple(zones_list)} OR country != 'US')"]
                    weight_zone_sub += zones_list
                weight_zone_query.append('('+(' AND ').join(weight_zone_sub)+')')
            query.append(
                f"{'' if include_weight_zones[1] else 'NOT ('}{(' OR ' if include_weight_zones[0] else ' AND ').join(weight_zone_query)}{'' if include_weight_zones[1] else ')'}")
        if 'services' in shipment_filter:
            service_query = []
            for service in shipment_filter['services']:
                service_query.append(
                    f"(service = '{service['service name']}' AND weight_threshold = '{'>=' if service['weight threshold'][:5] == 'Over ' else '<'}' and country {'=' if service['location'] == 'US' else '!='} 'US')")
            query.append('('+(' OR ').join(service_query)+')')
        print(' AND '.join(query) + ' ORDER BY shipdate')
        return cls.query.from_statement(db.text(' AND '.join(query) + ' ORDER BY shipdate')).all()

    def save_to_db(self):
        db.session.add(self)

    def commit_to_db():
        db.session.commit()

    def delete_from_db(self):
        db.session.delete(self)
        db.session.commit()

    def row_to_rate(row):
        shipdate = row.shipdate.date()
        if len(row.country) == 2 and row.country.isupper():
            ctry_code = country_to_code.get(row.country)
        else:
            ctry_code = country_to_code.get(re.sub(r'[^\w]', '', row.country).lower())
        domestic = True if ctry_code == 'US' else False
        sv_name = row['service'] if isinstance(row['service'], str) else ''
        sc = ManifestModel.service_from_params(sv_name, ctry_code, row['weight'])
        if not ctry_code:
            print(f'{row.country} not in country to code hash table')
            return [None*2]+[sc[1]]+[None]*11
        if domestic:
            zip = row.zip.replace('-', '')
            if zip == 'N/A':
                zone = f"{int(row.zone[5:]):02d}"
            else:
                if len(zip) <= 5:
                    zip3 = int(zip[: len(zip)-2])
                else:
                    zip3 = int(zip[: len(zip)-6])
                zone = dhl_zip_zone_2020[(zip3)] if zip != 'N/A' else row['zone']
            # zone = lib.dhl_zip_zone_2020[(zip3)] if zip != 'N/A' else row['zone']
            if zone == '09':
                zone = '08'
            zone = "USPS" + zone
        else:
            if ctry_code == 'CA' and sc[0] == 60:
                zone = ca_zip_zone[row.zip[:3]]
            else:
                zone = ctry_code
        zone_desc = 'Zone ' + zone[-2:] if domestic else zone
        if not sc[0]:
            return [ctry_code, zone_desc, sc[2], sc[1]]+[None]*10
        no_costs = []
        output = [ctry_code, zone_desc]+[sc[2], sc[1]]
        output.append(CarrierCharge.charge_weight(
            'DHL', 'domestic' if domestic else 'international', str(shipdate), sc[0], zone, row['weight']))
        if domestic and int(zone[-2:]) in (11, 12, 13) and row['weight'] < 16:
            return output+[None]*9
        for tier in range(1, 6):
            charge = CarrierCharge.charge_rate(
                tier, 'domestic' if domestic else 'international', str(date(2021, 2, 1)), sc[0], zone, row['weight'])
            charge = None if charge and charge < 0 else charge
            output.append(charge)
        dhl_c_2021 = CarrierCharge.charge_rate("DHL", "domestic" if domestic else "international",
                                               str(date(2021, 2, 1)), sc[0], zone, row['weight'])
        usps_c_2021 = CarrierCharge.charge_rate("USPS", "domestic" if domestic else "international",
                                                str(date(2021, 2, 1)), sc[0], zone, row['weight'])
        dhl_c = CarrierCharge.charge_rate("DHL", "domestic" if domestic else "international",
                                          str(shipdate), sc[0], zone, row['weight'])
        usps_c = CarrierCharge.charge_rate("USPS", "domestic" if domestic else "international",
                                           str(shipdate), sc[0], zone, row['weight'])
        # rates.extend([lib.CarrierCharge.charge_rate('DHL', 'domestic' if domestic else 'international', date(2021, 2, 1), sc, zone, w), lib.CarrierCharge.charge_rate('USPS', 'domestic' if domestic else 'international', date(2021, 2, 1), sc, zone, w), our_c, dhl_c, usps_c, ship_date])

        output.extend([dhl_c_2021, usps_c_2021, dhl_c, usps_c])
        return output

    def correct_service_rates(self, service_override):
        service = int(service_names[service_override])
        self.sugg_service = service_override
        self.dhl_tier_1_2021 = CarrierCharge.charge_rate(1, 'domestic' if self.country == 'US' else 'international', str(
            date(2021, 3, 1)), service, self.zone.replace('Zone ', 'USPS'), self.weight)
        self.dhl_tier_2_2021 = CarrierCharge.charge_rate(2, 'domestic' if self.country == 'US' else 'international', str(
            date(2021, 3, 1)), service, self.zone.replace('Zone ', 'USPS'), self.weight)
        self.dhl_tier_3_2021 = CarrierCharge.charge_rate(3, 'domestic' if self.country == 'US' else 'international', str(
            date(2021, 3, 1)), service, self.zone.replace('Zone ', 'USPS'), self.weight)
        self.dhl_tier_4_2021 = CarrierCharge.charge_rate(4, 'domestic' if self.country == 'US' else 'international', str(
            date(2021, 3, 1)), service, self.zone.replace('Zone ', 'USPS'), self.weight)
        self.dhl_tier_5_2021 = CarrierCharge.charge_rate(5, 'domestic' if self.country == 'US' else 'international', str(
            date(2021, 3, 1)), service, self.zone.replace('Zone ', 'USPS'), self.weight)
        self.dhl_cost_2021 = CarrierCharge.charge_rate('DHL', 'domestic' if self.country == 'US' else 'international', str(
            date(2021, 3, 1)), service, self.zone.replace('Zone ', 'USPS'), self.weight)
        self.usps_2021 = CarrierCharge.charge_rate('USPS', 'domestic' if self.country == 'US' else 'international', str(
            date(2021, 3, 1)), service, self.zone.replace('Zone ', 'USPS'), self.weight)
        self.dhl_cost_shipdate = CarrierCharge.charge_rate('DHL', 'domestic' if self.country == 'US' else 'international', str(
            self.shipdate), service, self.zone.replace('Zone ', 'USPS'), self.weight)
        self.usps_shipdate = CarrierCharge.charge_rate('USPS', 'domestic' if self.country == 'US' else 'international', str(
            self.shipdate), service, self.zone.replace('Zone ', 'USPS'), self.weight)
        return self

    def weight_threshold_display(row):
        return f"{'Under' if row['weight_threshold'] == '<' else 'Over or equal to'} {'1 lb' if row['country'] == 'US' else '4.4 lbs'}"


class ManifestFormat:
    if os.path.exists(r'manifests/format.json'):
        with open(r'manifests/format.json', 'r') as f:
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


class ManifestModel(db.Model):
    __tablename__ = 'manifest'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(45))
    init_time = db.Column(db.DateTime())
    manifest_data = db.relationship('ManifestDataModel', cascade='all,delete', lazy='dynamic')
    manifest_missing = db.relationship('ManifestMissingModel', cascade='all,delete', lazy='dynamic')
    format = ManifestFormat.format
    ai1s_headers = {'orderno', 'shipdate', 'weight', 'service provider and name', 'service provider', 'service name', 'zip', 'country', 'price',
                    'insured', 'dim1', 'dim2', 'dim3', 'address'}
    ai1s_headers_ordered = ['orderno', 'shipdate', 'weight', 'service', 'zip', 'country', 'insured', 'dim1', 'dim2', 'dim3', 'price', 'zone',
                            'sugg_service', 'dhl_tier_1_2021', 'dhl_tier_2_2021', 'dhl_tier_3_2021', 'dhl_tier_4_2021', 'dhl_tier_5_2021', 'dhl_cost_2021', 'usps_2021', 'dhl_cost_shipdate', 'usps_shipdate']
    upload_directory = 'api_uploads'
    type_conv = {'str': str, 'float': float, 'int': pd.Int64Dtype(), 'bool': bool}
    # with open(r'dependencies\services\dhl_service_hash.json', 'r') as f:
    # service = json.load(f)

    def __init__(self, name):
        self.name = name
        self.init_time = datetime.today()

    @ classmethod
    def find_by_name(cls, name):
        return cls.query.filter_by(name=name).first()

    @ classmethod
    def find_by_id(cls, _id):
        return cls.query.filter_by(id=_id).first()

    # @ classmethod
    # def manifest_shipments(cls, _id, filter=None):
    #
    #     # if not filter:
    #     #     filter = {'shipdate': ['2020-12-28', '2021-01-12', True]}
    #     # *dates, include_dates = filter['shipdate']
    #     # mf_data = self.manifest_data.all()
    #     # for mf_object in mf_data:
    #     #     print(mf_object.shipdate)
    #     # shipments = cls.query(ManifestModel).options(lazyload(ManifestModel.children)).all()
    #     # shipments = cls.query.join(cls.manifest_data, aliased=True)\
    #     #     .filter_by(id=_id).all()
    #     # for shipment in shipments:
    #     #     print(shipment.shipdate)
    #     query_results = db.query(cls, ManifestDataModel).\
    #         join(ManifestDataModel, cls.id == ManifestDataModel.id).\
    #         filter(
    #             cls.to_topic == _id
    #     ).all()
    #     for res in query_results:
    #         print(query_results)
    #
    # @ classmethod
    # def filtered_shipments_by_id(cls, shipment_filter, _id):
    #
    #     #         {'name': 'ship1212', 'filters': {'shipdates': [False, '2021-01-01', '2021-01-12'], 'weight_zone': [{'weight': [False, '1', '4'], 'zone': [True, ['Non-contiguous US', 'International']]}, {'weight': [True, '1', '2'], 'zone': [False, ['zone b', 'non contiguous b', 'non contiguous c', 'International']]}], 'services': [{'service name': 'USPS First Class Mail', 'location': 'US', 'weight threshold': '<', 'service': None}, {'service name': 'USPS Priority Mail', 'location': 'US', 'weight threshold': '>=', 'service': 'DHL SmartMail Parcel Plus Expedited'}, {'service name': 'USPS First Class Mail Intl', 'location': 'Intl', 'weight threshold': '<', 'service': None}]}}
    #     # False 2021-01-01 2021-01-12
    #     # {'shipdates': [False, '2021-01-01', '2021-01-12'], 'weight_zone': [{'weight': [False, '1', '4'], 'zone': [True, ['Non-contiguous US', 'International']]}, {'weight': [True, '1', '2'], 'zone': [False, ['zone b', 'non contiguous b', 'non contiguous c', 'International']]}], 'services': [{'service name': 'USPS First Class Mail', 'location': 'US', 'weight threshold': '<', 'service': None}, {'service name': 'USPS Priority Mail', 'location': 'US', 'weight threshold': '>=', 'service': 'DHL SmartMail Parcel Plus Expedited'}, {'service name': 'USPS First Class Mail Intl', 'location': 'Intl', 'weight threshold': '<', 'service': None}]}
    #     # {'shipdates': [False, '2021-01-01', '2021-01-12'], 'weight_zone': [{'weight': [False, '1', '4'], 'zone': [True, ['Non-contiguous US', 'International']]}, {'weight': [True, '1', '2'], 'zone': [False, ['zone b', 'non contiguous b', 'non contiguous c', 'International']]}], 'services': [{'service name': 'USPS First Class Mail', 'location': 'US', 'weight threshold': '<', 'service': None}, {'service name': 'USPS Priority Mail', 'location': 'US', 'weight threshold': '>=', 'service': 'DHL SmartMail Parcel Plus Expedited'}, {'service name': 'USPS First Class Mail Intl', 'location': 'Intl', 'weight threshold': '<', 'service': None}]}
    #     zone_areas = {'Non-contiguous US': list(range(11, 14)), 'Contiguous US': list(range(1, 9))}
    #     include_dates, start_date, end_date = shipment_filter['shipdates']
    #     weight_zones = shipment_filter['weight_zone']
    #     if weight_zones:
    #         weight_zones_query = ' & '
    #         for weight_zone in weight_zones:
    #             if 'weight' in weight_zone:
    #                 include_weight, min_weight, max_weight = weight_zone['weight']
    #             if 'zone' in weight_zone['zone']:
    #                 include_zones, *zones = weight_zone['zone']
    #                 # for zone in zones:
    #                 #
    #
    #     # for w_z in weight_zone:
    #
    #     print(include_dates, start_date, end_date)
    #
    #     print(shipment_filter)
    #     # query = ManifestDataModel.query.filter((ManifestDataModel.id == _id) & (
    #     # ManifestDataModel.shipdate >= start_date) & (ManifestDataModel.shipdate <= end_date)).all()
    #     query_to_eval_start = 'ManifestDataModel.query.filter((ManifestDataModel.id == _id)'
    #     query_dates = f' & {"" if include_dates else "~"}((ManifestDataModel.shipdate >= start_date) & (ManifestDataModel.shipdate <= end_date))'
    #     # query_to_eval_end = ').all()'
    #     # query = eval(query_to_eval_start+query_dates+query_to_eval_end)
    #     query = ManifestDataModel.query.filter((ManifestDataModel.id == _id) & ~((
    #         ManifestDataModel.shipdate >= start_date) & (ManifestDataModel.shipdate <= end_date))).all()
    #
    #     # for res in query:
    #     #     print(res, res.shipdate)
    #     # return {}
    #     #
    #     # for res in query:
    #     #     print(res, res.shipdate)
    # # @ classmethod
    # # def find_by_id(cls, _id):
    # #     return cls.query.filter_by(id=_id).first()

    @ classmethod
    def find_all(cls):
        return cls.query.order_by(cls.init_time.desc()).all()

    def save_to_db(self):
        db.session.add(self)
        db.session.commit()

    def delete_from_db(self):
        db.session.delete(self)
        db.session.commit()

    def vali_date(date_text):
        try:
            datetime.strptime(date_text, '%Y-%m-%d')
            return str(datetime.strptime(date_text, '%Y-%m-%d').date())
        except ValueError:
            return

    def allowed_file(filename):
        ALLOWED_EXTENSIONS = {'csv', 'xlsx'}
        if '.' not in filename:
            return False
        f_ext = filename.rsplit('.', 1)[1].lower()
        if f_ext not in ALLOWED_EXTENSIONS:
            return False
        return True

    def json(self):
        return {'name': self.name, 'items': [item.json() for item in self.items.all()]}

    def random_dates(start, end, n):
        start_u = start.value//10**9
        end_u = end.value//10**9
        return pd.DatetimeIndex((10**9*random.randint(start_u, end_u, n, dtype=int64)).view('M8[ns]'))

    def w_lbs_or_w_oz(string):
        string = string.rstrip(' ')
        weights = string.split(' ')
        val_total = 0
        for i in range(len(weights))[::2]:
            val, unit = float(weights[i]), weights[i+1]
            # print(val, unit)
        # (val, unit) = string.split(' ')
        # val = float(val)
            if unit in ('lb', 'lbs'):
                val *= 16
            val_total += val
        # print(val_total)
        return val_total

    def service_from_params(sv_name, ctry_code, weight):
        # OVERRIDE:

        # sv_to_code = service_override
        dom_intl = 'domestic' if ctry_code == 'US' else 'international'
        if dom_intl == 'domestic':
            weight_thres = '<' if weight < 16 else '>='
        else:
            weight_thres = '<' if weight < 70.4 else '>='
        if sv_name in service_names:
            return [int(service_names[sv_name]), sv_name, weight_thres]
        sv_name = re.sub(r'[^\w]', '', sv_name).lower()
        if sv_name in sv_to_code:
            if dom_intl in sv_to_code[sv_name]:
                if weight_thres in sv_to_code[sv_name][dom_intl]:
                    return [sv_to_code[sv_name][dom_intl][weight_thres], lib_service[str(sv_to_code[sv_name][dom_intl][weight_thres])][3], weight_thres]
                else:
                    print(f'Weight threshold not found for service name: {sv_name}.')
            else:
                print(f'{dom_intl} not found for service name: {sv_name}.')
        return [None, None, weight_thres]

    # def row_to_rate(row):
    #     shipdate = row.shipdate.date()
    #     if len(row.country) == 2 and row.country.isupper():
    #         ctry_code = country_to_code.get(row.country)
    #     else:
    #         ctry_code = country_to_code.get(re.sub(r'[^\w]', '', row.country).lower())
    #     domestic = True if ctry_code == 'US' else False
    #     sv_name = re.sub(r'[^\w]', '', (row['service'] if isinstance(row['service'], str) else '')).lower()
    #     sc = ManifestModel.service_from_params(sv_name, ctry_code, row['weight'])
    #     if not ctry_code:
    #         print(f'{row.country} not in country to code hash table')
    #         return [None*2]+[sc[1]]+[None]*11
    #     if domestic:
    #         zip = row.zip.replace('-', '')
    #         if zip == 'N/A':
    #             zone = f"{int(row.zone[5:]):02d}"
    #         else:
    #             if len(zip) <= 5:
    #                 zip3 = int(zip[: len(zip)-2])
    #             else:
    #                 zip3 = int(zip[: len(zip)-6])
    #             zone = dhl_zip_zone_2020[(zip3)] if zip != 'N/A' else row['zone']
    #         # zone = lib.dhl_zip_zone_2020[(zip3)] if zip != 'N/A' else row['zone']
    #         if zone == '09':
    #             zone = '08'
    #         zone = "USPS" + zone
    #     else:
    #         if ctry_code == 'CA' and sc[0] == 60:
    #             zone = ca_zip_zone[row.zip[:3]]
    #         else:
    #             zone = ctry_code
    #     zone_desc = 'Zone ' + zone[-2:] if domestic else zone
    #     if not sc[0]:
    #         return [ctry_code, zone_desc, sc[2], sc[1]]+[None]*10
    #     no_costs = []
    #     output = [ctry_code, zone_desc]+[sc[2], sc[1]]
    #     output.append(CarrierCharge.charge_weight(
    #         'DHL', 'domestic' if domestic else 'international', str(shipdate), sc[0], zone, row['weight']))
    #     if domestic and int(zone[-2:]) in (11, 12, 13) and row['weight'] < 16:
    #         return output+[None]*9
    #     for tier in range(1, 6):
    #         charge = CarrierCharge.charge_rate(
    #             tier, 'domestic' if domestic else 'international', str(date(2021, 2, 1)), sc[0], zone, row['weight'])
    #         charge = None if charge < 0 else charge
    #         output.append(charge)
    #     dhl_c_2021 = CarrierCharge.charge_rate("DHL", "domestic" if domestic else "international",
    #                                            str(date(2021, 2, 1)), sc[0], zone, row['weight'])
    #     usps_c_2021 = CarrierCharge.charge_rate("USPS", "domestic" if domestic else "international",
    #                                             str(date(2021, 2, 1)), sc[0], zone, row['weight'])
    #     dhl_c = CarrierCharge.charge_rate("DHL", "domestic" if domestic else "international",
    #                                       str(shipdate), sc[0], zone, row['weight'])
    #     usps_c = CarrierCharge.charge_rate("USPS", "domestic" if domestic else "international",
    #                                        str(shipdate), sc[0], zone, row['weight'])
    #     # rates.extend([lib.CarrierCharge.charge_rate('DHL', 'domestic' if domestic else 'international', date(2021, 2, 1), sc, zone, w), lib.CarrierCharge.charge_rate('USPS', 'domestic' if domestic else 'international', date(2021, 2, 1), sc, zone, w), our_c, dhl_c, usps_c, ship_date])
    #
    #     output.extend([dhl_c_2021, usps_c_2021, dhl_c, usps_c])
    #     return output
    def add_to_zip_ctry(add):
        add_split = add.split(', ')
        n = len(add_split)-2
        for i in range(n):
            if re.search(r'\d', add_split[n-i]) or not add_split[n-i]:
                zip = add_split[n-i]
                country = ' '.join(add_split[n-i+1:])
                return (zip, country)

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
                    sv_to_code[sv_name][dom_intl][row['weight threshold']] = int(
                        service_names[row['sugg. service']])
                    with open(r'dependencies\services\sv_to_code.json', 'w') as f:
                        json.dump(sv_to_code, f, indent=4)
