# from sqlalchemy import create_engine, select, MetaData, Table
from db import db
from datetime import datetime, date
from dateutil import relativedelta
from sqlalchemy.sql import func
# import mf_lib as mflib
import pandas as pd
from numpy import random, int64, inf
# from flask-sqlalchemy import
from app_lib import CarrierCharge, country_to_code, service as lib_service, dhl_zip_zone_2020, ca_zip_zone, service_names, weight
import re
import os
from sqlalchemy.exc import ProgrammingError
from pymongo import MongoClient
from bson.objectid import ObjectId
mongodb_url = os.environ.get('MONGO_URL')
if not mongodb_url:
    from c import mongodb_url
client = MongoClient(mongodb_url)
mongo_db = client.manifests
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
    carrier_fields = {'DHL':
                      {'cost': 'dhl_cost_2021',
                       'locations': {
                           'US':
                           ('dhl_tier_1_2021', 'dhl_tier_2_2021', 'dhl_tier_3_2021', 'dhl_tier_4_2021',
                            'dhl_tier_5_2021'),
                           'Intl':
                           ('dhl_tier_2_2021', 'dhl_tier_3_2021', 'dhl_tier_5_2021')
                       }
                       }
                      }
    pickup_expense_const = 15

    def __init__(self, id, orderno, shipdate, weight, service, zip, country, insured, dim1, dim2, dim3, price, zone, weight_threshold, sugg_service, dhl_tier_1_2021, dhl_tier_2_2021, dhl_tier_3_2021, dhl_tier_4_2021, dhl_tier_5_2021, dhl_cost_2021, usps_2021, dhl_cost_shipdate, usps_shipdate):
        self.id = id
        self.orderno = orderno
        shipdate = datetime.strptime(shipdate, '%Y-%m-%d').date()
        self.shipdate = shipdate if shipdate.weekday() < 5 else shipdate+relativedelta.relativedelta(days=7-shipdate.weekday())
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
        return cls.query.filter(cls.id == _id).order_by(cls.shipdate).paginate(page, per_page, False)

    @ classmethod
    def find_all_shipments_query(cls, _id):
        # cls.query.with_entities(*report_fields).filter(cls.id == {_id}, {(', ').join(query)}).order_by(cls.shipdate)
        return f'cls.query.with_entities(*report_fields).filter(cls.id == {_id}).order_by(cls.shipdate)'

    @ classmethod
    def find_distinct_services(cls, _id):
        return cls.query.distinct().with_entities(cls.service, cls.weight_threshold, cls.country, cls.sugg_service).filter(cls.id == _id).all()

    @ classmethod
    def find_distinct_zones(cls, _id):
        all_zones = cls.query.distinct().with_entities(cls.zone).filter(cls.id == _id).order_by(cls.zone).all()
        domestic_zones, international_zones = [], []
        for zone in all_zones:
            if zone[0][:5] == 'Zone ':
                domestic_zones.append(zone[0])
            else:
                international_zones.append(zone[0])
        return domestic_zones, international_zones

    @ classmethod
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
                        f"cls.weight.between({min_weight if min_weight is not None else  0}, {max_weight if max_weight is not None else 999999})")
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
        report_query_string = f"cls.query.with_entities(*report_fields).filter(cls.id == {_id}, {(', ').join(query)}).order_by(cls.shipdate)"
        print(query_string)
        return query_string, report_query_string

    @ classmethod
    def find_filtered_shipments(cls, filter_query, page=1, per_page=20):
        paginated_result = eval(filter_query).paginate(page, per_page, False)
        return paginated_result

    @ classmethod
    def shipment_report(cls, filter_query=None, service_replacements=None, df=None, include_loss=True):
        if filter_query:
            query_eval = eval(filter_query)
            print(query_eval.statement)
            pd.set_option('display.max_columns', None)
            pd.set_option('display.max_rows', None)
            df = pd.read_sql(query_eval.statement, query_eval.session.bind)
            if df.empty:
                return
            if service_replacements:
                df = df.apply(lambda row: ManifestDataModel.correct_service_rates(
                    row, service_replacements.get((row.service, row.country, row.weight_threshold))), axis=1)
        elif df is None:
            raise TypeError('Filter_query or df argument required to generate report.')
        price_sum = df['price'].sum()
        print(price_sum)
        df.drop(['weight', 'weight_threshold', 'dim1', 'dim2', 'dim3', 'zone', 'service'], inplace=True, axis=1)
        price_columns = [_ for _ in df.columns if _ not in ('shipdate', 'country', 'insured')]
        # print(price_columns)
        # print(df[['price']].head(2000))
        df_date_pcs = df[['shipdate', 'country']].groupby(by='shipdate', sort=False, as_index=False).count()
        df_date_pcs['shipdate'] = df_date_pcs['shipdate'].astype(str)
        date_pcs_lst = df_date_pcs.values.tolist()
        df_by_date = df[['shipdate', 'price'] if price_sum else ['shipdate']].groupby(by='shipdate', sort=False).sum()
        pickup_days_count = len(df_by_date.index)
        packages_count = len(df.index)
        daily_packages = round(packages_count/pickup_days_count, 2)
        df_by_dom_intl = df.groupby(by='country', sort=False, as_index=True).sum()
        if include_loss:
            if 'US' in df_by_dom_intl.index:
                current_price_total_dom = round(df_by_dom_intl['price'].loc['US'], 2) if price_sum else None
            if 'Intl' in df_by_dom_intl.index:
                current_price_total_intl = round(df_by_dom_intl['price'].loc['Intl'], 2) if price_sum else None
            # packages_count_domestic = len(df['country'][df['country'] == 'US'].index)
            # packages_count_intl = packages_count-packages_count_domestic
            pickup_expenses = pickup_days_count*cls.pickup_expense_const
        highest_cost_date = df_by_date['price'].idxmax() if price_sum else None
        lowest_cost_date = df_by_date[df_by_date['price'] > 0]['price'].idxmin() if price_sum else None
        (highest_cost, lowest_cost) = (round(df_by_date['price'].loc[highest_cost_date], 2), round(
            df_by_date['price'].loc[lowest_cost_date], 2)) if price_sum else (None, None)
        # weekend dates are converted to next Monday at insert - shipdate is given to be a weekday
        start_date, end_date = df.shipdate.min(), df.shipdate.max()
        if isinstance(start_date, str):
            start_date, end_date = datetime.strptime(
                start_date, '%Y-%m-%d').date(), datetime.strptime(end_date, '%Y-%m-%d').date()
        diff = relativedelta.relativedelta(end_date, start_date)
        duration_dict = {'year': diff.years, 'month': diff.months, 'day': diff.days+1}
        duration = []
        for time_field, val in duration_dict.items():
            if val:
                duration.append(f'{val} {time_field}{"s" if val>1 else ""}')
        duration_str = (', ').join(duration)
        carrier_stats = []
        report = {'Duration': duration_str, 'Highest Cost': [highest_cost, str(highest_cost_date) if highest_cost_date else None],
                  'Lowest Cost': [lowest_cost, str(lowest_cost_date) if lowest_cost_date else None], 'Pickups': pickup_days_count,
                  'Daily Packages': daily_packages, 'date_pcs': date_pcs_lst}
        top_carriers = {'US': {}, 'Intl': {}}
        for carrier in cls.carrier_fields:
            cost_field = cls.carrier_fields[carrier]['cost']
            for location in cls.carrier_fields[carrier]['locations']:
                if location not in df_by_dom_intl.index:
                    continue
                first_tier = True
                cost_total = round(df[cost_field][df['country'] == location].sum(), 2)
                for tier_field in cls.carrier_fields[carrier]['locations'][location]:
                    if include_loss:
                        print('include loss')
                        # if 'weight_threshold' not in df_by_dom_intl.columns:
                        #     continue
                        current_price_total = current_price_total_dom if location == 'US' else current_price_total_intl
                        tier_total = round(df_by_dom_intl[tier_field].loc[location], 2)
                        tier_total = tier_total if tier_total else None
                    else:
                        print('exclude loss')
                        cost_total = round(df[cost_field][(df['country'] == location)
                                                          & (df[tier_field] < df['price'])].sum(), 2) if price_sum else None
                        if not cost_total:
                            continue
                        tier_total = round(df[tier_field][(df['country'] == location)
                                                          & (df[tier_field] < df['price'])].sum(), 2)
                        tier_total = tier_total if tier_total else None
                        current_price_total = round(df['price'][(df['country'] == location)
                                                                & (df[tier_field] < df['price'])].sum(), 2) if price_sum else None
                        pickup_days_count = len(df['shipdate'][df[tier_field] < df['price']
                                                               ].unique()) if price_sum else None
                        pickup_expenses = pickup_days_count*cls.pickup_expense_const
                    savings_total_amount = round(current_price_total - tier_total,
                                                 2) if price_sum and tier_total else None
                    savings_total_percentage = round(100*savings_total_amount /
                                                     current_price_total, 2) if tier_total and price_sum and current_price_total else None
                    profit_total_amount = round(tier_total-cost_total-pickup_expenses,
                                                2) if tier_total is not None and cost_total is not None else None
                    profit_total_percentage = round(100*profit_total_amount/tier_total, 2) if tier_total else None
                    if not carrier_stats or carrier_stats[-1]['Carrier Name'] != carrier:
                        carrier_stats.append({'Carrier Name': carrier})
                    if f'{"Domestic" if location == "US" else "International"} Tier Stats' not in carrier_stats[-1]:
                        carrier_stats[-1][f'{"Domestic" if location == "US" else "International"} Tier Stats'] = []
                    carrier_stats[-1][f'{"Domestic" if location == "US" else "International"} Tier Stats'].append(
                        {'Tier Name': tier_field, 'Current Cost': f"${current_price_total}" if current_price_total else current_price_total,
                         'Tier Cost': f"${tier_total}" if tier_total else tier_total,
                         'Savings ($)': f"${savings_total_amount}" if savings_total_amount else savings_total_amount,
                         'Savings (%)': f"{savings_total_percentage}%" if savings_total_percentage else savings_total_percentage,
                         'Our Cost': f"${cost_total}" if cost_total else cost_total,
                         'Profit ($)': f"${profit_total_amount}" if profit_total_amount else profit_total_amount,
                         'Profit (%)': f"{profit_total_percentage}%" if profit_total_percentage else profit_total_percentage,
                         'Pickups': pickup_days_count, 'Daily Packages': daily_packages}
                    )
                    if not first_tier:
                        continue
                    if savings_total_amount and savings_total_amount > top_carriers[location].get('Max Savings', -inf):
                        top_carriers[location]['Max Savings'] = carrier
                    if profit_total_amount and profit_total_amount > top_carriers[location].get('Max Profit', -inf):
                        top_carriers[location]['Max Profit'] = carrier
                    first_tier = False
                    print(tier_total, savings_total_amount, savings_total_percentage,
                          cost_total, profit_total_amount, profit_total_percentage)
        df.dropna(subset=price_columns, how='all', inplace=True)
        prices_found = len(df.index)
        print(f'A price was found for {round(100*prices_found/packages_count, 2)}% of shipments.')
        print('Done')
        report.update({'Carrier Stats': sorted(carrier_stats, key=lambda x: x.get(
            f'{"Domestic" if "US" in df_by_dom_intl.index else "International"} Tier Stats', [{'Tier Cost': inf}])[0]['Tier Cost']),
            'Top Domestic Carriers': top_carriers.get('US'),
            'Top International Carriers': top_carriers.get('Intl')})
        return report

    @ classmethod
    def find_raw_shipments(cls, _id, raw_input):
        try:
            return cls.query.from_statement(db.text(f"SELECT * FROM manifest_data_test WHERE id = {_id} AND {raw_input}")).all()
        except ProgrammingError:
            return [None]

    def save_to_db(self):
        db.session.add(self)

    def commit_to_db():
        db.session.commit()

    def delete_from_db(self):
        db.session.delete(self)
        db.session.commit()

    def row_to_rate(row):
        shipdate = str(row.shipdate.date())
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
        billing_weight = weight(row.weight, zone, row.dim1, row.dim2, row.dim3)[0]
        output.append(CarrierCharge.charge_weight(
            'DHL', 'domestic' if domestic else 'international', shipdate, sc[0], zone, billing_weight))
        if domestic and int(zone[-2:]) in (11, 12, 13) and row['weight'] < 16:
            return output+[None]*9
        for tier in range(1, 6):
            charge = CarrierCharge.charge_rate(
                tier, 'domestic' if domestic else 'international', str(date(2021, 2, 1)), sc[0], zone, billing_weight)
            charge = None if charge and charge < 0 else charge
            output.append(charge)
        dhl_c_2021 = CarrierCharge.charge_rate("DHL", "domestic" if domestic else "international",
                                               str(date(2021, 2, 1)), sc[0], zone, billing_weight)
        usps_c_2021 = CarrierCharge.charge_rate("USPS", "domestic" if domestic else "international",
                                                str(date(2021, 2, 1)), sc[0], zone, billing_weight)
        dhl_c = CarrierCharge.charge_rate("DHL", "domestic" if domestic else "international",
                                          shipdate, sc[0], zone, billing_weight)
        usps_c = CarrierCharge.charge_rate("USPS", "domestic" if domestic else "international",
                                           shipdate, sc[0], zone, billing_weight)
        # rates.extend([lib.CarrierCharge.charge_rate('DHL', 'domestic' if domestic else 'international', date(2021, 2, 1), sc, zone, w), lib.CarrierCharge.charge_rate('USPS', 'domestic' if domestic else 'international', date(2021, 2, 1), sc, zone, w), our_c, dhl_c, usps_c, ship_date])

        output.extend([dhl_c_2021, usps_c_2021, dhl_c, usps_c])
        return output

    def correct_service_rates(self, service_override):
        if service_override is None:
            return self
        service = int(service_names.get(service_override, 0))
        self.sugg_service = service_override
        zone = self.zone.replace('Zone ', 'USPS')
        location = 'domestic' if self.country == 'US' else 'international'
        print(location, zone)
        billing_weight = weight(self.weight, zone, self.dim1, self.dim2, self.dim3)[0]
        self.dhl_tier_1_2021 = CarrierCharge.charge_rate(1, location, str(
            date(2021, 3, 1)), service, zone, billing_weight)
        self.dhl_tier_2_2021 = CarrierCharge.charge_rate(2, location, str(
            date(2021, 3, 1)), service, zone, billing_weight)
        self.dhl_tier_3_2021 = CarrierCharge.charge_rate(3, location, str(
            date(2021, 3, 1)), service, zone, billing_weight)
        self.dhl_tier_4_2021 = CarrierCharge.charge_rate(4, location, str(
            date(2021, 3, 1)), service, zone, billing_weight)
        self.dhl_tier_5_2021 = CarrierCharge.charge_rate(5, location, str(
            date(2021, 3, 1)), service, zone, billing_weight)
        self.dhl_cost_2021 = CarrierCharge.charge_rate('DHL', location, str(
            date(2021, 3, 1)), service, zone, billing_weight)
        self.usps_2021 = CarrierCharge.charge_rate('USPS', location, str(
            date(2021, 3, 1)), service, zone, billing_weight)
        self.dhl_cost_shipdate = CarrierCharge.charge_rate('DHL', location, str(
            self.shipdate), service, zone, billing_weight)
        self.usps_shipdate = CarrierCharge.charge_rate('USPS', location, str(
            self.shipdate), service, zone, billing_weight)
        return self

    def weight_threshold_display(row):
        return f"{'Under' if row['weight_threshold'] == '<' else 'Over or equal to'} {'1 lb' if row['country'] == 'US' else '4.4 lbs'}"


class ManifestFormatModel:
    # if os.path.exists(r'manifests/format.json'):
    #     with open(r'manifests/format.json', 'r') as f:
    #         format = json.load(f)
    # else:
    #     format = {}
    Collection = mongo_db.format
    format = Collection.find_one({"_id": ObjectId("605b48c443fb2bf66ebb32c0")})

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
        if ManifestFormatModel.format.get('order_no') is None:
            ManifestFormatModel.format['order_no'] = {}
        if ManifestFormatModel.format['order_no'].get(sw) is None and order_no:
            ManifestFormatModel.format['order_no'][sw] = order_no

        if ManifestFormatModel.format.get('date') is None:
            ManifestFormatModel.format['date'] = {}
        if ManifestFormatModel.format['date'].get(sw) is None and date:
            ManifestFormatModel.format['date'][sw] = date

        if ManifestFormatModel.format.get('weight') is None:
            ManifestFormatModel.format['weight'] = {}
        if ManifestFormatModel.format['weight'].get(sw) is None and weight:
            ManifestFormatModel.format['weight'][sw] = weight

        if ManifestFormatModel.format.get('service') is None:
            ManifestFormatModel.format['service'] = {}
        if ManifestFormatModel.format['service'].get(sw) is None and service:
            ManifestFormatModel.format['service'][sw] = service

        if ManifestFormatModel.format.get('current_price') is None:
            ManifestFormatModel.format['current_price'] = {}
        if ManifestFormatModel.format['current_price'].get(sw) is None and current_price:
            ManifestFormatModel.format['current_price'][sw] = current_price

        if ManifestFormatModel.format.get('insured_parcel') is None:
            ManifestFormatModel.format['insured_parcel'] = {}
        if ManifestFormatModel.format['insured_parcel'].get(sw) is None and insured_parcel:
            ManifestFormatModel.format['insured_parcel'][sw] = insured_parcel

        if ManifestFormatModel.format.get('address') is None:
            ManifestFormatModel.format['address'] = {}
        if ManifestFormatModel.format['address'].get(sw) is None and address:
            ManifestFormatModel.format['address'][sw] = address

        if ManifestFormatModel.format.get('address1') is None:
            ManifestFormatModel.format['address1'] = {}
        if ManifestFormatModel.format['address1'].get(sw) is None and address1:
            ManifestFormatModel.format['address1'][sw] = address1

        if ManifestFormatModel.format.get('dim1') is None:
            ManifestFormatModel.format['dim1'] = {}
        if ManifestFormatModel.format['dim1'].get(sw) is None and dim1:
            ManifestFormatModel.format['dim1'][sw] = dim1

        if ManifestFormatModel.format.get('dim2') is None:
            ManifestFormatModel.format['dim2'] = {}
        if ManifestFormatModel.format['dim2'].get(sw) is None and dim2:
            ManifestFormatModel.format['dim2'][sw] = dim2

        if ManifestFormatModel.format.get('dim3') is None:
            ManifestFormatModel.format['dim3'] = {}
        if ManifestFormatModel.format['dim3'].get(sw) is None and dim3:
            ManifestFormatModel.format['dim3'][sw] = dim3

        # if CarrierCharge.map[carrier][location][date][service_code][ship_zone].get(weight) is None:
        # with open(r'Manifests\format.json', 'w') as f:
        #     json.dump(ManifestFormatModel.format, f, indent=4)

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
    #
    # def update_sw_format(sw, order_no=None, date=None, weight=None, service=None, current_price=None, insured_parcel=None, address=None, address1=None, dim1=None, dim2=None, dim3=None):
    #     if order_no:
    #         ManifestFormatModel.format['order_no'][sw] = order_no
    #     if date:
    #         ManifestFormatModel.format['date'][sw] = date
    #     if weight:
    #         ManifestFormatModel.format['weight'][sw] = weight
    #     if service:
    #         ManifestFormatModel.format['service'][sw] = service
    #     if current_price:
    #         ManifestFormatModel.format['current_price'][sw] = current_price
    #     if insured_parcel:
    #         ManifestFormatModel.format['insured_parcel'][sw] = insured_parcel
    #     if address:
    #         ManifestFormatModel.format['address'][sw] = address
    #     if address1:
    #         ManifestFormatModel.format['address1'][sw] = address
    #     if dim1:
    #         ManifestFormatModel.format['dim1'][sw] = dim1
    #     if dim2:
    #         ManifestFormatModel.format['dim2'][sw] = dim1
    #     if dim3:
    #         ManifestFormatModel.format['dim3'][sw] = dim1
    #     with open(r'Manifests\format.json', 'w') as f:
    #         json.dump(ManifestFormatModel.format, f, indent=4)

    @classmethod
    def find_platformat(cls, platform):
        platformat = {}
        for field in cls.format:
            if field == '_id':
                continue
            if platform in cls.format[field]:
                platformat[field] = {'header': cls.format[field][platform]['header']}
                if 'header_alt' in cls.format[field][platform]:
                    platformat[field]['header_alt'] = cls.format[field][platform]['header_alt']
        return platformat

    @classmethod
    def add_format_fields(cls, platform, field, header, value, index):
        if header == 'header_alt':
            cls.Collection.update_one({"_id": ObjectId("605b48c443fb2bf66ebb32c0")}, {
                                      '$push': {f'{field}.{platform}.{header}.{index}': value}})
            cls.format[field][platform][header][index].append(value)
        else:
            cls.Collection.update_one({"_id": ObjectId("605b48c443fb2bf66ebb32c0")}, {
                                      '$push': {f'{field}.{platform}.{header}': value}})
            cls.format[field][platform][header].append(value)


class ManifestRaw:
    ManifestCollection = mongo_db.manifest_names

    def __init__(self, name, init_time):
        self.name = name
        self.init_time = init_time

    @classmethod
    def find_manifest_by_id(cls, _id):
        manifest = cls.ManifestCollection.find_one({'_id': ObjectId(_id)}, {'_id': 0})
        return manifest

    @classmethod
    def find_shipments_by_name(cls, name, columns, dtype=None, limit=0):
        headers = {col: 1 for col in columns}
        headers['_id'] = 0
        df = pd.DataFrame(
            list(mongo_db[name].find({}, headers, limit=limit)), columns=columns, dtype=dtype)
        print(name, columns, limit, headers, df.head(5), df.columns)
        return pd.DataFrame(list(mongo_db[name].find({}, headers, limit=limit)), columns=columns, dtype=dtype)

    @classmethod
    def save_to_db(cls, df, **kwargs):
        RawCollection = mongo_db[kwargs['name']]
        RawCollection.insert_many(df.to_dict('records'))
        kwargs['init_time'] = datetime.now()
        _id = cls.ManifestCollection.insert(kwargs)
        return str(_id)

    @classmethod
    def delete_from_db(cls, _id):
        name = cls.ManifestCollection.find_one_and_delete({'_id': _id}, {'name': 1, '_id': 0})
        if name:
            mongo_db[name].drop()
        # continue manual manifest - complete saving to db, db to df, deleting afterwards


class ManifestModel(db.Model):
    __tablename__ = 'manifest'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(45))
    init_time = db.Column(db.DateTime())
    manifest_data = db.relationship('ManifestDataModel', cascade='all,delete', lazy='dynamic')
    manifest_missing = db.relationship('ManifestMissingModel', cascade='all,delete', lazy='dynamic')
    ai1s_headers = {'orderno', 'shipdate', 'weight', 'service provider and name', 'service provider', 'service name', 'zip', 'country', 'price',
                    'insured', 'dim1', 'dim2', 'dim3', 'address'}
    ai1s_headers_ordered = ['orderno', 'shipdate', 'weight', 'service', 'zip', 'country', 'insured', 'dim1', 'dim2', 'dim3', 'price', 'zone',
                            'sugg_service', 'dhl_tier_1_2021', 'dhl_tier_2_2021', 'dhl_tier_3_2021', 'dhl_tier_4_2021', 'dhl_tier_5_2021', 'dhl_cost_2021', 'usps_2021', 'dhl_cost_shipdate', 'usps_shipdate']
    default_types = {'price': 'float', 'dim1': 'float', 'dim2': 'float', 'dim3': 'float'}
    upload_directory = 'api_uploads'
    type_conv = {'str': str, 'float': float, 'int': pd.Int64Dtype(), 'bool': bool}
    ServiceCollection = mongo_db.service_to_code
    sv_to_code = ServiceCollection.find_one()

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

    @classmethod
    def service_from_params(cls, sv_name, ctry_code, weight):
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
        if sv_name in cls.sv_to_code:
            if dom_intl in cls.sv_to_code[sv_name]:
                if weight_thres in cls.sv_to_code[sv_name][dom_intl]:
                    return [cls.sv_to_code[sv_name][dom_intl][weight_thres], lib_service[str(cls.sv_to_code[sv_name][dom_intl][weight_thres])][3], weight_thres]
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

    @classmethod
    def update_services(cls, request, **form):
        print(request, form)
        sv_name = re.sub(r'[^\w]', '', form['service']).lower()
        dom_intl = 'domestic' if form['country'] == 'US' else 'international'
        weight_threshold = '>=' if form['weight_threshold'][:5] == 'Over ' else '<'
        if sv_name not in cls.sv_to_code:
            cls.sv_to_code[sv_name] = {}
        if dom_intl not in cls.sv_to_code[sv_name]:
            cls.sv_to_code[sv_name][dom_intl] = {}
        if request == 'post':
            if weight_threshold in cls.sv_to_code[sv_name][dom_intl]:
                return 0
            cls.sv_to_code[sv_name][dom_intl][weight_threshold] = int(
                service_names[form['sugg_service']])
        elif request == 'put':
            cls.sv_to_code[sv_name][dom_intl][weight_threshold] = int(
                service_names[form['sugg_service']])
            # with open(r'dependencies\services\sv_to_code.json', 'w') as f:
            #     json.dump(sv_to_code, f, indent=4)
        else:
            return 0
        cls.ServiceCollection.update_one({'_id': ObjectId('605b86138bd7990a0c25a526')}, {
                                         '$set': {sv_name: cls.sv_to_code[sv_name]}})
        return 1


report_fields = set(c for c in ManifestDataModel.__table__.c if c.name not in {
    'id', 'orderno', 'zip', 'sugg_service'})
