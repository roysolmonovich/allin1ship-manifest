from flask_restful import Resource
from flask import request
from werkzeug.utils import secure_filename
import os
import pandas as pd
from datetime import datetime
from math import ceil
from app_lib import service as lib_service
from numpy import nan, where
from flask_jwt_extended import jwt_required

from models.manifest import ManifestModel, ManifestDataModel, ManifestMissingModel, ManifestFormatModel, ManifestRaw
from schemas.manifest import ManifestSchema
from resource_helpers.manifest import create_df

import pdb

# Redis is currently not used
if os.environ.get('REDIS_URL') is not None:
    redis_cred = os.environ['REDIS_URL']
else:
    # from c import redis_cred
    redis_cred = 'redis://localhost:6379'
# from xlrd import open_workbook
manifest_schema = ManifestSchema()
# celery = Celery('apptst_h - Copy', backend='redis',
#                 broker=redis_cred)

dom_service_names = []
intl_service_names = []
for v in lib_service.values():
    if v[-1] == 'domestic':
        dom_service_names.append(v[3])
    elif v[-1] == 'international':
        intl_service_names.append(v[3])
dom_service_names.append('Service Currently Not Provided')
intl_service_names.append('Service Currently Not Provided')
dom_intl = {'domestic services': dom_service_names, 'international services': intl_service_names}


class Manifest(Resource):
    @jwt_required()
    def get(self):
        args = request.args
        if errors := manifest_schema.validate(request.args):
            return errors, 400
        name = args['name']
        existing = ManifestModel.find_by_name(name=name.replace('%20', ' '))
        if not existing:
            return {'message': f'Name {name} not found in system.'}, 400
        missing_columns = ManifestMissingModel.json(_id=existing.id)
        print(list(missing_columns), "missing columns")
        shipments = []
        paginated_result = ManifestDataModel.find_all_shipments(existing.id)
        query = ManifestDataModel.find_all_shipments_query(existing.id)
        for shipment_item in paginated_result.items:
            shipment = shipment_item.__dict__
            del shipment['_sa_instance_state']
            shipment['shipdate'] = str(shipment['shipdate'])
            for missing_column in missing_columns:
                shipment[missing_column+'_gen'] = shipment.pop(missing_column)
            shipments.append(shipment)
        date_range = ManifestDataModel.find_date_range(existing.id)
        services = []
        for service, weight_threshold, country, sugg_service in ManifestDataModel.find_distinct_services(existing.id):
            service_parameters = {'service': service, 'weight_threshold': weight_threshold,
                                  'country': country, 'sugg_service': sugg_service}
            service_parameters['weight_threshold'] = ManifestDataModel.weight_threshold_display(service_parameters)
            services.append(service_parameters)
        domestic_zones, international_zones = ManifestDataModel.find_distinct_zones(existing.id)
        existing_headers_ordered = [v if v
                                    not in missing_columns else v+'_gen' for v in ManifestModel.ai1s_headers_ordered]
        return {
            'ordered headers': existing_headers_ordered, 
            'filtered shipments': shipments, 
            'service options': dom_intl, 
            'date range': date_range, 
            'suggested services': services, 
            'domestic zones': domestic_zones, 
            'international zones': international_zones, 
            'curr_page': paginated_result.page, 
            'has_prev': paginated_result.has_prev, 
            'has_next': paginated_result.has_next, 
            'pages': paginated_result.pages, 
            'total': paginated_result.total, 
            'Report': ManifestDataModel.shipment_report(filter_query=query)
        }
    
    @jwt_required()
    def post(self):
        data = request.form.to_dict()
        print(data.keys())
        if 'name' not in data and '_id' not in data:
            print('No name or _id chosen')
            return {'message': 'No name or _id chosen'}, 400
        elif 'name' in data:
            if 'start date' not in data:
                print('Start date not found')
                return {'message': 'Start date not found'}, 400
            if 'end date' not in data:
                print('End date not found')
                return {'message': 'End date not found'}, 400
            start_date, end_date = ManifestModel.vali_date(
                data.pop('start date')), ManifestModel.vali_date(data.pop('end date'))
            if not start_date:
                print(f'Invalid start date {start_date}. Enter in YYYY-mm-dd format.')
                return {'message': f'Invalid start date {start_date}. Enter in YYYY-mm-dd format.'}, 400
            if not end_date:
                print(f'Invalid end date {end_date}. Enter in YYYY-mm-dd format.')
                return {'message': f'Invalid end date {end_date}. Enter in YYYY-mm-dd format.'}, 400
            if end_date < start_date:
                print(f'End date {end_date} can\'t be earlier than start date {start_date}')
                return {'message': f'End date {end_date} can\'t be earlier than start date {start_date}'}, 400
            zone_weights = []
            for i in list(range(1, 9))+list(range(11, 14)):
                if f'zone {i}' not in data:
                    print(f'zone {i} not found')
                    return {'message': f'zone {i} not found'}, 400
                zone_weight = {f'Zone {i}': data.pop(f'zone {i}')}
                if not zone_weight[f'Zone {i}'].isnumeric() or int(zone_weight[f'Zone {i}']) < 0:
                    print(f'Invalid value entered for zone {i}. Enter a non-negative integer.')
                    return {'message': f'Invalid value entered for zone {i}. Enter a non-negative integer.'}, 400
                zone_weight[f'Zone {i}'] = int(zone_weight[f'Zone {i}'])
                zone_weights.append(zone_weight)
            if not zone_weights:
                print('Zone likeliness can\'t be left empty. Enter at least one positive integer for any zone.')
                return {'message': 'Zone likeliness can\'t be left empty. Enter at least one non-negative integer for any zone.'}, 400
            max_weight = max(zone_weights, key=lambda x: tuple(x.items())[0][1])
            if tuple(max_weight.values())[0] == 0:
                print('Zone likeliness can\'t all be 0. Enter at least one positive integer for any zone.')
                return {'message': 'Zone likeliness can\'t all be 0. Enter at least one positive integer for any zone.'}, 400
            name = data.pop('name')
            if len(name) > 45:
                return {'message': f'name is {len(name)} characters long. Please enter max 45 characters.'}, 400
            upload_directory = ManifestModel.upload_directory
            if 'platform' not in data:
                print('No platform selected')
                return {'message': 'No platform selected'}, 400
            if 'manifest' not in request.files:
                print('No manifest file')
                return {'message': 'No manifest file'}, 400
            file = request.files['manifest']
            if file.filename == '':
                return {'message': 'No selected file'}, 400
            if not ManifestModel.allowed_file(file.filename):
                return {'message': 'Unsupported file extension'}, 400
            filename = secure_filename(file.filename)
            api_file_path = os.path.join(upload_directory, filename)
            file.save(api_file_path)
            file_ext = filename.rsplit('.', 1)[1]
            pf = data.pop('platform').lower()
            def_domestic, def_international = data.pop('domestic service', -1), data.pop('international service', -1)
            if def_domestic is -1 or def_international is -1:
                return {'message': 'Missing default services.'}, 400
        else:
            _id = data.pop('_id')
            mongo_manifest = ManifestRaw.find_manifest_by_id(_id)
            if not mongo_manifest:
                return {'message': f'_id {_id} not found.'}
            pf = 'manual'
            name = mongo_manifest.pop('name')
            def_domestic, def_international = mongo_manifest.pop(
                'domestic service', -1), mongo_manifest.pop('international service', -1)
            zone_weights = mongo_manifest.pop('zone weights')
            start_date, end_date = mongo_manifest.pop('start date'), mongo_manifest.pop('end date')
        existing = ManifestModel.find_by_name(name=name)
        if existing:
            return {'message': f'Name {name} already taken.'}, 400

        
        
        @jwt_required()
        def generate_defaults(df):
            generated_columns = {}
            if 'country' not in df.columns:
                df['country'] = 'US'
                generated_columns['country'] = 'country_gen'
            if 'zip' not in df.columns:
                df['zip'] = 'N/A'
                zones_df = pd.DataFrame([tuple(zone_weight.keys())[0] for zone_weight in zone_weights])
                weights = [tuple(zone_weight.values())[0] for zone_weight in zone_weights]
                df['zone'] = zones_df.sample(len(df.index), weights=weights, replace=True).reset_index()[0]
                generated_columns['zip'] = 'zip_gen'
            if 'shipdate' not in df.columns:
                # global start_date, end_date
                # start_date, end_date = pd.to_datetime(start_date), pd.to_datetime(end_date)
                df['shipdate'] = ManifestModel.random_dates(pd.to_datetime(
                    start_date), pd.to_datetime(end_date), len(df.index)).sort_values()
                # df['shipdate'] = df.apply(lambda row: mflib.rand_date_str(row), axis=1)
                # df['shipdate'] = df['shipdate'].dt.strftime('%Y-%m-%d')
                generated_columns['shipdate'] = 'shipdate_gen'
            if 'service' not in df.columns:
                df['service'] = where(df['country'] == 'US', def_domestic, def_international)
                generated_columns['service'] = 'service_gen'
            return df, generated_columns

        @jwt_required()
        def manual(df, headers):
            columns = list(headers.keys())
            dtype = {column: ManifestModel.default_types[headers[column]]
                     for column in columns if headers[column] in ManifestModel.default_types}
            weight_name = None
            concatenate_services = [None, None]
            for k, v in headers.items():
                if v == 'weight':
                    weight_name = k
                if v == 'service provider':
                    concatenate_services[0] = k
                if v == 'service name':
                    concatenate_services[1] = k
            print(df.head(3), headers)
            if weight_name:
                for i, row in df.iterrows():
                    weight_test = str(row[weight_name])
                    if weight_test.replace('.', '').isnumeric():
                        dtype[weight_name] = 'float'
                        df = create_df(columns, dtype, headers, pf, filename, api_file_path, name)
                        break
                    elif 'oz' in weight_test or 'lb' in weight_test or 'lbs' in weight_test:
                        dtype[weight_name] = 'str'
                        df = create_df(columns, dtype, headers, pf, filename, api_file_path, name)
                        df['weight'] = df.apply(lambda row: ManifestModel.w_lbs_or_w_oz(row['weight']), axis=1)
                        break
            else:
                df = create_df(columns, dtype, headers, pf, filename, api_file_path, name)
            if concatenate_services[1]:
                df['service'] = df['service'].combine(df['service 2nd column'], lambda x1, x2: f'{x1} {x2}')
                del df['service 2nd column']
            if 'address' in df.columns:
                df[['zip', 'country']] = df.apply(lambda row: ManifestModel.add_to_zip_ctry(
                    row.address), axis=1, result_type='expand')
            empty_cols = ManifestModel.ai1s_headers.difference(
                set(df.columns)).difference({'shipdate', 'zip', 'country', 'service provider and name', 'service provider', 'service name', 'address'})
            for col in empty_cols:
                df[col] = None
            return df, empty_cols

        @jwt_required()
        def shopify(col_set, df):
            columns = []
            dtype = {}
            headers = {}
            not_found = []
            weight_name = None
            pot_columns = {'orderno': orderno, 'shipdate': shipdate, 'weight': weight,
                           'service': sv, 'zip': address, 'country': address_country, 'price': current_price}
            for pot_c in pot_columns.keys():
                for col in pot_columns[pot_c]['header']:
                    found = False
                    if col in col_set:
                        if pot_c != 'weight':
                            dtype[col] = pot_columns[pot_c]['format']
                        else:
                            weight_name = col
                        headers[col] = pot_c
                        found = True
                        break
                if not found:
                    not_found.append(pot_c)
            columns = list(headers.keys())
            if weight_name:
                print(weight_name)
                df.dropna(subset=[weight_name], inplace=True)
                for i, row in df.iterrows():
                    weight_test = str(row[weight_name])
                    if weight_test.replace('.', '').isnumeric():
                        dtype[weight_name] = weight['format']
                        df = create_df(columns, dtype, headers, pf, filename, api_file_path, name)
                        break
                    elif 'oz' in weight_test or 'lb' in weight_test or 'lbs' in weight_test:
                        dtype[weight_name] = 'str'
                        df = create_df(columns, dtype, headers, pf, filename, api_file_path, name)
                        df['weight'] = df.apply(lambda row: ManifestModel.w_lbs_or_w_oz(row['weight']), axis=1)
                        break
            else:
                df = create_df(columns, dtype, headers, pf, filename, api_file_path, name)
            # At this point we either have a service column with vendor and service code,
            # or a service code column with an optional service provider column.
            # If service provider is given, concatenate with service code.
            empty_cols = ManifestModel.ai1s_headers.difference(
                set(df.columns)).difference({'shipdate', 'zip', 'country', 'service provider and name', 'service provider', 'service name', 'address'})
            for col in empty_cols:
                df[col] = None
            return df, empty_cols

        @jwt_required()
        def shipstation(col_set, df):
            columns = []
            dtype = {}
            headers = {}
            not_found = []
            weight_name = None
            pot_columns = {'orderno': orderno, 'shipdate': shipdate, 'weight': weight,
                           'service': sv, 'zip': address, 'country': address_country, 'price': current_price,
                           'insured': insured_parcel}
            for pot_c in pot_columns.keys():
                for col in pot_columns[pot_c]['header']:
                    found = False
                    if col in col_set:
                        if pot_c != 'weight':
                            dtype[col] = pot_columns[pot_c]['format']
                        else:
                            weight_name = col
                        headers[col] = pot_c
                        found = True
                        break
                if not found:
                    not_found.append(pot_c)
            optional_pot_columns = {'dim1': dim1, 'dim2': dim2, 'dim3': dim3}
            for pot_c in optional_pot_columns.keys():
                for col in optional_pot_columns[pot_c]['header']:
                    found = False
                    if col in col_set:
                        dtype[col] = optional_pot_columns[pot_c]['format']
                        headers[col] = pot_c
                        found = True
                        break
            columns = list(headers.keys())
            sv_main, sv_alt0, sv_alt1 = False, False, False
            # change service alt lookups t oloop instead of looking at first two header fields
            if 'service' in headers.values():
                sv_main = True
            else:
                if sv['header_alt'][0][0] in col_set:
                    sv_alt0 = True
                    columns.append(sv['header_alt'][0][0])
                    headers[columns[-1]] = 'service_provider'
                    dtype[headers[columns[-1]]] = ManifestModel.type_conv[sv['format']]
                elif sv['header_alt'][0][1] in col_set:
                    sv_alt0 = True
                    columns.append(sv['header_alt'][0][1])
                    headers[columns[-1]] = 'service_provider'
                    dtype[headers[columns[-1]]] = ManifestModel.type_conv[sv['format']]
                if sv['header_alt'][1][0] in col_set:
                    sv_alt1 = True
                    columns.append(sv['header_alt'][1][0])
                    headers[columns[-1]] = 'service_code'
                    dtype[headers[columns[-1]]] = ManifestModel.type_conv[sv['format']]
                elif sv['header_alt'][1][1] in col_set:
                    sv_alt1 = True
                    columns.append(sv['header_alt'][1][1])
                    headers[columns[-1]] = 'service_code'
                    dtype[headers[columns[-1]]] = ManifestModel.type_conv[sv['format']]
                # if not (sv_alt0 and sv_alt1):
                #     return  # If service name not included, we can't process the file. Service provider name is not essential
            if weight_name:
                weight_test = str(df[weight_name].iloc[0])
                if weight_test.replace('.', '', 1).isnumeric():
                    dtype[weight_name] = weight['format']
                    df = create_df(columns, dtype, headers, pf, filename, api_file_path, name)
                elif 'oz' in weight_test or 'lb' in weight_test or 'lbs' in weight_test:
                    dtype[weight_name] = 'str'
                    df = create_df(columns, dtype, headers, pf, filename, api_file_path, name)
                    df['weight'] = df.apply(lambda row: ManifestModel.w_lbs_or_w_oz(row['weight']), axis=1)
            else:
                df = create_df(columns, dtype, headers, pf, filename, api_file_path, name)

            if sv_alt0 and sv_alt1:
                # df['service'] = df[['service_provider', 'service_code']].agg(' '.join, axis=1)
                df['service'] = df['service_provider'].combine(df['service_code'], lambda x1, x2: f'{x1} {x2}')
                del df['service_provider']
                del df['service_code']
                {1, 2, 3, 4}, {3, 4, 5}
            empty_cols = ManifestModel.ai1s_headers.difference(
                set(df.columns)).difference({'shipdate', 'zip', 'country', 'service provider and name', 'service provider', 'service name', 'address'})
            for col in empty_cols:
                df[col] = None
            return df, empty_cols

        @jwt_required()
        def sellercloud_shipbridge(col_set, df):
            columns = []
            dtype = {}
            headers = {}
            not_found = []
            weight_name = None
            # [orderno['header'], shipdate['header'], weight['header'],
            # sv['header'], address['header'], current_price['header']]
            pot_columns = {'orderno': orderno, 'shipdate': shipdate, 'weight': weight,
                           'service': sv, 'address': address, 'price': current_price}
            for pot_c in pot_columns.keys():
                for col in pot_columns[pot_c]['header']:
                    found = False
                    if col in col_set:
                        if pot_c != 'weight':
                            dtype[col] = pot_columns[pot_c]['format']
                        else:
                            weight_name = col
                        headers[col] = pot_c
                        found = True
                        break
                if not found:
                    not_found.append(pot_c)
            columns = list(headers.keys())
            if weight_name:
                weight_test = str(df[weight_name].iloc[0])
                if weight_test.replace('.', '').isnumeric():
                    dtype[weight_name] = weight['format']
                    df = create_df(columns, dtype, headers, pf, filename, api_file_path, name)
                elif 'oz' in weight_test or 'lb' in weight_test or 'lbs' in weight_test:
                    dtype[weight_name] = 'str'
                    df = create_df(columns, dtype, headers, pf, filename, api_file_path, name)
                    df['weight'] = df.apply(lambda row: ManifestModel.w_lbs_or_w_oz(row['weight']), axis=1)
            else:
                df = create_df(columns, dtype, headers, pf, filename, api_file_path, name)
            df['weight'] *= 16
            df[['zip', 'country']] = df.apply(lambda row: ManifestModel.add_to_zip_ctry(
                row.address), axis=1, result_type='expand')
            # At this point we either have a service column with vendor and service code,
            # or a service code column with an optional service provider column.
            # If service provider is given, concatenate with service code.
            # if sv_alt0:
            #     df['service'] = df[['service_provider', 'service_code']].agg(' '.join, axis=1)
            #     del df['service_provider']
            #     del df['service_code']
            empty_cols = ManifestModel.ai1s_headers.difference(
                set(df.columns)).difference({'shipdate', 'zip', 'country', 'service provider and name', 'service provider', 'service name', 'address'})
            for col in empty_cols:
                df[col] = None
            return df, empty_cols

        def teapplix(col_set, df):
            columns = []
            dtype = {}
            headers = {}
            not_found = []
            weight_name = None
            pot_columns = {'orderno': orderno, 'shipdate': shipdate, 'weight': weight,
                           'service': sv, 'zip': address, 'country': address_country, 'price': current_price, 'insured': insured_parcel}
            for pot_c in pot_columns.keys():
                for col in pot_columns[pot_c]['header']:
                    found = False
                    if col in col_set:
                        if pot_c != 'weight':
                            dtype[col] = pot_columns[pot_c]['format']
                        else:
                            weight_name = col
                        headers[col] = pot_c
                        found = True
                        break
                if not found:
                    not_found.append(pot_c)
            # optional_pot_columns = {'dim1': dim1, 'dim2': dim2, 'dim3': dim3}
            # for pot_c in optional_pot_columns.keys():
            #     for col in optional_pot_columns[pot_c]['header']:
            #         found = False
            #         if col in col_set:
            #             dtype[col] = optional_pot_columns[pot_c]['format']
            #             headers[col] = pot_c
            #             found = True
            #             break
            columns = list(headers.keys())
            sv_main, sv_alt0, sv_alt1 = False, False, False
            if 'service' in headers.values():
                sv_main = True
            else:
                if sv['header_alt'][0][0] in col_set:
                    sv_alt0 = True
                    columns.append(sv['header_alt'][0][0])
                    headers[columns[-1]] = 'service_provider'
                    dtype[headers[columns[-1]]] = ManifestModel.type_conv[sv['format']]
                elif sv['header_alt'][0][1] in col_set:
                    sv_alt0 = True
                    columns.append(sv['header_alt'][0][1])
                    headers[columns[-1]] = 'service_provider'
                    dtype[headers[columns[-1]]] = ManifestModel.type_conv[sv['format']]
                if sv['header_alt'][1][0] in col_set:
                    sv_alt1 = True
                    columns.append(sv['header_alt'][1][0])
                    headers[columns[-1]] = 'service_code'
                    dtype[headers[columns[-1]]] = ManifestModel.type_conv[sv['format']]
                elif sv['header_alt'][1][1] in col_set:
                    sv_alt1 = True
                    columns.append(sv['header_alt'][1][1])
                    headers[columns[-1]] = 'service_code'
                    dtype[headers[columns[-1]]] = ManifestModel.type_conv[sv['format']]
                    # if not (sv_alt0 and sv_alt1):
                    #     return  # If service name not included, we can't process the file. Service provider name is not essential
            if weight_name:
                df.dropna(subset=[weight_name], inplace=True)
                for i, row in df.iterrows():
                    weight_test = str(row[weight_name])
                    if weight_test.replace('.', '').isnumeric():
                        dtype[weight_name] = weight['format']
                        df = create_df(columns, dtype, headers, pf, filename, api_file_path, name)
                        break
                    elif 'oz' in weight_test or 'lb' in weight_test or 'lbs' in weight_test:
                        dtype[weight_name] = 'str'
                        df = create_df(columns, dtype, headers, pf, filename, api_file_path, name)
                        df['weight'] = df.apply(lambda row: ManifestModel.w_lbs_or_w_oz(row['weight']), axis=1)
                        break
            else:
                df = create_df(columns, dtype, headers, pf, filename, api_file_path, name)
            if sv_alt0 and sv_alt1:

                # df[['service_code', 'service_provider']].replace(np.nan, '', regex=True, inplace=True)
                # df['service'] = df[['service_provider', 'service_code']].agg(' '.join, axis=1)
                df['service'] = df['service_provider'].combine(df['service_code'], lambda x1, x2: f'{x1} {x2}')
                del df['service_provider']
                del df['service_code']
            # At this point we either have a service column with vendor and service code,
            # or a service code column with an optional service provider column.
            # If service provider is given, concatenate with service code.
            empty_cols = ManifestModel.ai1s_headers.difference(
                set(df.columns)).difference({'shipdate', 'zip', 'country', 'service provider and name', 'service provider', 'service name', 'address'})
            for col in empty_cols:
                df[col] = None
            return df, empty_cols

        if pf != 'manual':
            f_ext = filename.rsplit('.', 1)[1]
            if f_ext == 'xlsx':
                df = pd.read_excel(api_file_path, nrows=5)
            elif f_ext == 'csv':
                df = pd.read_csv(api_file_path, nrows=5)
            orderno = ManifestFormatModel.format['order_no'][pf]  # not required
            shipdate = ManifestFormatModel.format['date'][pf]
            weight = ManifestFormatModel.format['weight'][pf]
            sv = ManifestFormatModel.format['service'][pf]  # not required
            address = ManifestFormatModel.format['address'][pf]
            address_country = ManifestFormatModel.format['address_country'].get(pf)
            current_price = ManifestFormatModel.format['current_price'].get(pf)  # optional
            insured_parcel = ManifestFormatModel.format['insured_parcel'].get(pf)  # optional
            dim1 = ManifestFormatModel.format['dim1'].get(pf)  # optional
            dim2 = ManifestFormatModel.format['dim2'].get(pf)  # optional
            dim3 = ManifestFormatModel.format['dim3'].get(pf)  # optional
            col_set = set()
            for i in df.columns:
                col_set.add(i)
            df, empty_cols = locals()[pf](col_set, df)
        else:
            df = ManifestRaw.find_shipments_by_name(name, list(data.keys()), limit=5)
            df, empty_cols = locals()[pf](df, data)

        df.reset_index(inplace=True, drop=True)
        df, generated_columns = generate_defaults(df)

        df[['country', 'zone', 'weight_threshold', 'sugg_service', 'bill_weight', 'dhl_tier_1_2021',
            'dhl_tier_2_2021', 'dhl_tier_3_2021', 'dhl_tier_4_2021',
            'dhl_tier_5_2021', 'dhl_cost_2021', 'usps_2021', 'dhl_cost_shipdate', 'usps_shipdate']] = df.apply(ManifestDataModel.row_to_rate, axis=1, result_type='expand')
        df.weight = df.apply(lambda row: ceil(row.weight) if row.weight < 16 else ceil(row.weight/16)*16, axis=1)
        df.loc[~df['bill_weight'].isna(), 'weight'] = df['bill_weight']
        del df['bill_weight']
        df['shipdate'] = df['shipdate'].dt.strftime('%Y-%m-%d')
        subset = ['service', 'weight_threshold', 'country']
        df.loc[df['zone'].str.len() != 7, 'country'] = 'Intl'
        df = df.replace({nan: None})
        df.sort_values(by=['shipdate', 'orderno'], ascending=[True, True], inplace=True)
        df_unique_services = df[['service', 'weight_threshold', 'country',
                                 'sugg_service']].drop_duplicates(subset=subset, inplace=False)
        df_unique_services['weight_threshold'] = df.apply(ManifestDataModel.weight_threshold_display, axis=1)
        df_unique_services.sort_values(by=['country', 'weight_threshold', 'service'],
                                       ascending=[False, True, True], inplace=True)
        all_zones = df['zone'].drop_duplicates(inplace=False).sort_values(inplace=False)
        domestic_zones = all_zones[all_zones.str.startswith('Zone ')].tolist()
        international_zones = all_zones[~all_zones.str.startswith('Zone ')].tolist()
        date_range = [df['shipdate'].min(), df['shipdate'].max()]
        pd.set_option('display.max_columns', None)
        df = df.rename(generated_columns, axis=1)
        print(df.head(5))
        df.columns = df.columns.str.replace(' ', '_')
        df.columns = df.columns.str.replace('.', '')
        weight_thres_column = df['weight_threshold']
        del df['weight_threshold']
        existing_headers_ordered = ManifestModel.ai1s_headers_ordered
        for i, v in enumerate(existing_headers_ordered):
            if v in generated_columns:
                existing_headers_ordered[i] = generated_columns[v]
        df_size = len(df.index)
        response = {'ordered headers': existing_headers_ordered, 'filtered shipments': df.head(20).to_dict(orient='records'),
                    'service options': dom_intl,
                    'suggested services': df_unique_services.to_dict(orient='records'),
                    'date range': date_range, 'domestic zones': domestic_zones,
                    'international zones': international_zones, 'curr_page': 1, 'has_prev': False,
                    'has_next': True if df_size > 20 else False, 'pages': df_size//20+1,
                    'total': df_size}
        df['weight_threshold'] = weight_thres_column
        generated_columns_rev = {v: k for k, v in generated_columns.items()}
        df = df.rename(generated_columns_rev, axis=1)
        manifest_user = ManifestModel(name=name)
        manifest_user.save_to_db()
        id = ManifestModel.find_by_name(name=name).id
        for generated_column in generated_columns.keys():
            missing_field = ManifestMissingModel(id, generated_column)
            missing_field.save_to_db()
        if 'price' not in df.columns:
            missing_field = ManifestMissingModel(id, 'price')
            missing_field.save_to_db()
        for i, row in df.iterrows():
            shipment = ManifestDataModel(id=id, orderno=row.orderno, shipdate=row.shipdate,
                                         weight=row.weight, service=row.service, zip=row.zip, country=row.country,
                                         insured=row.insured, dim1=row.dim1, dim2=row.dim2, dim3=row.dim3, price=row.price,
                                         zone=row.zone, weight_threshold=row.weight_threshold, sugg_service=row.sugg_service,
                                         dhl_tier_1_2021=row['dhl_tier_1_2021'], dhl_tier_2_2021=row['dhl_tier_2_2021'], dhl_tier_3_2021=row['dhl_tier_3_2021'],
                                         dhl_tier_4_2021=row['dhl_tier_4_2021'], dhl_tier_5_2021=row[
                                             'dhl_tier_5_2021'], dhl_cost_2021=row['dhl_cost_2021'], usps_2021=row['usps_2021'],
                                         dhl_cost_shipdate=row['dhl_cost_shipdate'], usps_shipdate=row['usps_shipdate'])
            shipment.save_to_db()
        ManifestDataModel.commit_to_db()
        df.drop(['orderno', 'zip', 'sugg_service'], inplace=True, axis=1)
        response.update({'Report': ManifestDataModel.shipment_report(df=df)})
        if pf == 'manual':
            ManifestRaw.delete_from_db(_id)
        return response

    # def delete(self):
    #     args = request.args
    #     print(args)
    #     errors = manifest_schema.validate(request.args)
    #     if errors:
    #         return errors, 400
    #     name = args['name']
    #     existing = ManifestModel.find_by_name(name=name)
    #     if not existing:
    #         return {'message': f'Name {name} not found in system.'}, 400
    #     existing.delete_from_db()
    #     return {'message': f'Name {name} successfully deleted.'}

    # @jwt_required()
    # def delete(self):
    #     data = User.parser.parse_args()
    #     existing = UserModel.find_by_username(username=data['username'])
    #     if not existing:
    #         return {'message': f'Username {data["username"]} not found.'}, 400
    #     elif not pbkdf2_sha256.verify(data['password'], existing.hashed_pw):
    #         return {'message': 'Wrong password entered.'}, 400
    #     existing.delete_from_db()
    #     return{'message': f'User {data["username"]} deleted successfully.'}, 200

    # user1 = User.find_by_username('rsolmonovich')
    # user1 = User.add_user('rsolmonovich', '5392295Ai1S')
    # print(user1, len(user1))



# "filters":
#     {"shipdate":
#         [first, last, include?]
#     },
#     {"weight_zone":
#         [{"weight":
#             [min, max, include?],
#         "zone":
#             [min, max, include?]
#         },
#         {"weight":
#             [min, max, include?],
#         "zone":
#             [min, max, include?]
#         },
#         ...
#         ]
#     },
#     "services":
#         [service_params]



class ManifestAuthTest(Resource):
    @ jwt_required()
    def get(self):
        return 'ok'

