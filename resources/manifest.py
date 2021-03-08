from flask_restful import Resource
from models.manifest import ManifestModel, ManifestDataModel, ManifestMissingModel
from schemas.manifest import ManifestSchema, ManifestUpdateSchema
# from flask import jsonify, request
from flask import request
from werkzeug.utils import secure_filename
import os
import pandas as pd
from datetime import datetime
from math import ceil
from app_lib import service as lib_service
from numpy import nan
from flask_jwt import JWT, jwt_required
manifest_schema = ManifestSchema()
manifest_update_schema = ManifestUpdateSchema()
# from flask_jwt import jwt_required
dom_service_names = []
intl_service_names = []
for v in lib_service.values():
    if v[-1] == 'domestic':
        dom_service_names.append(v[3])
    elif v[-1] == 'international':
        intl_service_names.append(v[3])
dom_intl = {'domestic services': dom_service_names, 'international services': intl_service_names}


class ManifestColumns(Resource):
    def get(self):
        return {'headers': sorted(list(ManifestModel.ai1s_headers))}


class ManifestManual(Resource):
    def post(self):
        data = request.form
        print(data.keys())
        if 'manifest' not in request.files:
            print('No manifest file')
            return {'message': 'No manifest file'}, 400
        if 'name' not in data:
            print('No name chosen')
            return {'message': 'No name chosen'}, 400
        if 'start date' not in data:
            print('Start date not found')
            return {'message': 'Start date not found'}, 400
        if 'end date' not in data:
            print('End date not found')
            return {'message': 'End date not found'}, 400
        start_date, end_date = ManifestModel.vali_date(data['start date']), ManifestModel.vali_date(data['end date'])
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
            zone_weight = {f'Zone {i}': data[f'zone {i}']}
            if not zone_weight[f'Zone {i}'].isnumeric() or int(zone_weight[f'Zone {i}']) < 0:
                print(f'Invalid value entered for zone {i}. Enter a non-negative integer.')
                return {'message': f'Invalid value entered for zone {i}. Enter a non-negative integer.'}, 400
            zone_weight[f'Zone {i}'] = int(zone_weight[f'Zone {i}'])
            zone_weights.append(zone_weight)
        # if not data[f'zone + {str(i)}'].isnumeric() or int(data[f'zone + {str(i)}']) < 0:
        #     return jsonify({'message': f'{data["zone "+ str(i)]} is an invalid value. Enter a non-negative integer.'}), 400
        # default_parameters = data['default parameters']
        # print(default_parameters)
        # if 'shipdate' not in default_parameters:
        #     return jsonify({'message': 'No shipdate range sent'}), 400
        # if 'zone_weights' not in default_parameters:
        #     return jsonify({'message': 'No zone weights sent'}), 400
        # shipdate_range, zone_weights = default_parameters['shipdate'], default_parameters['zone_weights']
        name = data['name']
        if len(name) >= 45:
            return {'message': f'name is {len(name)} characters long. Please enter max 45 characters.'}, 400
        file = request.files['manifest']
        if file.filename == '':
            return {'message': 'No selected file'}, 400
        if not ManifestModel.allowed_file(file.filename):
            return {'message': 'Unsupported file extension'}, 400
        filename = secure_filename(file.filename)
        upload_directory = ManifestModel.upload_directory
        api_file_path = os.path.join(upload_directory, filename)
        file.save(api_file_path)
        print(filename)
        existing = ManifestModel.find_by_name(name=name)
        if existing:
            print(existing)
            print(f'Name {name} already taken.')
            return {'message': f'Name {name} already taken.'}, 400
        if len(name) > 45:
            return {'message': 'Invalid name - cannot exceed 45 characters.'}, 400
        f_ext = filename.rsplit('.', 1)[1]
        if f_ext == 'xlsx':
            df = pd.read_excel(api_file_path, nrows=10, dtype=str)
        elif f_ext == 'csv':
            df = pd.read_csv(api_file_path, nrows=10, dtype=str)
        df = df.replace({nan: None})
        response = {'shipments': df.to_dict(orient='list'), 'file name': filename}
        response.update(data.to_dict())
        return response


class ManifestNames(Resource):
    def get(self):
        all_manifests = ManifestModel.find_all()
        json_manifests = {manifest.name: str(manifest.init_date) for manifest in all_manifests}
        print(json_manifests)
        return json_manifests

    def put(self):
        args = request.args
        print(args.keys())
        errors = manifest_update_schema.validate(request.args)
        if errors:
            return errors, 400
        old_name = args['old_name']
        new_name = args['new_name']
        existing = ManifestModel.find_by_name(name=old_name)
        if not existing:
            return {'message': f'Old name {old_name} not found in system'}, 400
        if new_name == old_name:
            return {'message': f'Old name {old_name} has to be different than new name.'}, 400
        check_new = ManifestModel.find_by_name(name=new_name)
        if check_new:
            return {'message': f'New name {new_name} already taken.'}, 400
        existing.name = new_name
        existing.save_to_db()
        return {'message': 'Name successfully updated.'}

    def delete(self):
        args = request.args
        print(args.keys())
        errors = manifest_schema.validate(request.args)
        if errors:
            return errors, 400
        name = args['name']
        existing = ManifestModel.find_by_name(name=name)
        if not existing:
            return {'message': f'Name {name} not found in system.'}, 400
        existing.delete_from_db()
        return {'message': f'Name {name} successfully deleted.'}


class Manifest(Resource):
    def get(self):
        # args = request.args
        # print(args)
        # all_manifests = ManifestModel.find_all()
        # # for manifest in all_manifests:
        # #     print(manifest.name, manifest.init_date)
        # json_manifests = {manifest.name: str(manifest.init_date) for manifest in all_manifests}
        # print(json_manifests)
        # return json_manifests
        # return json_manifests
        # return 'in construction'
        args = request.args
        print(args.keys())
        errors = manifest_schema.validate(request.args)
        if errors:
            return errors, 400
        name = args['name']
        existing = ManifestModel.find_by_name(name=name.replace('%20', ' '))
        if not existing:
            return {'message': f'Name {name} not found in system.'}, 400
        missing_columns = ManifestMissingModel.json(_id=existing.id)
        print(missing_columns)
        # shipments = {'id': [], 'orderno': [],
        #              'shipdate' if 'shipdate' not in missing_columns else 'shipdate (gen.)': [],
        #              'weight': [], 'service': [], 'zip': [],
        #              'country' if 'country' not in missing_columns else 'country (gen.)': [],
        #              'insured': [], 'dim1': [], 'dim2': [], 'dim3': [], 'price': [],
        #              'zone' if 'zone' not in missing_columns else 'zone (gen.)': [],
        #              'weight_threshold': [], 'sugg_service': [], 'tier_1_2021': [], 'tier_2_2021': [],
        #              'tier_3_2021': [], 'tier_4_2021': [], 'tier_5_2021': [], 'dhl_2021': [],
        #              'usps_2021': [], 'shipdate_dhl': [], 'shipdate_usps': []}
        shipments = []
        for shipment_item in ManifestDataModel.find_all_shipments(existing.id):
            # '_sa_instance_state'
            shipment = shipment_item.__dict__
            del shipment['_sa_instance_state']
            shipment['shipdate'] = str(shipment['shipdate'])
            for missing_column in missing_columns:
                shipment[missing_column+' (gen.)'] = shipment.pop(missing_column)
            shipments.append(shipment)
        #     print(shipment.keys())
        # print(shipments)
        return {'filtered shipments': shipments}

    def post(self):
        data = request.form.to_dict()
        # ImmutableMultiDict([('Shipment #', 'dim1'), ('Order #', 'dim3'), ('Recipient', 'orderno'), ('file name', 'shipstation_Shippping_details_2.xlsx'), ('name', 'asss'), ('start date', '2021-01-03'), ('end date', '2021-03-04'), ('zone 1', '3'), ('zone 2', '3'), ('zone 3', '3'), ('zone 4', '3'), ('zone 5', '3'), ('zone 6', '3'), ('zone 7', '3'), ('zone 8', '3'), ('zone 11', '1'), ('zone 12', '1'), ('zone 13', '1'), ('domestic service', 'first Class Parcels'), ('international service', 'DHL Packket International')])
        print(data.keys())
        if 'name' not in data:
            print('No name chosen')
            return {'message': 'No name chosen'}, 400
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
        # if not data[f'zone + {str(i)}'].isnumeric() or int(data[f'zone + {str(i)}']) < 0:
        #     return jsonify({'message': f'{data["zone "+ str(i)]} is an invalid value. Enter a non-negative integer.'}), 400
        # default_parameters = data['default parameters']
        # print(default_parameters)
        # if 'shipdate' not in default_parameters:
        #     return jsonify({'message': 'No shipdate range sent'}), 400
        # if 'zone_weights' not in default_parameters:
        #     return jsonify({'message': 'No zone weights sent'}), 400
        # shipdate_range, zone_weights = default_parameters['shipdate'], default_parameters['zone_weights']
        name = data.pop('name')
        if len(name) >= 45:
            return {'message': f'name is {len(name)} characters long. Please enter max 45 characters.'}, 400
        upload_directory = ManifestModel.upload_directory
        if 'platform' not in data:
            if 'file name' not in data:
                print('No platform selected')
                return {'message': 'No platform selected'}, 400
            pf = 'manual'
            filename = filename = secure_filename(data.pop('file name'))
            api_file_path = os.path.join(upload_directory, filename)
        else:
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
        print(pf)
        print(filename)
        print(data)
        # sql_check = 'SELECT name FROM manifest WHERE name = %s'
        existing = ManifestModel.find_by_name(name=name)
        if existing:
            print(existing)
            print(f'Name {name} already taken.')
            return {'message': f'Name {name} already taken.'}, 400
        if len(name) > 45:
            return {'message': 'Invalid name - cannot exceed 45 characters.'}, 400

        def create_df(columns, dtype, headers):
            # def create_df(path, columns, dtype, headers):
            f_ext = filename.rsplit('.', 1)[1]
            print(columns)
            print(dtype)
            print(headers)
            if f_ext == 'xlsx':
                df = pd.read_excel(api_file_path, usecols=columns, dtype=dtype)
            elif f_ext == 'csv':
                df = pd.read_csv(api_file_path, usecols=columns, dtype=dtype)
            # parse_dates=[columns[1]], date_parser=dateparse
            print(columns)
            df = df[columns]
            print(columns)
            df = df.rename(columns=headers)
            # df['orderno'] = df.index
            df.dropna(how='all', inplace=True)
            dup_df = df[df.duplicated(subset=['orderno'], keep=False)]
            duplicate_index = 0
            for i, row in dup_df.iterrows():
                df.loc[i, 'orderno'] += f'-{duplicate_index}'
                duplicate_index += 1
            df.dropna(subset=['weight'], inplace=True)
            if 'insured' in dtype and dtype['insured'] == 'float':
                df['insured'].fillna(value=0, inplace=True)
                df['insured'] = df['insured'].astype(bool)
            # df.loc[~df['bill weight'].isna(), 'weight'] = df['bill weight']
            if 'shipdate' in df.columns:
                shipdate_is_numeric = False
                for i, row in df.iterrows():
                    if i == 5:
                        break
                    shipdate = row.shipdate
                    if isinstance(shipdate, int):
                        shipdate_is_numeric = True
                        # df = df.astype({'shipdate': 'int'})
                        # break
                    elif isinstance(shipdate, float):
                        df = df.astype({'shipdate': 'float'})
                        df = df.astype({'shipdate': 'int'})
                        shipdate_is_numeric = True
                        break
                if shipdate_is_numeric:
                    df['shipdate'] = df.apply(lambda row: datetime.fromordinal(
                        datetime(1900, 1, 1).toordinal() + int(row['shipdate']) - 2), axis=1)
                else:
                    df['shipdate'] = pd.to_datetime(df['shipdate'])
            pd.set_option('display.max_columns', None)
            return df

        def generate_defaults(df):
            generated_columns = {}
            print(df.columns)
            # Fix country missing issue...
            if 'country' not in df.columns:
                df['country'] = 'US'
            if 'zip' not in df.columns:
                df['zip'] = 'N/A'
                zones_df = pd.DataFrame([tuple(zone_weight.keys())[0] for zone_weight in zone_weights])
                weights = [tuple(zone_weight.values())[0] for zone_weight in zone_weights]
                # df1 = pd.DataFrame([None]*100, columns=['zones'])
                # print(zones_df.sample(100, weights=zone_weights, replace=True, axis=1))
                # x = zones_df.sample(100, weights=zone_weights, replace=True).reset_index()[0]
                # print(x.head(20))
                # print(x.columns)
                # weights = x
                # print(list(weights))
                df['zone'] = zones_df.sample(len(df.index), weights=weights, replace=True).reset_index()[0]
                print(df.head(8))
                generated_columns['zip'], generated_columns['country'] = 'zip_gen', 'country_gen'
            if 'shipdate' not in df.columns:
                # global start_date, end_date
                # start_date, end_date = pd.to_datetime(start_date), pd.to_datetime(end_date)
                df['shipdate'] = ManifestModel.random_dates(pd.to_datetime(
                    start_date), pd.to_datetime(end_date), len(df.index)).sort_values()
                # df['shipdate'] = df.apply(lambda row: mflib.rand_date_str(row), axis=1)
                # df['shipdate'] = df['shipdate'].dt.strftime('%Y-%m-%d')
                generated_columns['shipdate'] = 'shipdate_gen'
            # else:
            #     print(df['country'])
            return df, generated_columns

        def manual(df, headers):
            columns = list(headers.keys())
            dtype = {column: 'str' for column in columns}
            # df = create_df(columns, dtype, headers)
            weight_name = None
            concatenate_services = [None, None]
            for k, v in headers.items():
                if v == 'weight':
                    weight_name = k
                if v == 'service provider':
                    concatenate_services[0] = k
                if v == 'service name':
                    concatenate_services[1] = k
            if weight_name:
                for i, row in df.iterrows():
                    weight_test = str(row[weight_name])
                    if weight_test.replace('.', '').isnumeric():
                        dtype[weight_name] = 'float'
                        df = create_df(columns, dtype, headers)
                        break
                    elif 'oz' in weight_test or 'lb' in weight_test or 'lbs' in weight_test:
                        dtype[weight_name] = 'str'
                        df = create_df(columns, dtype, headers)
                        df['weight'] = df.apply(lambda row: ManifestModel.w_lbs_or_w_oz(row['weight']), axis=1)
                        break
            else:
                df = create_df(columns, dtype, headers)
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

        def shopify(col_set, df):
            columns = []
            dtype = {}
            headers = {}
            not_found = []
            weight_name = None
            pot_columns = {'orderno': orderno, 'shipdate': shipdate, 'weight': weight,
                           'service': sv, 'zip': address, 'country': address1, 'price': current_price}
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
                        df = create_df(columns, dtype, headers)
                        break
                    elif 'oz' in weight_test or 'lb' in weight_test or 'lbs' in weight_test:
                        dtype[weight_name] = 'str'
                        df = create_df(columns, dtype, headers)
                        df['weight'] = df.apply(lambda row: ManifestModel.w_lbs_or_w_oz(row['weight']), axis=1)
                        break
            else:
                df = create_df(columns, dtype, headers)
            # At this point we either have a service column with vendor and service code,
            # or a service code column with an optional service provider column.
            # If service provider is given, concatenate with service code.
            # df['insured'] = (df['insured'] > 0)
            # print(df.head())
            empty_cols = ManifestModel.ai1s_headers.difference(
                set(df.columns)).difference({'shipdate', 'zip', 'country', 'service provider and name', 'service provider', 'service name', 'address'})
            for col in empty_cols:
                df[col] = None
            return df, empty_cols

        def shipstation(col_set, df):
            columns = []
            dtype = {}
            headers = {}
            not_found = []
            weight_name = None
            pot_columns = {'orderno': orderno, 'shipdate': shipdate, 'weight': weight,
                           'service': sv, 'zip': address, 'country': address1, 'price': current_price,
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
            print(columns)
            print(dtype)
            print(headers)
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
                weight_test = str(df[weight_name].iloc[0])
                if weight_test.replace('.', '').isnumeric():
                    dtype[weight_name] = weight['format']
                    df = create_df(columns, dtype, headers)
                elif 'oz' in weight_test or 'lb' in weight_test or 'lbs' in weight_test:
                    dtype[weight_name] = 'str'
                    df = create_df(columns, dtype, headers)
                    df['weight'] = df.apply(lambda row: ManifestModel.w_lbs_or_w_oz(row['weight']), axis=1)
            else:
                df = create_df(columns, dtype, headers)
            # At this point we either have a service column with vendor and service code,
            # or a service code column with an optional service provider column.
            # If service provider is given, concatenate with service code.
            if sv_alt0 and sv_alt1:
                # df['service'] = df[['service_provider', 'service_code']].agg(' '.join, axis=1)
                df['service'] = df['service_provider'].combine(df['service_code'], lambda x1, x2: f'{x1} {x2}')
                del df['service_provider']
                del df['service_code']
            empty_cols = ManifestModel.ai1s_headers.difference(
                set(df.columns)).difference({'shipdate', 'zip', 'country', 'service provider and name', 'service provider', 'service name', 'address'})
            for col in empty_cols:
                df[col] = None
            return df, empty_cols

        def sellercloud_shipbridge(col_set, df):
            print(col_set)
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
            print(columns)
            print(dtype)
            print(headers)
            # sv_main, sv_alt0, sv_alt1 = False, False, False
            # if 'service' in headers.values():
            #     sv_main = True
            # else:
            #     if sv['header_alt'][0][0] in col_set:
            #         sv_alt0 = True
            #         columns.append(sv['header_alt'][0][0])
            #         dtype[columns[-1]] = type[sv['format']]
            #         headers[columns[-1]] = 'service_provider'
            #     elif sv['header_alt'][0][1] in col_set:
            #         sv_alt0 = True
            #         columns.append(sv['header_alt'][0][1])
            #         dtype[columns[-1]] = type[sv['format']]
            #         headers[columns[-1]] = 'service_provider'
            #     if sv['header_alt'][1][0] in col_set:
            #         sv_alt1 = True
            #         columns.append(sv['header_alt'][1][0])
            #         dtype[columns[-1]] = type[sv['format']]
            #         headers[columns[-1]] = 'service_code'
            #     elif sv['header_alt'][1][1] in col_set:
            #         sv_alt1 = True
            #         columns.append(sv['header_alt'][1][1])
            #         dtype[columns[-1]] = type[sv['format']]
            #         headers[columns[-1]] = 'service_code'
            #     if not (sv_alt0 and sv_alt1):
            #         return  # If service name not included, we can't process the file. Service provider name is not essential
            if weight_name:
                weight_test = str(df[weight_name].iloc[0])
                if weight_test.replace('.', '').isnumeric():
                    dtype[weight_name] = weight['format']
                    print(weight_test)
                    df = create_df(columns, dtype, headers)
                elif 'oz' in weight_test or 'lb' in weight_test or 'lbs' in weight_test:
                    print(weight_test)
                    dtype[weight_name] = 'str'
                    df = create_df(columns, dtype, headers)
                    df['weight'] = df.apply(lambda row: ManifestModel.w_lbs_or_w_oz(row['weight']), axis=1)
            else:
                df = create_df(columns, dtype, headers)
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
                           'service': sv, 'zip': address, 'country': address1, 'price': current_price, 'insured': insured_parcel}
            for pot_c in pot_columns.keys():
                print(pot_c)
                for col in pot_columns[pot_c]['header']:
                    found = False
                    if col in col_set:
                        print(f'found column {col}')
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
            print(columns)
            print(dtype)
            print(headers)
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
            # dtype = {columns[2]: type[address['format']],
            #          columns[3]: type[address['format']], columns[4]: type[current_price['format']],
            #          columns[5]: type[dimensions['format']], columns[6]: type[dimensions['format']],
            #          columns[7]: type[dimensions['format']], columns[8]: type[insured_parcel['format']]}
            # headers = {columns[0]: 'shipdate', columns[1]: 'weight',
            #            columns[2]: 'zip', columns[3]: 'country', columns[4]: 'price',
            #            columns[5]: 'dim0', columns[6]: 'dim1', columns[7]: 'dim2', columns[8]: 'insured'}
            # if (dimensions['header0'] not in col_set):
            #     del dtype[columns[-4]]
            #     del dtype[columns[-3]]
            #     del dtype[columns[-2]]
            #     del headers[columns[-4]]
            #     del headers[columns[-3]]
            #     del headers[columns[-2]]
            #     print(columns)
            #     columns = columns[:-4]+[columns[-1]]
            #     print(columns)
            if weight_name:
                print(weight_name)
                df.dropna(subset=[weight_name], inplace=True)
                for i, row in df.iterrows():
                    weight_test = str(row[weight_name])
                    if weight_test.replace('.', '').isnumeric():
                        dtype[weight_name] = weight['format']
                        df = create_df(columns, dtype, headers)
                        break
                    elif 'oz' in weight_test or 'lb' in weight_test or 'lbs' in weight_test:
                        dtype[weight_name] = 'str'
                        df = create_df(columns, dtype, headers)
                        df['weight'] = df.apply(lambda row: ManifestModel.w_lbs_or_w_oz(row['weight']), axis=1)
                        break
            else:
                df = create_df(columns, dtype, headers)
            if sv_alt0 and sv_alt1:

                # df[['service_code', 'service_provider']].replace(np.nan, '', regex=True, inplace=True)
                # df['service'] = df[['service_provider', 'service_code']].agg(' '.join, axis=1)
                df['service'] = df['service_provider'].combine(df['service_code'], lambda x1, x2: f'{x1} {x2}')
                del df['service_provider']
                del df['service_code']
            # At this point we either have a service column with vendor and service code,
            # or a service code column with an optional service provider column.
            # If service provider is given, concatenate with service code.
            # df['insured'] = (df['insured'] > 0)
            # print(df.head())
            empty_cols = ManifestModel.ai1s_headers.difference(
                set(df.columns)).difference({'shipdate', 'zip', 'country', 'service provider and name', 'service provider', 'service name', 'address'})
            for col in empty_cols:
                df[col] = None
            return df, empty_cols

        f_ext = filename.rsplit('.', 1)[1]
        if f_ext == 'xlsx':
            df = pd.read_excel(api_file_path, nrows=5)
        elif f_ext == 'csv':
            df = pd.read_csv(api_file_path, nrows=5)
        if pf != 'manual':
            pd.set_option('display.max_columns', None)
            orderno = ManifestModel.format['order_no'][pf]  # not required
            shipdate = ManifestModel.format['date'][pf]
            weight = ManifestModel.format['weight'][pf]
            sv = ManifestModel.format['service'][pf]  # not required
            address = ManifestModel.format['address'][pf]
            address1 = ManifestModel.format['address1'].get(pf)
            current_price = ManifestModel.format['current_price'].get(pf)  # optional
            insured_parcel = ManifestModel.format['insured_parcel'].get(pf)  # optional
            dim1 = ManifestModel.format['dim1'].get(pf)  # optional
            dim2 = ManifestModel.format['dim2'].get(pf)  # optional
            dim3 = ManifestModel.format['dim3'].get(pf)  # optional
            col_set = set()
            for i in df.columns:
                col_set.add(i)
            df, empty_cols = locals()[pf](col_set, df)
        else:
            df, empty_cols = locals()[pf](df, data)

        # try:
        #     df = locals()[pf](col_set, df)
        # except:
        #     error = {'message': f'Could not recognize the data. Make sure the platform is really {pf}.'}

        print(df.head(10))
        df.reset_index(inplace=True, drop=True)
        df, generated_columns = generate_defaults(df)
        # if ''
        # print(df.iloc[:, :])
        df[['country', 'zone', 'weight_threshold', 'sugg_service', 'bill_weight', 'tier_1_2021',
            'tier_2_2021', 'tier_3_2021', 'tier_4_2021',
            'tier_5_2021', 'dhl_2021', 'usps_2021', 'dhl_shipdate', 'usps_shipdate']] = df.apply(
            lambda row: ManifestDataModel.row_to_rate(row), axis=1, result_type='expand')
        df.weight = df.apply(lambda row: ceil(row.weight) if row.weight < 16 else ceil(row.weight/16)*16, axis=1)
        df.loc[~df['bill_weight'].isna(), 'weight'] = df['bill_weight']
        del df['bill_weight']
        # df.astype({'2021 tier 1': 'float', '2021 tier 2': 'float'}).dtypes
        df['shipdate'] = df['shipdate'].dt.strftime('%Y-%m-%d')
        # return jsonify({'dataframe': df.values.tolist()})
        # result = jsonify({'object1': 1, 'object2': 2})
        # print(result, 'x')
        manifest_id_insert = 'INSERT INTO manifest (name, init_date) \
            VALUES (%s, %s)'
        manifest_data_insert = 'INSERT INTO manifest_data (id, order_no, ship_date, weight, service \
            address, address1, insured, dim1, dim2, dim3) \
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        # mycursor.execute(manifest_id_insert, (name, date.today()))
        # mydb.commit()
        # stmt = select([table.columns.id]).where(table.columns.name == name)
        # res = conn.execute(stmt).fetchone()
        # df.to_sql(con=conn, name='manifest_data', if_exists='append', index=False)
        # mycursor.executemany(manifest_data_insert, (name, date.today()))
        # mydb.commit()
        # table = Table(
        #     'manifest',
        #     metadata,
        #     autoload=True,
        #     autoload_with=engine
        # )
        # stmt = select([table.columns.id]).where(table.columns.name == 'test')
        # results = conn.execute(stmt).fetchone()
        # print(results)
        summary = None
        # engine.execute(manifest_id_insert, (name, date.today()))
        subset = ['service', 'weight_threshold', 'country']
        df.loc[df['zone'].str.len() != 7, 'country'] = 'Intl'
        df = df.replace({nan: None})
        df_unique_services = df[['service', 'weight_threshold', 'country',
                                 'sugg_service']].drop_duplicates(subset=subset, inplace=False)
        df_unique_services['weight_threshold'] = df.apply(
            lambda row: ManifestDataModel.weight_threshold_display(row), axis=1)
        df_unique_services.sort_values(by=['country', 'weight_threshold', 'service'],
                                       ascending=[False, True, True], inplace=True)
        date_range = [df['shipdate'].min(), df['shipdate'].max()]
        pd.set_option('display.max_columns', None)
        df = df.rename(generated_columns, axis=1)
        print(df.head(20))
        # dom_service_names = []
        # intl_service_names = []
        # for v in ManifestModel.service.values():
        #     if v[-1] == 'domestic':
        #         dom_service_names.append(v[3])
        #     elif v[-1] == 'international':
        #         intl_service_names.append(v[3])
        # dom_intl = {'domestic services': dom_service_names, 'international services': intl_service_names}
        df.columns = df.columns.str.replace(' ', '_')
        df.columns = df.columns.str.replace('.', '')
        response = {'filtered shipments': {k: v for k, v in df.to_dict(orient='list').items() if k != 'weight_threshold'},
                    'summary': summary, 'service options': dom_intl,
                    'suggested services': df_unique_services.to_dict(orient='records'),
                    'date range': date_range}
        generated_columns_rev = {v: k for k, v in generated_columns.items()}
        df = df.rename(generated_columns_rev, axis=1)
        manifest_user = ManifestModel(name=name)
        manifest_user.save_to_db()
        id = ManifestModel.find_by_name(name=name).id
        for generated_column in generated_columns.keys():
            missing_field = ManifestMissingModel(id, generated_column)
            missing_field.save_to_db()
        # df['id'] = id
        for i, row in df.iterrows():
            shipment = ManifestDataModel(id=id, orderno=row.orderno, shipdate=row.shipdate,
                                         weight=row.weight, service=row.service, zip=row.zip, country=row.country,
                                         insured=row.insured, dim1=row.dim1, dim2=row.dim2, dim3=row.dim3, price=row.price,
                                         zone=row.zone, weight_threshold=row.weight_threshold, sugg_service=row.sugg_service,
                                         tier_1_2021=row['tier_1_2021'], tier_2_2021=row['tier_2_2021'], tier_3_2021=row['tier_3_2021'],
                                         tier_4_2021=row['tier_4_2021'], tier_5_2021=row['tier_5_2021'], dhl_2021=row['dhl_2021'], usps_2021=row['usps_2021'],
                                         shipdate_dhl=row['dhl_shipdate'], shipdate_usps=row['usps_shipdate'])
            shipment.save_to_db()
        ManifestDataModel.commit_to_db()

        # try:
        #     df.to_sql(con=conn, name='manifest_data', if_exists='append', index=False)
        # except:
        #     manifest_user.delete_from_db()

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


class ManifestFilter(Resource):
    def post(self):
        request_data = request.get_json()
        print(request_data.keys())
        if 'name' not in request_data:
            print('No name chosen')
            return {'message': 'No name chosen'}, 400
        manifest = ManifestModel.find_by_name(name=request_data['name'])
        if not manifest:
            return {'message': 'Name not found in system'}, 400
        if 'filters' not in request_data:
            print('No filters selected')
            return {'message': 'No filters selected'}, 400
        filters = request_data['filters']
        id = manifest.id
        # ManifestModel.filtered_shipments_by_id(filters, id)
        filters = request_data['filters']
        print(filters)
        missing_columns = ManifestMissingModel.json(_id=id)
        service_replacements = {}
        for service_override in filters['services']:
            if service_override['service'] is not None:
                service_replacements[(service_override['service name'], service_override['location'],
                                      service_override['weight threshold'])] = service_override['service']
        shipments = []
        for shipment_item in ManifestDataModel.find_filtered_shipments(id, filters):
            # '_sa_instance_state'
            if (shipment_item.service, shipment_item.country, shipment_item.weight_threshold) in service_replacements:
                shipment_item = shipment_item.correct_service_rates(service_replacements[(
                    shipment_item.service, shipment_item.country, shipment_item.weight_threshold)])
            shipment = shipment_item.__dict__
            del shipment['_sa_instance_state']
            shipment['shipdate'] = str(shipment['shipdate'])
            for missing_column in missing_columns:
                shipment[missing_column+' (gen.)'] = shipment.pop(missing_column)
            shipments.append(shipment)
        return {'filtered shipments': shipments, 'filters': filters}
        # shipments = ManifestModel.manifest_shipments(_id=id, filter=None)
        # print(id)
        # print(shipments)
        # for shipment in shipments:
        #     print(shipment.shipdate)
        # df = pd.read_sql(query.statement, query.session.bind)
        # return request_data


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
