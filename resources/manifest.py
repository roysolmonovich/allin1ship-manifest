from flask_restful import Resource
from models.manifest import ManifestModel, ManifestDataModel, ManifestMissingModel, ManifestFormatModel, ManifestRaw
from schemas.manifest import ManifestSchema, ManifestUpdateSchema, ManifestFormatSchema, ManifestServiceUpdateSchema
# from flask import jsonify, request
from flask import request
from werkzeug.utils import secure_filename
import os
# from celery import Celery
import pandas as pd
from datetime import datetime
from math import ceil
from app_lib import service as lib_service
from numpy import nan, where
from flask_jwt_extended import jwt_required

import pdb

# Redis is currently not used
if os.environ.get('REDIS_URL') is not None:
    redis_cred = os.environ['REDIS_URL']
else:
    # from c import redis_cred
    redis_cred = 'redis://localhost:6379'
# from xlrd import open_workbook
manifest_schema = ManifestSchema()
manifest_update_schema = ManifestUpdateSchema()
manifest_format_schema = ManifestFormatSchema()
manifest_service_update_schema = ManifestServiceUpdateSchema()
# celery = Celery('apptst_h - Copy', backend='redis',
#                 broker=redis_cred)

# from flask_jwt import jwt_required
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


class ManifestColumns(Resource):
    @jwt_required()
    def get(self):
        return {'headers': sorted(list(ManifestModel.ai1s_headers_required))}


# Not in use - could be used for celery app to make function calls async
class ManifestTaskStatus(Resource):
    @jwt_required()
    def get(self):
        task_id = request.args.get('task_id')
        print(self)
        print(task_id)
        task = get_task.AsyncResult(task_id)
        if task.state == 'PENDING':
            # job did not start yet
            response = {
                'state': task.state,
                'current': 0,
                'total': 1,
                'status': 'Pending...'
            }
        elif task.state != 'FAILURE':
            response = {
                'state': task.state,
                'current': task.info.get('current', 0),
                'total': task.info.get('total', 1),
                'status': task.info.get('status', '')
            }
            if 'result' in task.info:
                response['result'] = task.info['result']
        else:
            # something went wrong in the background job
            response = {
                'state': task.state,
                'current': 1,
                'total': 1,
                'status': str(task.info),  # this is the exception raised
            }
        return response


# class ManifestColumnsTest(Resource):
#     def get(self):
#         task = get_task.apply_async()
#         return {'message': 'In progress.'}, 202, {'Location': url_for('manifesttaskstatus', task_id=task.id)}


class ManifestManual(Resource):
    @jwt_required()
    def post(self):
        data = request.form.to_dict()
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
        name = data['name']
        if len(name) > 45:
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
        def_domestic, def_international = data.get('domestic service', -1), data.get('international service', -1)
        if def_domestic is -1:
            return {'message': 'Missing domestic service.'}, 400
        if def_international is -1:
            return {'message': 'Missing international service.'}, 400
        existing = ManifestModel.find_by_name(name=name)
        if existing:
            print(existing)
            print(f'Name {name} already taken.')
            return {'message': f'Name {name} already taken.'}, 400
        f_ext = filename.rsplit('.', 1)[1]
        if f_ext == 'xlsx':
            # df = pd.read_excel(api_file_path, dtype=str)
            df = pd.read_excel(api_file_path, engine='openpyxl', dtype=str)
        elif f_ext == 'csv':
            df = pd.read_csv(api_file_path, dtype=str)
        df = df.replace({nan: None})
        data['zone weights'] = zone_weights
        _id = ManifestRaw.save_to_db(df, **data)
        response = {'shipments': df.head(10).to_dict(orient='records'), '_id': str(_id)}
        # response.update(data)
        return response


class ManifestNames(Resource):
    @jwt_required()
    def get(self):
        all_manifests = ManifestModel.find_all()
        json_manifests = {manifest.name: str(manifest.init_time)[:10] for manifest in all_manifests}
        return json_manifests
    @jwt_required()
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
    
    @jwt_required()
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
    @jwt_required()
    def get(self):
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
        paginated_result = ManifestDataModel.find_all_shipments(existing.id)
        query = ManifestDataModel.find_all_shipments_query(existing.id)
        for shipment_item in paginated_result.items:
            shipment = shipment_item.__dict__
            del shipment['_sa_instance_state']
            shipment['shipdate'] = str(shipment['shipdate'])
            for missing_column in missing_columns:
                shipment[missing_column+'_gen'] = shipment.pop(missing_column)
            shipments.append(shipment)
        # date_range = [shipments[0].get('shipdate') or shipments[0].get(
        #     'shipdate_gen'), shipments[-1].get('shipdate') or shipments[-1].get('shipdate_gen')] if shipments else [None, None]
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
        return {'ordered headers': existing_headers_ordered, 'filtered shipments': shipments, 'service options': dom_intl, 'date range': date_range, 'suggested services': services, 'domestic zones': domestic_zones, 'international zones': international_zones, 'curr_page': paginated_result.page, 'has_prev': paginated_result.has_prev, 'has_next': paginated_result.has_next, 'pages': paginated_result.pages, 'total': paginated_result.total, 'Report': ManifestDataModel.shipment_report(filter_query=query)}
    
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
        # if 'platform' not in data:
        #     if 'file name' not in data:
        #         print('No platform selected')
        #         return {'message': 'No platform selected'}, 400
        #     pf = 'manual'
        #     filename = filename = secure_filename(data.pop('file name'))
        #     api_file_path = os.path.join(upload_directory, filename)
        # else:
        #     if 'manifest' not in request.files:
        #         print('No manifest file')
        #         return {'message': 'No manifest file'}, 400
        #     file = request.files['manifest']
        #     if file.filename == '':
        #         return {'message': 'No selected file'}, 400
        #     if not ManifestModel.allowed_file(file.filename):
        #         return {'message': 'Unsupported file extension'}, 400
        #     filename = secure_filename(file.filename)
        #     api_file_path = os.path.join(upload_directory, filename)
        #     file.save(api_file_path)
        #     file_ext = filename.rsplit('.', 1)[1]
        #     pf = data.pop('platform').lower()
        # def_domestic, def_international = data.pop('domestic service'), data.pop('international service')
        # print(pf)
        # print(data)
        # sql_check = 'SELECT name FROM manifest WHERE name = %s'
        existing = ManifestModel.find_by_name(name=name)
        if existing:
            # print(existing)
            # print(f'Name {name} already taken.')
            return {'message': f'Name {name} already taken.'}, 400
        @jwt_required()
        def create_df(columns, dtype, headers):
            if pf != 'manual':
                f_ext = filename.rsplit('.', 1)[1]
                if f_ext == 'xlsx':
                    df = pd.read_excel(api_file_path, usecols=columns, dtype=dtype)
                elif f_ext == 'csv':
                    df = pd.read_csv(api_file_path, usecols=columns, dtype=dtype)
            else:
                print(name, columns, dtype)
                df = ManifestRaw.find_shipments_by_name(name, columns, dtype=str)
                df = df.astype(dtype)
                # print(df.head(2), df.dtypes)
            df = df[columns]
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
            # pd.set_option('display.max_columns', None)
            return df
        
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
                    df = create_df(columns, dtype, headers)
                elif 'oz' in weight_test or 'lb' in weight_test or 'lbs' in weight_test:
                    dtype[weight_name] = 'str'
                    df = create_df(columns, dtype, headers)
                    df['weight'] = df.apply(lambda row: ManifestModel.w_lbs_or_w_oz(row['weight']), axis=1)
            else:
                df = create_df(columns, dtype, headers)

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
                    df = create_df(columns, dtype, headers)
                elif 'oz' in weight_test or 'lb' in weight_test or 'lbs' in weight_test:
                    dtype[weight_name] = 'str'
                    df = create_df(columns, dtype, headers)
                    df['weight'] = df.apply(lambda row: ManifestModel.w_lbs_or_w_oz(row['weight']), axis=1)
            else:
                df = create_df(columns, dtype, headers)
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
            # pd.set_option('display.max_columns', None)
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
        # df['id'] = id
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
        # try:
        #     df.to_sql(con=conn, name='manifest_data', if_exists='append', index=False)
        # except:
        #     manifest_user.delete_from_db()
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


# class ManifestTest(Resource):
#     def get(self):
#         args = request.args
#         print(args.keys())
#         errors = manifest_schema.validate(request.args)
#         if errors:
#             return errors, 400
#         name = args['name']
#         existing = ManifestModel.find_by_name(name=name.replace('%20', ' '))
#         if not existing:
#             return {'message': f'Name {name} not found in system.'}, 400
#         task = ManifestTest.manifest_get.apply_async(args=(existing.id, ))
#         return {'message': 'Accepted'}, 202, {'Location': url_for('taskstatus', task_id=task.id)}
#
#     @ celery.task(bind=True)
#     def manifest_get(self, _id):
#         missing_columns = ManifestMissingModel.json(_id=_id)
#         shipments = []
#         self.update_state(state='PROGRESS',
#                           meta={'status': 'Finding shipments...'})
#         paginated_result = ManifestDataModel.find_all_shipments(_id)
#         query = ManifestDataModel.find_all_shipments_query(_id)
#         for shipment_item in paginated_result.items:
#             # '_sa_instance_state'
#             shipment = shipment_item.__dict__
#             del shipment['_sa_instance_state']
#             shipment['shipdate'] = str(shipment['shipdate'])
#             for missing_column in missing_columns:
#                 shipment[missing_column+'_gen'] = shipment.pop(missing_column)
#             shipments.append(shipment)
#         date_range = [shipments[0].get('shipdate') or shipments[0].get(
#             'shipdate_gen'), shipments[-1].get('shipdate') or shipments[-1].get('shipdate_gen')] if shipments else [None, None]
#         date_range = ManifestDataModel.find_date_range(_id)
#         services = []
#         self.update_state(state='PROGRESS',
#                           meta={'status': 'Converting services...'})
#         for service, weight_threshold, country, sugg_service in ManifestDataModel.find_distinct_services(_id):
#             service_parameters = {'service': service, 'weight_threshold': weight_threshold,
#                                   'country': country, 'sugg_service': sugg_service}
#             service_parameters['weight_threshold'] = ManifestDataModel.weight_threshold_display(service_parameters)
#             services.append(service_parameters)
#         self.update_state(state='PROGRESS',
#                           meta={'status': 'Finding shipping zones...'})
#         domestic_zones, international_zones = ManifestDataModel.find_distinct_zones(_id)
#         existing_headers_ordered = [v if v
#                                     not in missing_columns else v+'_gen' for v in ManifestModel.ai1s_headers_ordered]
#         self.update_state(state='PROGRESS',
#                           meta={'status': 'Generating report...'})
#         return {'status': 'Task completed!',
#                 'result': {'ordered headers': existing_headers_ordered, 'filtered shipments': shipments, 'service options': dom_intl, 'date range': date_range, 'suggested services': services, 'domestic zones': domestic_zones, 'international zones': international_zones, 'curr_page': paginated_result.page, 'has_prev': paginated_result.has_prev, 'has_next': paginated_result.has_next, 'pages': paginated_result.pages, 'total': paginated_result.total, 'Report': ManifestDataModel.shipment_report(filter_query=query)}}
#
#     def post(self):
#         data = request.form.to_dict()
#         print(data.keys())
#         if 'name' not in data and '_id' not in data:
#             print('No name or _id chosen')
#             return {'message': 'No name or _id chosen'}, 400
#         elif 'name' in data:
#             if 'start date' not in data:
#                 print('Start date not found')
#                 return {'message': 'Start date not found'}, 400
#             if 'end date' not in data:
#                 print('End date not found')
#                 return {'message': 'End date not found'}, 400
#             start_date, end_date = ManifestModel.vali_date(
#                 data.pop('start date')), ManifestModel.vali_date(data.pop('end date'))
#             if not start_date:
#                 print(f'Invalid start date {start_date}. Enter in YYYY-mm-dd format.')
#                 return {'message': f'Invalid start date {start_date}. Enter in YYYY-mm-dd format.'}, 400
#             if not end_date:
#                 print(f'Invalid end date {end_date}. Enter in YYYY-mm-dd format.')
#                 return {'message': f'Invalid end date {end_date}. Enter in YYYY-mm-dd format.'}, 400
#             if end_date < start_date:
#                 print(f'End date {end_date} can\'t be earlier than start date {start_date}')
#                 return {'message': f'End date {end_date} can\'t be earlier than start date {start_date}'}, 400
#             zone_weights = []
#             for i in list(range(1, 9))+list(range(11, 14)):
#                 if f'zone {i}' not in data:
#                     print(f'zone {i} not found')
#                     return {'message': f'zone {i} not found'}, 400
#                 zone_weight = {f'Zone {i}': data.pop(f'zone {i}')}
#                 if not zone_weight[f'Zone {i}'].isnumeric() or int(zone_weight[f'Zone {i}']) < 0:
#                     print(f'Invalid value entered for zone {i}. Enter a non-negative integer.')
#                     return {'message': f'Invalid value entered for zone {i}. Enter a non-negative integer.'}, 400
#                 zone_weight[f'Zone {i}'] = int(zone_weight[f'Zone {i}'])
#                 zone_weights.append(zone_weight)
#             if not zone_weights:
#                 print('Zone likeliness can\'t be left empty. Enter at least one positive integer for any zone.')
#                 return {'message': 'Zone likeliness can\'t be left empty. Enter at least one non-negative integer for any zone.'}, 400
#             max_weight = max(zone_weights, key=lambda x: tuple(x.items())[0][1])
#             if tuple(max_weight.values())[0] == 0:
#                 print('Zone likeliness can\'t all be 0. Enter at least one positive integer for any zone.')
#                 return {'message': 'Zone likeliness can\'t all be 0. Enter at least one positive integer for any zone.'}, 400
#         # if not data[f'zone + {str(i)}'].isnumeric() or int(data[f'zone + {str(i)}']) < 0:
#         #     return jsonify({'message': f'{data["zone "+ str(i)]} is an invalid value. Enter a non-negative integer.'}), 400
#         # default_parameters = data['default parameters']
#         # print(default_parameters)
#         # if 'shipdate' not in default_parameters:
#         #     return jsonify({'message': 'No shipdate range sent'}), 400
#         # if 'zone_weights' not in default_parameters:
#         #     return jsonify({'message': 'No zone weights sent'}), 400
#         # shipdate_range, zone_weights = default_parameters['shipdate'], default_parameters['zone_weights']
#             name = data.pop('name')
#             if len(name) > 45:
#                 return {'message': f'name is {len(name)} characters long. Please enter max 45 characters.'}, 400
#             upload_directory = ManifestModel.upload_directory
#             if 'platform' not in data:
#                 print('No platform selected')
#                 return {'message': 'No platform selected'}, 400
#             if 'manifest' not in request.files:
#                 print('No manifest file')
#                 return {'message': 'No manifest file'}, 400
#             file = request.files['manifest']
#             if file.filename == '':
#                 return {'message': 'No selected file'}, 400
#             if not ManifestModel.allowed_file(file.filename):
#                 return {'message': 'Unsupported file extension'}, 400
#             filename = secure_filename(file.filename)
#             api_file_path = os.path.join(upload_directory, filename)
#             file.save(api_file_path)
#             file_ext = filename.rsplit('.', 1)[1]
#             pf = data.pop('platform').lower()
#             def_domestic, def_international = data.pop('domestic service', -1), data.pop('international service', -1)
#             if def_domestic is -1 or def_international is -1:
#                 return {'message': 'Missing default services.'}, 400
#         else:
#             _id = data.pop('_id')
#             mongo_manifest = ManifestRaw.find_manifest_by_id(_id)
#             if not mongo_manifest:
#                 return {'message': f'_id {_id} not found.'}
#             pf = 'manual'
#             name = mongo_manifest.pop('name')
#             def_domestic, def_international = mongo_manifest.pop(
#                 'domestic service', -1), mongo_manifest.pop('international service', -1)
#             zone_weights = mongo_manifest.pop('zone weights')
#             start_date, end_date = mongo_manifest.pop('start date'), mongo_manifest.pop('end date')
#         # if 'platform' not in data:
#         #     if 'file name' not in data:
#         #         print('No platform selected')
#         #         return {'message': 'No platform selected'}, 400
#         #     pf = 'manual'
#         #     filename = filename = secure_filename(data.pop('file name'))
#         #     api_file_path = os.path.join(upload_directory, filename)
#         # else:
#         #     if 'manifest' not in request.files:
#         #         print('No manifest file')
#         #         return {'message': 'No manifest file'}, 400
#         #     file = request.files['manifest']
#         #     if file.filename == '':
#         #         return {'message': 'No selected file'}, 400
#         #     if not ManifestModel.allowed_file(file.filename):
#         #         return {'message': 'Unsupported file extension'}, 400
#         #     filename = secure_filename(file.filename)
#         #     api_file_path = os.path.join(upload_directory, filename)
#         #     file.save(api_file_path)
#         #     file_ext = filename.rsplit('.', 1)[1]
#         #     pf = data.pop('platform').lower()
#         # def_domestic, def_international = data.pop('domestic service'), data.pop('international service')
#         print(pf)
#         print(data)
#         # sql_check = 'SELECT name FROM manifest WHERE name = %s'
#         existing = ManifestModel.find_by_name(name=name)
#         if existing:
#             print(existing)
#             print(f'Name {name} already taken.')
#             return {'message': f'Name {name} already taken.'}, 400
#
#         def create_df(columns, dtype, headers):
#             if pf != 'manual':
#                 f_ext = filename.rsplit('.', 1)[1]
#                 if f_ext == 'xlsx':
#                     df = pd.read_excel(api_file_path, usecols=columns, dtype=dtype)
#                 elif f_ext == 'csv':
#                     df = pd.read_csv(api_file_path, usecols=columns, dtype=dtype)
#             else:
#                 print(name, columns, dtype)
#                 df = ManifestRaw.find_shipments_by_name(name, columns, dtype=str)
#                 df = df.astype(dtype)
#                 print(df.head(2), df.dtypes)
#             df = df[columns]
#             df = df.rename(columns=headers)
#             # df['orderno'] = df.index
#             df.dropna(how='all', inplace=True)
#             dup_df = df[df.duplicated(subset=['orderno'], keep=False)]
#             duplicate_index = 0
#             for i, row in dup_df.iterrows():
#                 df.loc[i, 'orderno'] += f'-{duplicate_index}'
#                 duplicate_index += 1
#             df.dropna(subset=['weight'], inplace=True)
#             if 'insured' in dtype and dtype['insured'] == 'float':
#                 df['insured'].fillna(value=0, inplace=True)
#                 df['insured'] = df['insured'].astype(bool)
#             # df.loc[~df['bill weight'].isna(), 'weight'] = df['bill weight']
#             if 'shipdate' in df.columns:
#                 shipdate_is_numeric = False
#                 for i, row in df.iterrows():
#                     if i == 5:
#                         break
#                     shipdate = row.shipdate
#                     if isinstance(shipdate, int):
#                         shipdate_is_numeric = True
#                         # df = df.astype({'shipdate': 'int'})
#                         # break
#                     elif isinstance(shipdate, float):
#                         df = df.astype({'shipdate': 'float'})
#                         df = df.astype({'shipdate': 'int'})
#                         shipdate_is_numeric = True
#                         break
#                 if shipdate_is_numeric:
#                     df['shipdate'] = df.apply(lambda row: datetime.fromordinal(
#                         datetime(1900, 1, 1).toordinal() + int(row['shipdate']) - 2), axis=1)
#                 else:
#                     df['shipdate'] = pd.to_datetime(df['shipdate'])
#             pd.set_option('display.max_columns', None)
#             return df
#
#         def generate_defaults(df):
#             generated_columns = {}
#             if 'country' not in df.columns:
#                 df['country'] = 'US'
#             if 'zip' not in df.columns:
#                 df['zip'] = 'N/A'
#                 zones_df = pd.DataFrame([tuple(zone_weight.keys())[0] for zone_weight in zone_weights])
#                 weights = [tuple(zone_weight.values())[0] for zone_weight in zone_weights]
#                 df['zone'] = zones_df.sample(len(df.index), weights=weights, replace=True).reset_index()[0]
#                 generated_columns['zip'], generated_columns['country'] = 'zip_gen', 'country_gen'
#             if 'shipdate' not in df.columns:
#                 # global start_date, end_date
#                 # start_date, end_date = pd.to_datetime(start_date), pd.to_datetime(end_date)
#                 df['shipdate'] = ManifestModel.random_dates(pd.to_datetime(
#                     start_date), pd.to_datetime(end_date), len(df.index)).sort_values()
#                 # df['shipdate'] = df.apply(lambda row: mflib.rand_date_str(row), axis=1)
#                 # df['shipdate'] = df['shipdate'].dt.strftime('%Y-%m-%d')
#                 generated_columns['shipdate'] = 'shipdate_gen'
#             if 'service' not in df.columns:
#                 df['service'] = where(df['country'] == 'US', def_domestic, def_international)
#                 generated_columns['service'] = 'service_gen'
#             return df, generated_columns
#
#         def manual(df, headers):
#             columns = list(headers.keys())
#             dtype = {column: ManifestModel.default_types[headers[column]]
#                      for column in columns if headers[column] in ManifestModel.default_types}
#             weight_name = None
#             concatenate_services = [None, None]
#             for k, v in headers.items():
#                 if v == 'weight':
#                     weight_name = k
#                 if v == 'service provider':
#                     concatenate_services[0] = k
#                 if v == 'service name':
#                     concatenate_services[1] = k
#             print(df.head(3), headers)
#             if weight_name:
#                 for i, row in df.iterrows():
#                     weight_test = str(row[weight_name])
#                     if weight_test.replace('.', '').isnumeric():
#                         dtype[weight_name] = 'float'
#                         df = create_df(columns, dtype, headers)
#                         break
#                     elif 'oz' in weight_test or 'lb' in weight_test or 'lbs' in weight_test:
#                         dtype[weight_name] = 'str'
#                         df = create_df(columns, dtype, headers)
#                         df['weight'] = df.apply(lambda row: ManifestModel.w_lbs_or_w_oz(row['weight']), axis=1)
#                         break
#             else:
#                 df = create_df(columns, dtype, headers)
#             if concatenate_services[1]:
#                 df['service'] = df['service'].combine(df['service 2nd column'], lambda x1, x2: f'{x1} {x2}')
#                 del df['service 2nd column']
#             if 'address' in df.columns:
#                 df[['zip', 'country']] = df.apply(lambda row: ManifestModel.add_to_zip_ctry(
#                     row.address), axis=1, result_type='expand')
#             empty_cols = ManifestModel.ai1s_headers.difference(
#                 set(df.columns)).difference({'shipdate', 'zip', 'country', 'service provider and name', 'service provider', 'service name', 'address'})
#             for col in empty_cols:
#                 df[col] = None
#             return df, empty_cols
#
#         def shopify(col_set, df):
#             columns = []
#             dtype = {}
#             headers = {}
#             not_found = []
#             weight_name = None
#             pot_columns = {'orderno': orderno, 'shipdate': shipdate, 'weight': weight,
#                            'service': sv, 'zip': address, 'country': address_country, 'price': current_price}
#             for pot_c in pot_columns.keys():
#                 for col in pot_columns[pot_c]['header']:
#                     found = False
#                     if col in col_set:
#                         if pot_c != 'weight':
#                             dtype[col] = pot_columns[pot_c]['format']
#                         else:
#                             weight_name = col
#                         headers[col] = pot_c
#                         found = True
#                         break
#                 if not found:
#                     not_found.append(pot_c)
#             columns = list(headers.keys())
#             if weight_name:
#                 print(weight_name)
#                 df.dropna(subset=[weight_name], inplace=True)
#                 for i, row in df.iterrows():
#                     weight_test = str(row[weight_name])
#                     if weight_test.replace('.', '').isnumeric():
#                         dtype[weight_name] = weight['format']
#                         df = create_df(columns, dtype, headers)
#                         break
#                     elif 'oz' in weight_test or 'lb' in weight_test or 'lbs' in weight_test:
#                         dtype[weight_name] = 'str'
#                         df = create_df(columns, dtype, headers)
#                         df['weight'] = df.apply(lambda row: ManifestModel.w_lbs_or_w_oz(row['weight']), axis=1)
#                         break
#             else:
#                 df = create_df(columns, dtype, headers)
#             # At this point we either have a service column with vendor and service code,
#             # or a service code column with an optional service provider column.
#             # If service provider is given, concatenate with service code.
#             empty_cols = ManifestModel.ai1s_headers.difference(
#                 set(df.columns)).difference({'shipdate', 'zip', 'country', 'service provider and name', 'service provider', 'service name', 'address'})
#             for col in empty_cols:
#                 df[col] = None
#             return df, empty_cols
#
#         def shipstation(col_set, df):
#             columns = []
#             dtype = {}
#             headers = {}
#             not_found = []
#             weight_name = None
#             pot_columns = {'orderno': orderno, 'shipdate': shipdate, 'weight': weight,
#                            'service': sv, 'zip': address, 'country': address_country, 'price': current_price,
#                            'insured': insured_parcel}
#             for pot_c in pot_columns.keys():
#                 for col in pot_columns[pot_c]['header']:
#                     found = False
#                     if col in col_set:
#                         if pot_c != 'weight':
#                             dtype[col] = pot_columns[pot_c]['format']
#                         else:
#                             weight_name = col
#                         headers[col] = pot_c
#                         found = True
#                         break
#                 if not found:
#                     not_found.append(pot_c)
#             optional_pot_columns = {'dim1': dim1, 'dim2': dim2, 'dim3': dim3}
#             for pot_c in optional_pot_columns.keys():
#                 for col in optional_pot_columns[pot_c]['header']:
#                     found = False
#                     if col in col_set:
#                         dtype[col] = optional_pot_columns[pot_c]['format']
#                         headers[col] = pot_c
#                         found = True
#                         break
#             columns = list(headers.keys())
#             sv_main, sv_alt0, sv_alt1 = False, False, False
#             if 'service' in headers.values():
#                 sv_main = True
#             else:
#                 if sv['header_alt'][0][0] in col_set:
#                     sv_alt0 = True
#                     columns.append(sv['header_alt'][0][0])
#                     headers[columns[-1]] = 'service_provider'
#                     dtype[headers[columns[-1]]] = ManifestModel.type_conv[sv['format']]
#                 elif sv['header_alt'][0][1] in col_set:
#                     sv_alt0 = True
#                     columns.append(sv['header_alt'][0][1])
#                     headers[columns[-1]] = 'service_provider'
#                     dtype[headers[columns[-1]]] = ManifestModel.type_conv[sv['format']]
#                 if sv['header_alt'][1][0] in col_set:
#                     sv_alt1 = True
#                     columns.append(sv['header_alt'][1][0])
#                     headers[columns[-1]] = 'service_code'
#                     dtype[headers[columns[-1]]] = ManifestModel.type_conv[sv['format']]
#                 elif sv['header_alt'][1][1] in col_set:
#                     sv_alt1 = True
#                     columns.append(sv['header_alt'][1][1])
#                     headers[columns[-1]] = 'service_code'
#                     dtype[headers[columns[-1]]] = ManifestModel.type_conv[sv['format']]
#                 # if not (sv_alt0 and sv_alt1):
#                 #     return  # If service name not included, we can't process the file. Service provider name is not essential
#             if weight_name:
#                 weight_test = str(df[weight_name].iloc[0])
#                 if weight_test.replace('.', '').isnumeric():
#                     dtype[weight_name] = weight['format']
#                     df = create_df(columns, dtype, headers)
#                 elif 'oz' in weight_test or 'lb' in weight_test or 'lbs' in weight_test:
#                     dtype[weight_name] = 'str'
#                     df = create_df(columns, dtype, headers)
#                     df['weight'] = df.apply(lambda row: ManifestModel.w_lbs_or_w_oz(row['weight']), axis=1)
#             else:
#                 df = create_df(columns, dtype, headers)
#             # At this point we either have a service column with vendor and service code,
#             # or a service code column with an optional service provider column.
#             # If service provider is given, concatenate with service code.
#             if sv_alt0 and sv_alt1:
#                 # df['service'] = df[['service_provider', 'service_code']].agg(' '.join, axis=1)
#                 df['service'] = df['service_provider'].combine(df['service_code'], lambda x1, x2: f'{x1} {x2}')
#                 del df['service_provider']
#                 del df['service_code']
#             empty_cols = ManifestModel.ai1s_headers.difference(
#                 set(df.columns)).difference({'shipdate', 'zip', 'country', 'service provider and name', 'service provider', 'service name', 'address'})
#             for col in empty_cols:
#                 df[col] = None
#             return df, empty_cols
#
#         def sellercloud_shipbridge(col_set, df):
#             columns = []
#             dtype = {}
#             headers = {}
#             not_found = []
#             weight_name = None
#             # [orderno['header'], shipdate['header'], weight['header'],
#             # sv['header'], address['header'], current_price['header']]
#             pot_columns = {'orderno': orderno, 'shipdate': shipdate, 'weight': weight,
#                            'service': sv, 'address': address, 'price': current_price}
#             for pot_c in pot_columns.keys():
#                 for col in pot_columns[pot_c]['header']:
#                     found = False
#                     if col in col_set:
#                         if pot_c != 'weight':
#                             dtype[col] = pot_columns[pot_c]['format']
#                         else:
#                             weight_name = col
#                         headers[col] = pot_c
#                         found = True
#                         break
#                 if not found:
#                     not_found.append(pot_c)
#             columns = list(headers.keys())
#             if weight_name:
#                 weight_test = str(df[weight_name].iloc[0])
#                 if weight_test.replace('.', '').isnumeric():
#                     dtype[weight_name] = weight['format']
#                     df = create_df(columns, dtype, headers)
#                 elif 'oz' in weight_test or 'lb' in weight_test or 'lbs' in weight_test:
#                     dtype[weight_name] = 'str'
#                     df = create_df(columns, dtype, headers)
#                     df['weight'] = df.apply(lambda row: ManifestModel.w_lbs_or_w_oz(row['weight']), axis=1)
#             else:
#                 df = create_df(columns, dtype, headers)
#             df[['zip', 'country']] = df.apply(lambda row: ManifestModel.add_to_zip_ctry(
#                 row.address), axis=1, result_type='expand')
#             # At this point we either have a service column with vendor and service code,
#             # or a service code column with an optional service provider column.
#             # If service provider is given, concatenate with service code.
#             # if sv_alt0:
#             #     df['service'] = df[['service_provider', 'service_code']].agg(' '.join, axis=1)
#             #     del df['service_provider']
#             #     del df['service_code']
#             empty_cols = ManifestModel.ai1s_headers.difference(
#                 set(df.columns)).difference({'shipdate', 'zip', 'country', 'service provider and name', 'service provider', 'service name', 'address'})
#             for col in empty_cols:
#                 df[col] = None
#             return df, empty_cols
#
#         def teapplix(col_set, df):
#             columns = []
#             dtype = {}
#             headers = {}
#             not_found = []
#             weight_name = None
#             pot_columns = {'orderno': orderno, 'shipdate': shipdate, 'weight': weight,
#                            'service': sv, 'zip': address, 'country': address_country, 'price': current_price, 'insured': insured_parcel}
#             for pot_c in pot_columns.keys():
#                 for col in pot_columns[pot_c]['header']:
#                     found = False
#                     if col in col_set:
#                         if pot_c != 'weight':
#                             dtype[col] = pot_columns[pot_c]['format']
#                         else:
#                             weight_name = col
#                         headers[col] = pot_c
#                         found = True
#                         break
#                 if not found:
#                     not_found.append(pot_c)
#             # optional_pot_columns = {'dim1': dim1, 'dim2': dim2, 'dim3': dim3}
#             # for pot_c in optional_pot_columns.keys():
#             #     for col in optional_pot_columns[pot_c]['header']:
#             #         found = False
#             #         if col in col_set:
#             #             dtype[col] = optional_pot_columns[pot_c]['format']
#             #             headers[col] = pot_c
#             #             found = True
#             #             break
#             columns = list(headers.keys())
#             sv_main, sv_alt0, sv_alt1 = False, False, False
#             if 'service' in headers.values():
#                 sv_main = True
#             else:
#                 if sv['header_alt'][0][0] in col_set:
#                     sv_alt0 = True
#                     columns.append(sv['header_alt'][0][0])
#                     headers[columns[-1]] = 'service_provider'
#                     dtype[headers[columns[-1]]] = ManifestModel.type_conv[sv['format']]
#                 elif sv['header_alt'][0][1] in col_set:
#                     sv_alt0 = True
#                     columns.append(sv['header_alt'][0][1])
#                     headers[columns[-1]] = 'service_provider'
#                     dtype[headers[columns[-1]]] = ManifestModel.type_conv[sv['format']]
#                 if sv['header_alt'][1][0] in col_set:
#                     sv_alt1 = True
#                     columns.append(sv['header_alt'][1][0])
#                     headers[columns[-1]] = 'service_code'
#                     dtype[headers[columns[-1]]] = ManifestModel.type_conv[sv['format']]
#                 elif sv['header_alt'][1][1] in col_set:
#                     sv_alt1 = True
#                     columns.append(sv['header_alt'][1][1])
#                     headers[columns[-1]] = 'service_code'
#                     dtype[headers[columns[-1]]] = ManifestModel.type_conv[sv['format']]
#                     # if not (sv_alt0 and sv_alt1):
#                     #     return  # If service name not included, we can't process the file. Service provider name is not essential
#             if weight_name:
#                 df.dropna(subset=[weight_name], inplace=True)
#                 for i, row in df.iterrows():
#                     weight_test = str(row[weight_name])
#                     if weight_test.replace('.', '').isnumeric():
#                         dtype[weight_name] = weight['format']
#                         df = create_df(columns, dtype, headers)
#                         break
#                     elif 'oz' in weight_test or 'lb' in weight_test or 'lbs' in weight_test:
#                         dtype[weight_name] = 'str'
#                         df = create_df(columns, dtype, headers)
#                         df['weight'] = df.apply(lambda row: ManifestModel.w_lbs_or_w_oz(row['weight']), axis=1)
#                         break
#             else:
#                 df = create_df(columns, dtype, headers)
#             if sv_alt0 and sv_alt1:
#
#                 # df[['service_code', 'service_provider']].replace(np.nan, '', regex=True, inplace=True)
#                 # df['service'] = df[['service_provider', 'service_code']].agg(' '.join, axis=1)
#                 df['service'] = df['service_provider'].combine(df['service_code'], lambda x1, x2: f'{x1} {x2}')
#                 del df['service_provider']
#                 del df['service_code']
#             # At this point we either have a service column with vendor and service code,
#             # or a service code column with an optional service provider column.
#             # If service provider is given, concatenate with service code.
#             empty_cols = ManifestModel.ai1s_headers.difference(
#                 set(df.columns)).difference({'shipdate', 'zip', 'country', 'service provider and name', 'service provider', 'service name', 'address'})
#             for col in empty_cols:
#                 df[col] = None
#             return df, empty_cols
#
#         if pf != 'manual':
#             f_ext = filename.rsplit('.', 1)[1]
#             if f_ext == 'xlsx':
#                 df = pd.read_excel(api_file_path, nrows=5)
#             elif f_ext == 'csv':
#                 df = pd.read_csv(api_file_path, nrows=5)
#             pd.set_option('display.max_columns', None)
#             orderno = ManifestFormatModel.format['order_no'][pf]  # not required
#             shipdate = ManifestFormatModel.format['date'][pf]
#             weight = ManifestFormatModel.format['weight'][pf]
#             sv = ManifestFormatModel.format['service'][pf]  # not required
#             address = ManifestFormatModel.format['address'][pf]
#             address_country = ManifestFormatModel.format['address_country'].get(pf)
#             current_price = ManifestFormatModel.format['current_price'].get(pf)  # optional
#             insured_parcel = ManifestFormatModel.format['insured_parcel'].get(pf)  # optional
#             dim1 = ManifestFormatModel.format['dim1'].get(pf)  # optional
#             dim2 = ManifestFormatModel.format['dim2'].get(pf)  # optional
#             dim3 = ManifestFormatModel.format['dim3'].get(pf)  # optional
#             col_set = set()
#             for i in df.columns:
#                 col_set.add(i)
#             df, empty_cols = locals()[pf](col_set, df)
#         else:
#             df = ManifestRaw.find_shipments_by_name(name, list(data.keys()), limit=5)
#             df, empty_cols = locals()[pf](df, data)
#
#         df.reset_index(inplace=True, drop=True)
#         df, generated_columns = generate_defaults(df)
#         df[['country', 'zone', 'weight_threshold', 'sugg_service', 'bill_weight', 'dhl_tier_1_2021',
#             'dhl_tier_2_2021', 'dhl_tier_3_2021', 'dhl_tier_4_2021',
#             'dhl_tier_5_2021', 'dhl_cost_2021', 'usps_2021', 'dhl_cost_shipdate', 'usps_shipdate']] = df.apply(ManifestDataModel.row_to_rate, axis=1, result_type='expand')
#         df.weight = df.apply(lambda row: ceil(row.weight) if row.weight < 16 else ceil(row.weight/16)*16, axis=1)
#         df.loc[~df['bill_weight'].isna(), 'weight'] = df['bill_weight']
#         del df['bill_weight']
#         df['shipdate'] = df['shipdate'].dt.strftime('%Y-%m-%d')
#         subset = ['service', 'weight_threshold', 'country']
#         df.loc[df['zone'].str.len() != 7, 'country'] = 'Intl'
#         df = df.replace({nan: None})
#         df.sort_values(by=['shipdate', 'orderno'], ascending=[True, True], inplace=True)
#         df_unique_services = df[['service', 'weight_threshold', 'country',
#                                  'sugg_service']].drop_duplicates(subset=subset, inplace=False)
#         df_unique_services['weight_threshold'] = df.apply(ManifestDataModel.weight_threshold_display, axis=1)
#         df_unique_services.sort_values(by=['country', 'weight_threshold', 'service'],
#                                        ascending=[False, True, True], inplace=True)
#         all_zones = df['zone'].drop_duplicates(inplace=False).sort_values(inplace=False)
#         domestic_zones = all_zones[all_zones.str.startswith('Zone ')].tolist()
#         international_zones = all_zones[~all_zones.str.startswith('Zone ')].tolist()
#         date_range = [df['shipdate'].min(), df['shipdate'].max()]
#         pd.set_option('display.max_columns', None)
#         df = df.rename(generated_columns, axis=1)
#         print(df.head(5))
#         df.columns = df.columns.str.replace(' ', '_')
#         df.columns = df.columns.str.replace('.', '')
#         weight_thres_column = df['weight_threshold']
#         del df['weight_threshold']
#         existing_headers_ordered = ManifestModel.ai1s_headers_ordered
#         for i, v in enumerate(existing_headers_ordered):
#             if v in generated_columns:
#                 existing_headers_ordered[i] = generated_columns[v]
#         df_size = len(df.index)
#         response = {'ordered headers': existing_headers_ordered, 'filtered shipments': df.head(20).to_dict(orient='records'),
#                     'service options': dom_intl,
#                     'suggested services': df_unique_services.to_dict(orient='records'),
#                     'date range': date_range, 'domestic zones': domestic_zones,
#                     'international zones': international_zones, 'curr_page': 1, 'has_prev': False,
#                     'has_next': True if df_size > 20 else False, 'pages': df_size//20+1,
#                     'total': df_size}
#         df['weight_threshold'] = weight_thres_column
#         generated_columns_rev = {v: k for k, v in generated_columns.items()}
#         df = df.rename(generated_columns_rev, axis=1)
#         manifest_user = ManifestModel(name=name)
#         manifest_user.save_to_db()
#         id = ManifestModel.find_by_name(name=name).id
#         for generated_column in generated_columns.keys():
#             missing_field = ManifestMissingModel(id, generated_column)
#             missing_field.save_to_db()
#         if 'price' not in df.columns:
#             missing_field = ManifestMissingModel(id, 'price')
#             missing_field.save_to_db()
#         # df['id'] = id
#         for i, row in df.iterrows():
#             shipment = ManifestDataModel(id=id, orderno=row.orderno, shipdate=row.shipdate,
#                                          weight=row.weight, service=row.service, zip=row.zip, country=row.country,
#                                          insured=row.insured, dim1=row.dim1, dim2=row.dim2, dim3=row.dim3, price=row.price,
#                                          zone=row.zone, weight_threshold=row.weight_threshold, sugg_service=row.sugg_service,
#                                          dhl_tier_1_2021=row['dhl_tier_1_2021'], dhl_tier_2_2021=row['dhl_tier_2_2021'], dhl_tier_3_2021=row['dhl_tier_3_2021'],
#                                          dhl_tier_4_2021=row['dhl_tier_4_2021'], dhl_tier_5_2021=row[
#                                              'dhl_tier_5_2021'], dhl_cost_2021=row['dhl_cost_2021'], usps_2021=row['usps_2021'],
#                                          dhl_cost_shipdate=row['dhl_cost_shipdate'], usps_shipdate=row['usps_shipdate'])
#             shipment.save_to_db()
#         ManifestDataModel.commit_to_db()
#         df.drop(['orderno', 'zip', 'sugg_service'], inplace=True, axis=1)
#         response.update({'Report': ManifestDataModel.shipment_report(df=df)})
#         # try:
#         #     df.to_sql(con=conn, name='manifest_data', if_exists='append', index=False)
#         # except:
#         #     manifest_user.delete_from_db()
#         if pf == 'manual':
#             ManifestRaw.delete_from_db(_id)
#         return response


class ManifestFilter(Resource):
    @jwt_required()
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
        # filters = request_data['filters']
        print(filters)
        missing_columns = ManifestMissingModel.json(_id=id)
        service_replacements = {}
        existing_headers_ordered = [v if v
                                    not in missing_columns else v+'_gen' for v in ManifestModel.ai1s_headers_ordered]
        for service_override in filters.get('services', []):
            if service_override.get('service') is not None:
                service_replacements[(service_override['service name'], service_override['location'],
                                      '>=' if service_override['weight threshold'][:5] == 'Over ' else '<')] = service_override['service']
        shipments = []
        page = request_data.get('page', 1)
        per_page = request_data.get('per_page', 20)
        include_loss = request_data.get('include_loss', True)
        filter_queries = ManifestDataModel.filtered_query_builder(id, filters)
        send_report = request_data.get('send_report', True)
        print('\n\nhere\n\n')
        print(filter_queries)
        response = {}
        if send_report:
            response['Report'] = ManifestDataModel.shipment_report(
                filter_query=filter_queries[1], service_replacements=service_replacements, include_loss=include_loss)
        if per_page != 0:
            paginated_result = ManifestDataModel.find_filtered_shipments(filter_queries[0], page, per_page)
            for shipment_item in paginated_result.items:
                if (shipment_item.service, shipment_item.country, shipment_item.weight_threshold) in service_replacements:
                    print('here', shipment_item.service, shipment_item.country, shipment_item.weight_threshold)
                    shipment_item = shipment_item.correct_service_rates(service_replacements[(
                        shipment_item.service, shipment_item.country, shipment_item.weight_threshold)])
                shipment = shipment_item.__dict__
                print(shipment)
                del shipment['_sa_instance_state']
                shipment['shipdate'] = str(shipment['shipdate'])
                for missing_column in missing_columns:
                    shipment[missing_column+'_gen'] = shipment.pop(missing_column)
                shipments.append(shipment)
            response.update({'ordered headers': existing_headers_ordered,
                             'filtered shipments': shipments, 'curr_page': paginated_result.page,
                             'has_prev': paginated_result.has_prev, 'has_next': paginated_result.has_next,
                             'pages': paginated_result.pages, 'total': paginated_result.total})
        return response
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

class ManifestFormat(Resource):
    @jwt_required()
    def get(self):
        args = request.args
        platform = args.get('platform')
        if not platform:
            return {'message': 'platform field missing.'}, 400
        platformat = ManifestFormatModel.find_platformat(platform)
        if not platformat:
            return {'message': f'"{platform}" not found.'}, 400
        description = "A new column name field can be added at the end of an existing list for the app to recognize in future manifests. All fields are case sensitive."
        service_msg = "Before updating a service field, check first if there is a 'header_alt' header. If the field contains both service prodiver (e.g. 'Fedex') and service name (e.g. 'Ground') in the same column, add it to the regular 'header'. 'header_alt' is used if service provider and service name are not found in one column. In that case, they might be found in separate service provider and service name columns. For service provider only, enter field into the first header_alt list, and for service name only, enter field into the second header_alt list."
        address_msg = "Before updating an address field, check first if there are separate 'address' and 'address_country' fields. In that case, the platform normally only includes separate zip code and country columns, and not the full address. For zip code columns, enter field into the address header, and for country name or country code, enter field into the address_country header."
        return {'format': platformat, 'description': description, 'service_message': service_msg, 'address_message': address_msg}

    def post(self):
        args = request.form
        errors = manifest_format_schema.validate(args)
        if errors:
            return errors, 400
        # return 'ok'
        (platform, field, header, value, index) = (args.get(_)
                                                   for _ in ('platform', 'field', 'header', 'value', 'index'))
        if field not in ManifestFormatModel.format:
            return {'message': f"'{field}' field not found in format."}, 400
        if platform not in ManifestFormatModel.format[field]:
            return {'message': f"'{platform}' platform not found in format."}, 400
        if header not in ManifestFormatModel.format[field][platform]:
            return {'message': f"'{header}' header not found for '{platform}' platform."}, 400
        if len(value) > 45:
            return {'message': 'value length cannot exceed 45.'}, 400
        if len(value) < 2:
            return {'message': 'value length must exceed 1'}, 400
        starting_letter_or_num, contains_letter = False, False
        if value[0].isalpha():
            starting_letter_or_num, contains_letter = True, True
        elif value[0].isnumeric():
            starting_letter_or_num = True
            for char in value[1:]:
                if char.isalpha():
                    contains_letter = True
                    break
            if not contains_letter:
                return {'message': 'value must contain at least one letter.'}, 400
        if not starting_letter_or_num or not contains_letter:
            return {'message': 'value has to begin with an alphanumeric character.'}, 400
        if header == 'header_alt':
            if index is None:
                return {'message': "index is required for 'header_alt' header."}, 400
            index = int(index)
            if index >= len(ManifestFormatModel.format[field][platform][header]) or index < 0:
                return {'message': "Array index out of range."}, 400
            if value in ManifestFormatModel.format[field][platform][header][index]:
                return {'message': f"'{value}' value already exists for header '{header}' within {platform}.{field}.{index}"}, 400
        elif value in ManifestFormatModel.format[field][platform][header]:
            return {'message': f"'{value}' value already exists for header '{header}' within {platform}.{field}."}, 400
        ManifestFormatModel.add_format_fields(platform, field, header, value, index)
        if header == 'header_alt':
            return {'message': f"'{value}' value successfully added to header '{header}' within {platform}.{field}.{index}"}
        else:
            return {'message': f"'{value}' value successfully added to header '{header}' within {platform}.{field}."}


class ManifestServiceUpdate(Resource):
    @jwt_required()
    def post(self):
        errors = manifest_service_update_schema.validate(request.form)
        if errors:
            return errors, 400
        if ManifestModel.update_services('post', **request.form) is 0:
            return {'message': 'Service parameters already exist.'}, 400
        return {'message': 'Service successfully inserted.'}

    @jwt_required()
    def put(self):
        errors = manifest_service_update_schema.validate(request.form)
        if errors:
            return errors, 400
        if ManifestModel.update_services('put', **request.form) is 0:
            return {'message': 'An error occured.'}, 400
        return {'message': 'Service successfully updated.'}


class ManifestAuthTest(Resource):
    @ jwt_required()
    def get(self):
        return 'ok'


# @ celery.task(bind=True)
# def get_task(self):
#     self.update_state(state='PROGRESS')
#     return {'headers': sorted(list(ManifestModel.ai1s_headers))}
