from flask_restful import Resource
from models.manifest import ManifestModel, ManifestRaw
from flask import request
from werkzeug.utils import secure_filename
import os
import pandas as pd
from numpy import nan
from flask_jwt_extended import jwt_required

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