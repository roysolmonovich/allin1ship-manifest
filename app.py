from flask_cors import CORS, cross_origin
import pandas as pd
from lib import service
# uncomment sqlalchemy
# from sqlalchemy import create_engine, select, insert, MetaData, Table, and_
# import mysql.connector
# import pickle
from resources.carrieritem import CarrierItem
from resources.user import User
from resources.manifest import Manifest, ManifestFilter, ManifestNames, ManifestColumns, ManifestManual, ManifestAuthTest
# from models.manifest import ManifestModel
from flask import Flask, jsonify
from flask_jwt_extended import JWTManager
from flask_restful import Api
import os
# from werkzeug.utils import secure_filename
# from flask_wtf import FlaskForm
# import mysql.connector
# service_options = [v[3] for v in service.values()]
ai1s_headers = {'orderno', 'shipdate', 'weight', 'service', 'zip', 'country', 'price',
                'insured', 'dim1', 'dim2', 'dim3'}

ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'gz'}

supported_platforms = {'sellercloud_shipbridge', 'shipstation', 'shopify'}


def allowed_file(filename):
    if '.' not in filename:
        return False
    f_ext = filename.rsplit('.', 1)[1].lower()
    if f_ext not in ALLOWED_EXTENSIONS:
        return False
    if f_ext == 'gz':
        f_ext = filename.rsplit('.', 2)[1].lower()
        if f_ext not in ALLOWED_EXTENSIONS:
            return False
    return True


# with open(r'dependencies/charges_by_zone/carrier_charges111.pkl', 'rb') as f:
#     map = pickle.load(f)
# format_hash = mflib.ManifestFormat.format_hash
type_conv = {'str': str, 'float': float, 'int': pd.Int64Dtype(), 'bool': bool}

# mycursor = mydb.cursor()
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL')
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {'pool_size': 100, 'pool_recycle': 280, 'pool_pre_ping': True}
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['MAX_CONTENT_LENGTH'] = 20 * 1024 * 1024
app.config['PROPAGATE_EXCEPTIONS'] = True
# app.config['UPLOAD_FOLDER'] = Manifest.upload_directory
app.config['UPLOAD_FOLDER'] = 'api_uploads'
app.secret_key = 'roy'
api = Api(app)
jwt = JWTManager(app)


@jwt.expired_token_loader
def expired_token_callback():
    return jsonify({'description': 'The token has expired.',
                    'error': 'token_expired'}), 401


@jwt.invalid_token_loader
def invalid_token_callback(error):
    return jsonify({'description': 'Signature verification failed.',
                    'error': 'invalid_token'}), 401


@jwt.unauthorized_loader
def missing_token_callback(error):
    return jsonify({'description': 'Request does not contain an access token.',
                    'error': 'authorization_required'}), 401


@jwt.needs_fresh_token_loader
def token_not_fresh_callback():
    return jsonify({'description': 'The token is not fresh.',
                    'error': 'fresh_token_required'}), 401


@jwt.revoked_token_loader
def revoked_token_callback():
    return jsonify({'description': 'The token has been revoked.',
                    'error': 'token_revoked'}), 401


# cors = CORS(app) #REDO
cors = CORS(app, resources={r"/*": {"origins": "*"}})
# CORS(app, support_credentials=True)


# app.config['CORS_HEADERS'] = 'Content-Type'


@app.route('/')
def index():
    return "<h1>Welcome to our server !!</h1>"

# class CarrierItem(Resource):
#     def get(self):
#         request_data = request.get_json()
#         if 'carrier' not in request_data:
#             return {'message': 'Carrier object is missing.'}, 400
#         carrier = request_data['carrier']
#         if carrier not in CarrierCharge.map.keys():
#             return {'message': f'Carrier {carrier} not found in our system.'}, 400
#         return {'location': list(CarrierCharge.map[carrier].keys())}


# @app.route('/mapadd/', methods=['POST'])
# def create_carrier_or_tier():
#     request_data = request.get_json()
#     if 'carrier' in request_data:
#         c = request_data['carrier']
#         map[c] = {}
#     if 'location' in request_data:
#         loc = request_data['location']
#         map[c][loc] = {}
#     if 'date' in request_data:
#         # d = datetime.strptime(request_data['date'], '%Y-%m-%d').date()
#         d = request_data['date']
#         map[c][loc][d] = {}
#     if 'service' in request_data:
#         sv = request_data['service']
#         map[c][loc][d][sv] = {}
#     if 'zone' in request_data:
#         z = request_data['zone']
#         map[c][loc][d][sv][z] = {}
#     if 'weight' in request_data:
#         w = request_data['weight']
#         map[c][loc][d][sv][z][w] = None
#     if 'charge' in request_data:
#         ch = request_data['charge']
#         map[c][loc][d][sv][z][w] = ch
#     print(request_data)
#     with open(r'dependencies/charges_by_zone/carrier_charges.pkl', 'wb') as f:
#         pickle.dump(map, f, pickle.HIGHEST_PROTOCOL)
#     return jsonify(request_data)


# @app.route('/map/')
# @jwt_required()
# def get_carriers():
#     return jsonify({'carriers': list(map.keys())})
#
#
# @app.route('/map/<string:carrier>/')
# def get_locations(carrier):
#     if carrier.isnumeric():
#         carrier = int(carrier)
#     return jsonify({'locations': map[carrier]})
#
#
# @app.route('/map/<string:carrier>/<string:location>/')
# def get_dates(carrier, location):
#     if carrier.isnumeric():
#         carrier = int(carrier)
#     return jsonify({'dates': list(map[carrier][location].keys())})
#
#
# @app.route('/map/<string:carrier>/<string:location>/<string:date>/')
# def get_serivces(carrier, location, date):
#     request_data = request.get_json()
#     print(request_data)
#     date = datetime.strptime(date, '%Y-%m-%d').date()
#     if carrier.isnumeric():
#         carrier = int(carrier)
#     return jsonify({'services': list(map[carrier][location][date].keys())})
#
#
# @app.route('/map/<string:carrier>/<string:location>/<string:date>/<int:service>/')
# def get_zones(carrier, location, date, service):
#     date = datetime.strptime(date, '%Y-%m-%d').date()
#     if carrier.isnumeric():
#         carrier = int(carrier)
#     return jsonify({'zones': list(map[carrier][location][date][service].keys())})
#
#
# @app.route('/map/<string:carrier>/<string:location>/<string:date>/<int:service>/<string:zone>/')
# def get_weights(carrier, location, date, service, zone):
#     date = datetime.strptime(date, '%Y-%m-%d').date()
#     if carrier.isnumeric():
#         carrier = int(carrier)
#     return jsonify({'weights': list(map[carrier][location][date][service][zone].keys())})
#
#
# @app.route('/map/<string:carrier>/<string:location>/<string:date>/<int:service>/<string:zone>/<float:weight>/')
# def get_charge(carrier, location, date, service, zone, weight):
#     # return jsonify({'stores': stores})
#     weight = float(weight)
#     date = datetime.strptime(date, '%Y-%m-%d').date()
#     if carrier.isnumeric():
#         carrier = int(carrier)
#     return jsonify({'charge': CarrierCharge.charge_rate(carrier, location, date, service, zone, weight)})
#     # return jsonify({'charge': map[carrier][location][date][service][zone][weight]})
#
#
# @app.route('/manifest_filters/', methods=['GET'])
# @cross_origin()
# def manifest_filters():
#     request_data = request.get_json()
#     print(request_data)
#     return jsonify(request_data)


# @app.route('/manifest_test/', methods=['POST'])
# @cross_origin()  # REMOVE
# def manifest_test():
#     current, peak = tracemalloc.get_traced_memory()
#     data = request.form
#     print(data)
#     if 'platform' not in data:
#         print('No platform selected')
#         return jsonify({'message': 'No platform selected'}), 400
#     pf = data['platform'].lower()
#     if pf not in supported_platforms:
#         return jsonify({'message': f'Platform {pf} not supported'}), 400
#     if 'manifest' not in request.files:
#         print('No manifest file')
#         return jsonify({'message': 'No manifest file'}), 400
#     if 'name' not in data:
#         print('No name chosen')
#         return jsonify({'message': 'No name chosen'}), 400
#     if 'start date' not in data:
#         print('Start date not found')
#         return jsonify({'message': 'Start date not found'}), 400
#     if 'end date' not in data:
#         print('End date not found')
#         return jsonify({'message': 'End date not found'}), 400
#     start_date, end_date = vali_date(data['start date']), vali_date(data['end date'])
#     if not start_date:
#         print(f'Invalid start date {start_date}. Enter in YYYY-mm-dd format.')
#         return jsonify({'message': f'Invalid start date {start_date}. Enter in YYYY-mm-dd format.'}), 400
#     if not end_date:
#         print(f'Invalid end date {end_date}. Enter in YYYY-mm-dd format.')
#         return jsonify({'message': f'Invalid end date {end_date}. Enter in YYYY-mm-dd format.'}), 400
#     if end_date < start_date:
#         print(f'End date {end_date} can\'t be earlier than start date {start_date}')
#         return jsonify({'message': f'End date {end_date} can\'t be earlier than start date {start_date}'}), 400
#     zone_weights = []
#     for i in list(range(1, 9))+list(range(11, 14)):
#         if f'zone {i}' not in data:
#             print(f'zone {i} not found')
#             return jsonify({'message': f'zone {i} not found'}), 400
#         zone_weight = {f'Zone {i}': data[f'zone {i}']}
#         if not zone_weight[f'Zone {i}'].isnumeric() or int(zone_weight[f'Zone {i}']) < 0:
#             print(f'Invalid value entered for zone {i}. Enter a non-negative integer.')
#             return jsonify({'message': f'Invalid value entered for zone {i}. Enter a non-negative integer.'}), 400
#         zone_weight[f'Zone {i}'] = int(zone_weight[f'Zone {i}'])
#         zone_weights.append(zone_weight)
#     print(zone_weights)
#     # if not data[f'zone + {str(i)}'].isnumeric() or int(data[f'zone + {str(i)}']) < 0:
#     #     return jsonify({'message': f'{data["zone "+ str(i)]} is an invalid value. Enter a non-negative integer.'}), 400
#     # default_parameters = data['default parameters']
#     # print(default_parameters)
#     # if 'shipdate' not in default_parameters:
#     #     return jsonify({'message': 'No shipdate range sent'}), 400
#     # if 'zone_weights' not in default_parameters:
#     #     return jsonify({'message': 'No zone weights sent'}), 400
#     # shipdate_range, zone_weights = default_parameters['shipdate'], default_parameters['zone_weights']
#     name = data['name']
#
#     print(name)
#     if len(name) >= 45:
#         return jsonify({'message': f'name is {len(name)} characters long. Please enter max 45 characters.'}), 400
#     file = request.files['manifest']
#     if file.filename == '':
#         return jsonify({'message': 'No selected file'}), 400
#     if not allowed_file(file.filename):
#         return jsonify({'message': 'Unsupported file extension'}), 400
#     filename = secure_filename(file.filename)
#     api_file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
#     file.save(api_file_path)
#     file_ext = filename.rsplit('.', 1)[1]
#     if file_ext == 'gz':
#         file_ext = filename.rsplit('.', 2)[1]
#         if file_ext == 'xlsx':
#             new_api_file_path = api_file_path.rsplit('.', 1)[0]
#             with gzip.open(api_file_path, 'rb') as f_in, open(new_api_file_path, 'wb') as f_out:
#                 f_out.writelines(f_in)
#             os.remove(api_file_path)
#             api_file_path = new_api_file_path
#     print(pf)
#     print(filename)
#
#     sql_check = 'SELECT name FROM manifest WHERE name = %s'
#
#     existing = ManifestModel.find_by_name(name=name)
#     if existing:
#         print(existing)
#         print(f'Name {name} already taken.')
#         return {'message': f'Name {name} already taken.'}, 400
#     if len(name) > 45:
#         return {'message': f'Invalid name - cannot exceed 45 characters.'}, 400
#
#     # table = Table(
#     #     'manifest',
#     #     metadata,
#     #     autoload=True,
#     #     autoload_with=engine
#     # )
#     # stmt = select([table.columns.id]).where(table.columns.name == name)
#     # res = conn.execute(stmt).fetchone()
#     # if res:
#     #     return f'Name "{name}" already exists. Please choose a different name.', 400
#     # if 'platform' not in request_data:
#     #     return jsonify({'message': 'platform object missing.'}), 400
#     # shipments = request_data['shipments']
#     # pf = request_data['platform'].lower()
#     f_ext = filename.rsplit('.', 1)[1]
#     if f_ext == 'gz':
#         f_ext = filename.rsplit('.', 2)[1]
#     if f_ext == 'xlsx':
#         df = pd.read_excel(api_file_path, nrows=5)
#     elif f_ext == 'csv':
#         df = pd.read_csv(api_file_path, nrows=5)
#     pd.set_option('display.max_columns', None)
#     print(df.head(5))
#     orderno = format_hash['order_no'][pf]  # not required
#     shipdate = format_hash['date'][pf]
#     weight = format_hash['weight'][pf]
#     sv = format_hash['service'][pf]  # not required
#     address = format_hash['address'][pf]
#     address1 = format_hash['address1'].get(pf)
#     current_price = format_hash['current_price'].get(pf)  # optional
#     insured_parcel = format_hash['insured_parcel'].get(pf)  # optional
#     dim1 = format_hash['dim1'].get(pf)  # optional
#     dim2 = format_hash['dim2'].get(pf)  # optional
#     dim3 = format_hash['dim3'].get(pf)  # optional
#
#     def create_df(columns, dtype, headers):
#         # def create_df(path, columns, dtype, headers):
#         f_ext = filename.rsplit('.', 1)[1]
#         if f_ext == 'gz':
#             f_ext = filename.rsplit('.', 2)[1]
#         print(columns)
#         print(dtype)
#         print(headers)
#         if f_ext == 'xlsx':
#             df = pd.read_excel(api_file_path, usecols=columns, dtype=dtype)
#         elif f_ext == 'csv':
#             df = pd.read_csv(api_file_path, usecols=columns, dtype=dtype)
#         # parse_dates=[columns[1]], date_parser=dateparse
#         print(columns)
#         df = df[columns]
#         print(columns)
#         df = df.rename(columns=headers)
#         df.dropna(how='all', inplace=True)
#         df.dropna(subset=['weight'], inplace=True)
#         if 'insured' in dtype and dtype['insured'] == 'float':
#             df['insured'].fillna(value=0, inplace=True)
#             df['insured'] = df['insured'].astype(bool)
#         # df.loc[~df['bill weight'].isna(), 'weight'] = df['bill weight']
#         if 'shipdate' in df.columns:
#             shipdate_is_numeric = False
#             for i, row in df.iterrows():
#                 if i == 5:
#                     break
#                 shipdate = row.shipdate
#                 if isinstance(shipdate, int):
#                     shipdate_is_numeric = True
#                     # df = df.astype({'shipdate': 'int'})
#                     # break
#                 elif isinstance(shipdate, float):
#                     df = df.astype({'shipdate': 'float'})
#                     df = df.astype({'shipdate': 'int'})
#                     shipdate_is_numeric = True
#                     break
#             if shipdate_is_numeric:
#                 df['shipdate'] = df.apply(lambda row: datetime.fromordinal(
#                     datetime(1900, 1, 1).toordinal() + int(row['shipdate']) - 2), axis=1)
#             else:
#                 df['shipdate'] = pd.to_datetime(df['shipdate'])
#         pd.set_option('display.max_columns', None)
#         return df
#
#     def generate_defaults(df):
#         generated_columns = {}
#         print(df.columns)
#         # Fix country missing issue...
#         if 'zip' not in df.columns and 'country' not in df.columns:
#             df['country'] = 'US'
#             df['zip'] = 'N/A'
#             zones_df = pd.DataFrame([tuple(zone_weight.keys())[0] for zone_weight in zone_weights])
#             weights = [tuple(zone_weight.values())[0] for zone_weight in zone_weights]
#             # df1 = pd.DataFrame([None]*100, columns=['zones'])
#             # print(zones_df.sample(100, weights=zone_weights, replace=True, axis=1))
#             # x = zones_df.sample(100, weights=zone_weights, replace=True).reset_index()[0]
#             # print(x.head(20))
#             # print(x.columns)
#             # weights = x
#             # print(list(weights))
#             print(df.head(8))
#             df['zone'] = zones_df.sample(len(df.index), weights=weights, replace=True).reset_index()[0]
#             print(df.head(8))
#             generated_columns['zip'], generated_columns['country'] = 'zip (gen.)', 'country (gen.)'
#         if 'shipdate' not in df.columns:
#             # global start_date, end_date
#             # start_date, end_date = pd.to_datetime(start_date), pd.to_datetime(end_date)
#             df['shipdate'] = mflib.random_dates(pd.to_datetime(
#                 start_date), pd.to_datetime(end_date), len(df.index)).sort_values()
#             # df['shipdate'] = df.apply(lambda row: mflib.rand_date_str(row), axis=1)
#             # df['shipdate'] = df['shipdate'].dt.strftime('%Y-%m-%d')
#             generated_columns['shipdate'] = 'shipdate (gen.)'
#         # else:
#         #     print(df['country'])
#         return df, generated_columns
#
#     def shopify(col_set, df):
#         columns = []
#         dtype = {}
#         headers = {}
#         not_found = []
#         weight_name = None
#         pot_columns = {'orderno': orderno, 'shipdate': shipdate, 'weight': weight,
#                        'service': sv, 'zip': address, 'country': address1, 'price': current_price}
#         for pot_c in pot_columns.keys():
#             for col in pot_columns[pot_c]['header']:
#                 found = False
#                 if col in col_set:
#                     if pot_c != 'weight':
#                         dtype[col] = pot_columns[pot_c]['format']
#                     else:
#                         weight_name = col
#                     headers[col] = pot_c
#                     found = True
#                     break
#             if not found:
#                 not_found.append(pot_c)
#         # optional_pot_columns = {'dim1': dim1, 'dim2': dim2, 'dim3': dim3}
#         # for pot_c in optional_pot_columns.keys():
#         #     for col in optional_pot_columns[pot_c]['header']:
#         #         found = False
#         #         if col in col_set:
#         #             dtype[col] = optional_pot_columns[pot_c]['format']
#         #             headers[col] = pot_c
#         #             found = True
#         #             break
#         columns = list(headers.keys())
#         print(columns)
#         print(dtype)
#         print(headers)
#         # dtype = {columns[2]: type[address['format']],
#         #          columns[3]: type[address['format']], columns[4]: type[current_price['format']],
#         #          columns[5]: type[dimensions['format']], columns[6]: type[dimensions['format']],
#         #          columns[7]: type[dimensions['format']], columns[8]: type[insured_parcel['format']]}
#         # headers = {columns[0]: 'shipdate', columns[1]: 'weight',
#         #            columns[2]: 'zip', columns[3]: 'country', columns[4]: 'price',
#         #            columns[5]: 'dim0', columns[6]: 'dim1', columns[7]: 'dim2', columns[8]: 'insured'}
#         # if (dimensions['header0'] not in col_set):
#         #     del dtype[columns[-4]]
#         #     del dtype[columns[-3]]
#         #     del dtype[columns[-2]]
#         #     del headers[columns[-4]]
#         #     del headers[columns[-3]]
#         #     del headers[columns[-2]]
#         #     print(columns)
#         #     columns = columns[:-4]+[columns[-1]]
#         #     print(columns)
#         if weight_name:
#             print(weight_name)
#             df.dropna(subset=[weight_name], inplace=True)
#             for i, row in df.iterrows():
#                 weight_test = str(row[weight_name])
#                 if weight_test.replace('.', '').isnumeric():
#                     dtype[weight_name] = weight['format']
#                     df = create_df(columns, dtype, headers)
#                     break
#                 elif 'oz' in weight_test or 'lb' in weight_test or 'lbs' in weight_test:
#                     dtype[weight_name] = 'str'
#                     df = create_df(columns, dtype, headers)
#                     df['weight'] = df.apply(lambda row: mflib.w_lbs_or_w_oz(row['weight']), axis=1)
#                     break
#         else:
#             df = create_df(columns, dtype, headers)
#         # At this point we either have a service column with vendor and service code,
#         # or a service code column with an optional service provider column.
#         # If service provider is given, concatenate with service code.
#         # df['insured'] = (df['insured'] > 0)
#         # print(df.head())
#         empty_cols = ai1s_headers.difference(set(df.columns)).difference({'shipdate', 'zip', 'country', 'service'})
#         for col in empty_cols:
#             df[col] = None
#         return df
#
#     def shipstation(col_set, df):
#         columns = []
#         dtype = {}
#         headers = {}
#         not_found = []
#         weight_name = None
#         pot_columns = {'orderno': orderno, 'shipdate': shipdate, 'weight': weight,
#                        'service': sv, 'zip': address, 'country': address1, 'price': current_price,
#                        'insured': insured_parcel}
#         for pot_c in pot_columns.keys():
#             for col in pot_columns[pot_c]['header']:
#                 found = False
#                 if col in col_set:
#                     if pot_c != 'weight':
#                         dtype[col] = pot_columns[pot_c]['format']
#                     else:
#                         weight_name = col
#                     headers[col] = pot_c
#                     found = True
#                     break
#             if not found:
#                 not_found.append(pot_c)
#         optional_pot_columns = {'dim1': dim1, 'dim2': dim2, 'dim3': dim3}
#         for pot_c in optional_pot_columns.keys():
#             for col in optional_pot_columns[pot_c]['header']:
#                 found = False
#                 if col in col_set:
#                     dtype[col] = optional_pot_columns[pot_c]['format']
#                     headers[col] = pot_c
#                     found = True
#                     break
#         columns = list(headers.keys())
#         print(columns)
#         print(dtype)
#         print(headers)
#         sv_main, sv_alt0, sv_alt1 = False, False, False
#         if 'service' in headers.values():
#             sv_main = True
#         else:
#             if sv['header_alt'][0][0] in col_set:
#                 sv_alt0 = True
#                 columns.append(sv['header_alt'][0][0])
#                 headers[columns[-1]] = 'service_provider'
#                 dtype[headers[columns[-1]]] = type_conv[sv['format']]
#             elif sv['header_alt'][0][1] in col_set:
#                 sv_alt0 = True
#                 columns.append(sv['header_alt'][0][1])
#                 headers[columns[-1]] = 'service_provider'
#                 dtype[headers[columns[-1]]] = type_conv[sv['format']]
#             if sv['header_alt'][1][0] in col_set:
#                 sv_alt1 = True
#                 columns.append(sv['header_alt'][1][0])
#                 headers[columns[-1]] = 'service_code'
#                 dtype[headers[columns[-1]]] = type_conv[sv['format']]
#             elif sv['header_alt'][1][1] in col_set:
#                 sv_alt1 = True
#                 columns.append(sv['header_alt'][1][1])
#                 headers[columns[-1]] = 'service_code'
#                 dtype[headers[columns[-1]]] = type_conv[sv['format']]
#             # if not (sv_alt0 and sv_alt1):
#             #     return  # If service name not included, we can't process the file. Service provider name is not essential
#         if weight_name:
#             weight_test = str(df[weight_name].iloc[0])
#             if weight_test.replace('.', '').isnumeric():
#                 dtype[weight_name] = weight['format']
#                 df = create_df(columns, dtype, headers)
#             elif 'oz' in weight_test or 'lb' in weight_test or 'lbs' in weight_test:
#                 dtype[weight_name] = 'str'
#                 df = create_df(columns, dtype, headers)
#                 df['weight'] = df.apply(lambda row: mflib.w_lbs_or_w_oz(row['weight']), axis=1)
#         else:
#             df = create_df(columns, dtype, headers)
#         # At this point we either have a service column with vendor and service code,
#         # or a service code column with an optional service provider column.
#         # If service provider is given, concatenate with service code.
#         if sv_alt0 and sv_alt1:
#             df['service'] = df[['service_provider', 'service_code']].agg(' '.join, axis=1)
#             del df['service_provider']
#             del df['service_code']
#         empty_cols = ai1s_headers.difference(set(df.columns)).difference({'shipdate', 'zone', 'country', 'service'})
#         for col in empty_cols:
#             df[col] = None
#         return df
#
#     def sellercloud_shipbridge(col_set, df):
#         print(col_set)
#         columns = []
#         dtype = {}
#         headers = {}
#         not_found = []
#         weight_name = None
#         # [orderno['header'], shipdate['header'], weight['header'],
#         # sv['header'], address['header'], current_price['header']]
#         pot_columns = {'orderno': orderno, 'shipdate': shipdate, 'weight': weight,
#                        'service': sv, 'address': address, 'price': current_price}
#         for pot_c in pot_columns.keys():
#             for col in pot_columns[pot_c]['header']:
#                 found = False
#                 if col in col_set:
#                     if pot_c != 'weight':
#                         dtype[col] = pot_columns[pot_c]['format']
#                     else:
#                         weight_name = col
#                     headers[col] = pot_c
#                     found = True
#                     break
#             if not found:
#                 not_found.append(pot_c)
#         # optional_pot_columns = {'dim1': dim1, 'dim2': dim2, 'dim3': dim3}
#         # for pot_c in optional_pot_columns.keys():
#         #     for col in optional_pot_columns[pot_c]['header']:
#         #         found = False
#         #         if col in col_set:
#         #             dtype[col] = optional_pot_columns[pot_c]['format']
#         #             headers[col] = pot_c
#         #             found = True
#         #             break
#         columns = list(headers.keys())
#         print(columns)
#         print(dtype)
#         print(headers)
#         # sv_main, sv_alt0, sv_alt1 = False, False, False
#         # if 'service' in headers.values():
#         #     sv_main = True
#         # else:
#         #     if sv['header_alt'][0][0] in col_set:
#         #         sv_alt0 = True
#         #         columns.append(sv['header_alt'][0][0])
#         #         dtype[columns[-1]] = type[sv['format']]
#         #         headers[columns[-1]] = 'service_provider'
#         #     elif sv['header_alt'][0][1] in col_set:
#         #         sv_alt0 = True
#         #         columns.append(sv['header_alt'][0][1])
#         #         dtype[columns[-1]] = type[sv['format']]
#         #         headers[columns[-1]] = 'service_provider'
#         #     if sv['header_alt'][1][0] in col_set:
#         #         sv_alt1 = True
#         #         columns.append(sv['header_alt'][1][0])
#         #         dtype[columns[-1]] = type[sv['format']]
#         #         headers[columns[-1]] = 'service_code'
#         #     elif sv['header_alt'][1][1] in col_set:
#         #         sv_alt1 = True
#         #         columns.append(sv['header_alt'][1][1])
#         #         dtype[columns[-1]] = type[sv['format']]
#         #         headers[columns[-1]] = 'service_code'
#         #     if not (sv_alt0 and sv_alt1):
#         #         return  # If service name not included, we can't process the file. Service provider name is not essential
#         if weight_name:
#             weight_test = str(df[weight_name].iloc[0])
#             if weight_test.replace('.', '').isnumeric():
#                 dtype[weight_name] = weight['format']
#                 print(weight_test)
#                 df = create_df(columns, dtype, headers)
#             elif 'oz' in weight_test or 'lb' in weight_test or 'lbs' in weight_test:
#                 print(weight_test)
#                 dtype[weight_name] = 'str'
#                 df = create_df(columns, dtype, headers)
#                 df['weight'] = df.apply(lambda row: mflib.w_lbs_or_w_oz(row['weight']), axis=1)
#         else:
#             df = create_df(columns, dtype, headers)
#         df[['zip', 'country']] = df.apply(lambda row: mflib.add_to_zip_ctry(row.address), axis=1, result_type='expand')
#         # At this point we either have a service column with vendor and service code,
#         # or a service code column with an optional service provider column.
#         # If service provider is given, concatenate with service code.
#         # if sv_alt0:
#         #     df['service'] = df[['service_provider', 'service_code']].agg(' '.join, axis=1)
#         #     del df['service_provider']
#         #     del df['service_code']
#         empty_cols = ai1s_headers.difference(set(df.columns)).difference({'shipdate', 'zone', 'country', 'service'})
#         for col in empty_cols:
#             print(col)
#             df[col] = None
#         print(dtype)
#         return df
#
#     # import platform functions from local manifest func
#
#     col_set = set()
#     for i in df.columns:
#         col_set.add(i)
#     df = locals()[pf](col_set, df)
#     print(df.head(10))
#     df.reset_index(inplace=True, drop=True)
#     df, generated_columns = generate_defaults(df)
#     # if ''
#     # print(df.iloc[:, :])
#     df[['country', 'zone', 'weight threshold', 'sugg. service', 'bill weight', '2021 tier 1', '2021 tier 2', '2021 tier 3', '2021 tier 4',
#         '2021 tier 5', '2021 DHL', '2021 USPS', 'shipdate DHL', 'shipdate USPS']] = df.apply(
#         lambda row: mflib.row_to_rate(row), axis=1, result_type='expand')
#     df.weight = df.apply(lambda row: ceil(row.weight) if row.weight < 16 else ceil(row.weight/16)*16, axis=1)
#     df.loc[~df['bill weight'].isna(), 'weight'] = df['bill weight']
#     del df['bill weight']
#     # df.astype({'2021 tier 1': 'float', '2021 tier 2': 'float'}).dtypes
#     df['shipdate'] = df['shipdate'].dt.strftime('%Y-%m-%d')
#     # return jsonify({'dataframe': df.values.tolist()})
#     # result = jsonify({'object1': 1, 'object2': 2})
#     # print(result, 'x')
#     manifest_id_insert = 'INSERT INTO manifest (name, init_date) \
#         VALUES (%s, %s)'
#     manifest_data_insert = 'INSERT INTO manifest_data (id, order_no, ship_date, weight, service \
#         address, address1, insured, dim1, dim2, dim3) \
#         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
#     # mycursor.execute(manifest_id_insert, (name, date.today()))
#     # mydb.commit()
#     # stmt = select([table.columns.id]).where(table.columns.name == name)
#     # res = conn.execute(stmt).fetchone()
#     # df.to_sql(con=conn, name='manifest_data', if_exists='append', index=False)
#     # mycursor.executemany(manifest_data_insert, (name, date.today()))
#     # mydb.commit()
#     # table = Table(
#     #     'manifest',
#     #     metadata,
#     #     autoload=True,
#     #     autoload_with=engine
#     # )
#     # stmt = select([table.columns.id]).where(table.columns.name == 'test')
#     # results = conn.execute(stmt).fetchone()
#     # print(results)
#     summary = None
#     # engine.execute(manifest_id_insert, (name, date.today()))
#     subset = ['service', 'weight threshold', 'country']
#     df.loc[df['zone'].str.len() != 7, 'country'] = 'Intl'
#     df_unique_services = df[['service', 'weight threshold', 'country',
#                              'sugg. service']].drop_duplicates(subset=subset, inplace=False)
#     df_unique_services.sort_values(by=['country', 'weight threshold', 'service'],
#                                    ascending=[False, True, True], inplace=True)
#     date_range = [df['shipdate'].min(), df['shipdate'].max()]
#     df = df.replace({np.nan: None})
#     pd.set_option('display.max_columns', None)
#     df = df.rename(generated_columns, axis=1)
#     print(df.head(20))
#
#     dom_service_names = []
#     intl_service_names = []
#     for v in service.values():
#         if v[-1] == 'domestic':
#             dom_service_names.append(v[3])
#         elif v[-1] == 'international':
#             intl_service_names.append(v[3])
#     # return jsonify({'domestic services': dom_service_names, 'international services': intl_service_names})
#     dom_intl = {'domestic services': dom_service_names, 'international services': intl_service_names}
#     response = {'filtered shipments': df.to_dict(orient='list'),
#                 'summary': summary, 'service options': dom_intl,
#                 'suggested services': df_unique_services.to_dict(orient='records'),
#                 'date range': date_range}
#     generated_columns_rev = {v: k for k, v in generated_columns.items()}
#     df = df.rename(generated_columns_rev, axis=1)
#     df.columns = df.columns.str.replace(' ', '_')
#     df.columns = df.columns.str.replace('.', '')
#     print(df.columns)
#     manifest_user = ManifestModel(name=name)
#     manifest_user.save_to_db()
#     id = ManifestModel.find_by_name(name=name).id
#     df['id'] = id
#     try:
#         df.to_sql(con=conn, name='manifest_data', if_exists='append', index=False)
#         saved_to_db = True
#     except:
#         manifest_user.delete_from_db()
#         saved_to_db = False
#     response['saved to db'] = saved_to_db
#     response = jsonify(response)
#     tracemalloc.stop()
#     print(f"Current memory usage is {current / 10**6}MB; Peak was {peak / 10**6}MB")
#     return response


# @ app.route('/manifest/', methods=['POST'])
# @ cross_origin()  # REMOVE
# def manifest():
#     request_data = request.get_json()
#     # print(request_data)
#     # return jsonify(request_data)
#     if not request_data:
#         return jsonify({'message': 'No data sent.'}), 400
#     if 'shipments' not in request_data:
#         return jsonify({'message': 'shipments object missing.'}), 400
#     if 'name' not in request_data:
#         return jsonify({'message': 'name object missing.'}), 400
#     name = request_data['name']
#     print(name)
#     if len(name) >= 45:
#         return jsonify({'message': f'name object is {len(name)} characters long. Please enter max 45 characters.'}), 400
#     sql_check = 'SELECT name FROM manifest WHERE name = %s'
#     table = Table(
#         'manifest',
#         metadata,
#         autoload=True,
#         autoload_with=engine
#     )
#     stmt = select([table.columns.id]).where(table.columns.name == name)
#     res = conn.execute(stmt).fetchone()
#     # if res:
#     #     return f'Name "{name}" already exists. Please choose a different name.', 400
#     if 'platform' not in request_data:
#         return jsonify({'message': 'platform object missing.'}), 400
#     shipments = request_data['shipments']
#     pf = request_data['platform'].lower()
#     orderno = format_hash['order_no'][pf]  # not required
#     shipdate = format_hash['date'][pf]
#     weight = format_hash['weight'][pf]
#     sv = format_hash['service'][pf]  # not required
#     address = format_hash['address'][pf]
#     address1 = format_hash['address1'].get(pf)
#     current_price = format_hash['current_price'].get(pf)  # optional
#     insured_parcel = format_hash['insured_parcel'].get(pf)  # optional
#     dim1 = format_hash['dim1'].get(pf)  # optional
#     dim2 = format_hash['dim2'].get(pf)  # optional
#     dim3 = format_hash['dim3'].get(pf)  # optional
#
#     def create_df(shipments, dtype, headers):
#         print('creating df')
#         # shipment_dict = {}
#         # for header in shipments[0]:
#         #     if header in headers:
#         #         shipment_dict[headers[header]] = []
#         # for row in shipments[1:]:
#         #     for i, header in enumerate(shipments[0]):
#         #         if header in headers:
#         #             shipment_dict[headers[header]].append(row[i] if row[i] else None)
#         shipments = {v: shipments[k] for k, v in headers.items()}
#         print(dtype, headers, 'x')
#         # df = pd.DataFrame(shipments[1:], columns=shipments[0], usecols=columns, dtype=dtype)
#         df = pd.DataFrame.from_dict(shipments).replace(r'^\s*$', np.NaN, regex=True).astype(dtype)
#         df.fillna(value=np.NaN, inplace=True)
#         # df.dropna(subset=['weight'], inplace=True)
#         df = df[df.weight != 'nan']
#         # df.dropna(subset=['weight'], inplace=True)
#         # df = df.replace(to_replace=None, value=np.nan)
#         # parse_dates=[columns[1]], date_parser=dateparse
#         # df = df[columns]
#         # df = df.rename(columns=headers)
#         if 'insured' in dtype and dtype['insured'] == 'float':
#             df['insured'].fillna(value=0, inplace=True)
#             df['insured'] = df['insured'].astype(bool)
#         # df.loc[~df['bill weight'].isna(), 'weight'] = df['bill weight']
#         print(df.head(5))
#         # df['shipdate'] = pd.to_datetime(df['shipdate'])
#         # REMOVE AFTER TESTING:
#         # for shipdate in shipments['shipdate'][:5]:
#         #     print(shipdate, type(shipdate))
#         #     if isinstance(shipdate, float):
#         #         df = df.astype({'shipdate': 'int'})
#         #         shipdate_is_numeric = True
#         #     elif isinstance(shipdate, int):
#         #         shipdate_is_numeric = True
#         shipdate_is_numeric = False
#         for shipdate in shipments['shipdate'][:5]:
#             print(type(shipdate))
#             if isinstance(shipdate, int):
#                 shipdate_is_numeric = True
#                 # df = df.astype({'shipdate': 'int'})
#                 # break
#             elif isinstance(shipdate, float):
#                 df = df.astype({'shipdate': 'float'})
#                 df = df.astype({'shipdate': 'int'})
#                 shipdate_is_numeric = True
#                 break
#         if shipdate_is_numeric:
#             df['shipdate'] = df.apply(lambda row: datetime.fromordinal(
#                 datetime(1900, 1, 1).toordinal() + int(row['shipdate']) - 2), axis=1)
#         else:
#             df['shipdate'] = pd.to_datetime(df['shipdate'])
#         pd.set_option('display.max_columns', None)
#         print('df created successfully')
#         return df
#
#     def shopify(shipments, col_set, df):
#         print(col_set)
#         columns = []
#         dtype = {}
#         headers = {}
#         not_found = []
#         weight_name = None
#         pot_columns = {'orderno': orderno, 'shipdate': shipdate, 'weight': weight,
#                        'service': sv, 'zip': address, 'country': address1, 'price': current_price}
#         for pot_c in pot_columns.keys():
#             for col in pot_columns[pot_c]['header']:
#                 found = False
#                 if col in col_set:
#                     if pot_c != 'weight':
#                         dtype[pot_c] = pot_columns[pot_c]['format']
#                     else:
#                         weight_name = col
#                     headers[col] = pot_c
#                     found = True
#                     break
#             if not found:
#                 not_found.append(pot_c)
#         # optional_pot_columns = {'dim1': dim1, 'dim2': dim2, 'dim3': dim3}
#         # for pot_c in optional_pot_columns.keys():
#         #     for col in optional_pot_columns[pot_c]['header']:
#         #         found = False
#         #         if col in col_set:
#         #             dtype[col] = optional_pot_columns[pot_c]['format']
#         #             headers[col] = pot_c
#         #             found = True
#         #             break
#         columns = list(headers.keys())
#         print(columns)
#         print(dtype)
#         print(headers)
#         # dtype = {columns[2]: type[address['format']],
#         #          columns[3]: type[address['format']], columns[4]: type[current_price['format']],
#         #          columns[5]: type[dimensions['format']], columns[6]: type[dimensions['format']],
#         #          columns[7]: type[dimensions['format']], columns[8]: type[insured_parcel['format']]}
#         # headers = {columns[0]: 'shipdate', columns[1]: 'weight',
#         #            columns[2]: 'zip', columns[3]: 'country', columns[4]: 'price',
#         #            columns[5]: 'dim0', columns[6]: 'dim1', columns[7]: 'dim2', columns[8]: 'insured'}
#         # if (dimensions['header0'] not in col_set):
#         #     del dtype[columns[-4]]
#         #     del dtype[columns[-3]]
#         #     del dtype[columns[-2]]
#         #     del headers[columns[-4]]
#         #     del headers[columns[-3]]
#         #     del headers[columns[-2]]
#         #     print(columns)
#         #     columns = columns[:-4]+[columns[-1]]
#         #     print(columns)
#         if weight_name:
#             print(weight_name)
#             df.dropna(subset=[weight_name], inplace=True)
#             for i, row in df.iterrows():
#                 weight_test = str(row[weight_name])
#                 if weight_test.replace('.', '').isnumeric():
#                     dtype['weight'] = weight['format']
#                     df = create_df(shipments, dtype, headers)
#                     break
#                 elif 'oz' in weight_test or 'lb' in weight_test or 'lbs' in weight_test:
#                     dtype['weight'] = 'str'
#                     df = create_df(shipments, dtype, headers)
#                     df['weight'] = df.apply(lambda row: mflib.w_lbs_or_w_oz(row['weight']), axis=1)
#                     break
#         else:
#             df = create_df(shipments, dtype, headers)
#         # At this point we either have a service column with vendor and service code,
#         # or a service code column with an optional service provider column.
#         # If service provider is given, concatenate with service code.
#         # df['insured'] = (df['insured'] > 0)
#         # print(df.head())
#         empty_cols = ai1s_headers.difference(set(df.columns))
#         for col in empty_cols:
#             df[col] = None
#         return df
#
#     def shipstation(shipments, col_set, df):
#         columns = []
#         dtype = {}
#         headers = {}
#         not_found = []
#         weight_name = None
#         pot_columns = {'orderno': orderno, 'shipdate': shipdate, 'weight': weight,
#                        'service': sv, 'zip': address, 'country': address1, 'price': current_price,
#                        'insured': insured_parcel}
#         for pot_c in pot_columns.keys():
#             for col in pot_columns[pot_c]['header']:
#                 found = False
#                 if col in col_set:
#                     if pot_c != 'weight':
#                         dtype[pot_c] = pot_columns[pot_c]['format']
#                     else:
#                         weight_name = col
#                     headers[col] = pot_c
#                     found = True
#                     break
#             if not found:
#                 not_found.append(pot_c)
#         optional_pot_columns = {'dim1': dim1, 'dim2': dim2, 'dim3': dim3}
#         for pot_c in optional_pot_columns.keys():
#             for col in optional_pot_columns[pot_c]['header']:
#                 found = False
#                 if col in col_set:
#                     dtype[pot_c] = optional_pot_columns[pot_c]['format']
#                     headers[col] = pot_c
#                     found = True
#                     break
#         columns = list(headers.keys())
#         print(columns)
#         print(dtype)
#         print(headers)
#         sv_main, sv_alt0, sv_alt1 = False, False, False
#         if 'service' in headers.values():
#             sv_main = True
#         else:
#             if sv['header_alt'][0][0] in col_set:
#                 sv_alt0 = True
#                 columns.append(sv['header_alt'][0][0])
#                 headers[columns[-1]] = 'service_provider'
#                 dtype[headers[columns[-1]]] = type_conv[sv['format']]
#             elif sv['header_alt'][0][1] in col_set:
#                 sv_alt0 = True
#                 columns.append(sv['header_alt'][0][1])
#                 headers[columns[-1]] = 'service_provider'
#                 dtype[headers[columns[-1]]] = type_conv[sv['format']]
#             if sv['header_alt'][1][0] in col_set:
#                 sv_alt1 = True
#                 columns.append(sv['header_alt'][1][0])
#                 headers[columns[-1]] = 'service_code'
#                 dtype[headers[columns[-1]]] = type_conv[sv['format']]
#             elif sv['header_alt'][1][1] in col_set:
#                 sv_alt1 = True
#                 columns.append(sv['header_alt'][1][1])
#                 headers[columns[-1]] = 'service_code'
#                 dtype[headers[columns[-1]]] = type_conv[sv['format']]
#             if not (sv_alt0 and sv_alt1):
#                 return  # If service name not included, we can't process the file. Service provider name is not essential
#         if weight_name:
#             weight_test = str(df[weight_name].iloc[0])
#             if weight_test.replace('.', '').isnumeric():
#                 dtype['weight'] = weight['format']
#                 df = create_df(shipments, dtype, headers)
#             elif 'oz' in weight_test or 'lb' in weight_test or 'lbs' in weight_test:
#                 dtype['weight'] = 'str'
#                 df = create_df(shipments, dtype, headers)
#                 df['weight'] = df.apply(lambda row: mflib.w_lbs_or_w_oz(row['weight']), axis=1)
#         else:
#             df = create_df(shipments, dtype, headers)
#         # At this point we either have a service column with vendor and service code,
#         # or a service code column with an optional service provider column.
#         # If service provider is given, concatenate with service code.
#         if sv_alt0:
#             df['service'] = df[['service_provider', 'service_code']].agg(' '.join, axis=1)
#             del df['service_provider']
#             del df['service_code']
#         print(df.head(5))
#         empty_cols = ai1s_headers.difference(set(df.columns))
#         for col in empty_cols:
#             df[col] = None
#         return df
#
#     def sellercloud_shipbridge(path, col_set, df):
#         print(col_set)
#         columns = []
#         dtype = {}
#         headers = {}
#         not_found = []
#         weight_name = None
#         # [orderno['header'], shipdate['header'], weight['header'],
#         # sv['header'], address['header'], current_price['header']]
#         pot_columns = {'orderno': orderno, 'shipdate': shipdate, 'weight': weight,
#                        'service': sv, 'address': address, 'price': current_price}
#         for pot_c in pot_columns.keys():
#             for col in pot_columns[pot_c]['header']:
#                 print(col)
#                 print(pot_c)
#                 found = False
#                 if col in col_set:
#                     if pot_c != 'weight':
#                         dtype[pot_c] = pot_columns[pot_c]['format']
#                     else:
#                         weight_name = col
#                     headers[col] = pot_c
#                     found = True
#                     break
#             if not found:
#                 not_found.append(pot_c)
#         # optional_pot_columns = {'dim1': dim1, 'dim2': dim2, 'dim3': dim3}
#         # for pot_c in optional_pot_columns.keys():
#         #     for col in optional_pot_columns[pot_c]['header']:
#         #         found = False
#         #         if col in col_set:
#         #             dtype[col] = optional_pot_columns[pot_c]['format']
#         #             headers[col] = pot_c
#         #             found = True
#         #             break
#         columns = list(headers.keys())
#         print(columns)
#         print(dtype)
#         print(headers)
#         # sv_main, sv_alt0, sv_alt1 = False, False, False
#         # if 'service' in headers.values():
#         #     sv_main = True
#         # else:
#         #     if sv['header_alt'][0][0] in col_set:
#         #         sv_alt0 = True
#         #         columns.append(sv['header_alt'][0][0])
#         #         dtype[columns[-1]] = type[sv['format']]
#         #         headers[columns[-1]] = 'service_provider'
#         #     elif sv['header_alt'][0][1] in col_set:
#         #         sv_alt0 = True
#         #         columns.append(sv['header_alt'][0][1])
#         #         dtype[columns[-1]] = type[sv['format']]
#         #         headers[columns[-1]] = 'service_provider'
#         #     if sv['header_alt'][1][0] in col_set:
#         #         sv_alt1 = True
#         #         columns.append(sv['header_alt'][1][0])
#         #         dtype[columns[-1]] = type[sv['format']]
#         #         headers[columns[-1]] = 'service_code'
#         #     elif sv['header_alt'][1][1] in col_set:
#         #         sv_alt1 = True
#         #         columns.append(sv['header_alt'][1][1])
#         #         dtype[columns[-1]] = type[sv['format']]
#         #         headers[columns[-1]] = 'service_code'
#         #     if not (sv_alt0 and sv_alt1):
#         #         return  # If service name not included, we can't process the file. Service provider name is not essential
#         if weight_name:
#             weight_test = str(df[weight_name].iloc[0])
#             if weight_test.replace('.', '').isnumeric():
#                 dtype['weight'] = weight['format']
#                 print(weight_test)
#                 df = create_df(shipments, dtype, headers)
#             elif 'oz' in weight_test or 'lb' in weight_test or 'lbs' in weight_test:
#                 print(weight_test)
#                 dtype['weight'] = 'str'
#                 df = create_df(shipments, dtype, headers)
#                 df['weight'] = df.apply(lambda row: mflib.w_lbs_or_w_oz(row['weight']), axis=1)
#         else:
#             df = create_df(shipments, dtype, headers)
#         df[['zip', 'country']] = df.apply(lambda row: mflib.add_to_zip_ctry(row.address), axis=1, result_type='expand')
#         # At this point we either have a service column with vendor and service code,
#         # or a service code column with an optional service provider column.
#         # If service provider is given, concatenate with service code.
#         # if sv_alt0:
#         #     df['service'] = df[['service_provider', 'service_code']].agg(' '.join, axis=1)
#         #     del df['service_provider']
#         #     del df['service_code']
#         print(df.head(5))
#         empty_cols = ai1s_headers.difference(set(df.columns))
#         for col in empty_cols:
#             print(col)
#             df[col] = None
#         print(dtype)
#         return df
#         #
#         # columns = [orderno['header'], shipdate['header'], weight['header'],
#         #            sv['header'], address['header'], current_price['header']]
#         # for col in columns:
#         #     if col not in col_set:
#         #         print(f'{col} not found in file')
#         #         return
#         # dtype = {columns[0]: type[orderno['format']], columns[1]: type[shipdate['format']], columns[2]: type[weight['format']],
#         #          columns[3]: type[sv['format']], columns[4]: type[address['format']], columns[5]: type[current_price['format']]}
#         # headers = {columns[0]: 'orderno', columns[1]: 'shipdate', columns[2]: 'weight',
#         #            columns[3]: 'service', columns[4]: 'address', columns[5]: 'price'}
#         # df = create_df(path, columns, dtype, headers)
#         # if weight['parse'] == 'w_lbs_or_w_oz':
#         #     df.weight = df.apply(lambda row: mflib.w_lbs_or_w_oz(row['weight']), axis=1)
#         # df.drop(df[df['weight'] == 0].index, inplace=True)
#         # print('----here----')
#         # df[['zip', 'country']] = df.apply(lambda row: mflib.add_to_zip_ctry(row.address), axis=1, result_type='expand')
#         # print('x', df.head(5))
#         # empty_cols = ai1s_headers.difference(set(df.columns))
#         # for col in empty_cols:
#         #     df[col] = None
#         # print('------------here---------------')
#         # print(df.head(10))
#         # return df
#
#     sample_data = {k: v[:5] for k, v in shipments.items()}
#     df = pd.DataFrame.from_dict(sample_data)
#     print(sample_data)
#     # print(shipments[:6])
#     print(df.head(5))
#     col_set = set()
#     for i in df.columns:
#         col_set.add(i)
#     df = locals()[pf](shipments, col_set, df)
#     print(df.head(10))
#     current, peak = tracemalloc.get_traced_memory()
#     print(f"Current memory usage is {current / 10**6}MB; Peak was {peak / 10**6}MB")
#     df[['country', 'zone', 'weight threshold', 'sugg. service', 'bill weight', '2021 tier 1', '2021 tier 2', '2021 tier 3', '2021 tier 4',
#         '2021 tier 5', '2021 DHL', '2021 USPS', 'shipdate DHL', 'shipdate USPS']] = df.apply(
#         lambda row: mflib.row_to_rate(row), axis=1, result_type='expand')
#     df.weight = df.apply(lambda row: ceil(row.weight) if row.weight < 16 else ceil(row.weight/16)*16, axis=1)
#     df.loc[~df['bill weight'].isna(), 'weight'] = df['bill weight']
#     del df['bill weight']
#     # df.astype({'2021 tier 1': 'float', '2021 tier 2': 'float'}).dtypes
#     df['shipdate'] = df['shipdate'].dt.strftime('%Y-%m-%d')
#     tracemalloc.stop()
#     # return jsonify({'dataframe': df.values.tolist()})
#     # result = jsonify({'object1': 1, 'object2': 2})
#     # print(result, 'x')
#     manifest_id_insert = 'INSERT INTO manifest (name, init_date) \
#         VALUES (%s, %s)'
#     manifest_data_insert = 'INSERT INTO manifest_data (name, order_no, ship_date, weight, service \
#         address, address1, insured, dim1, dim2, dim3) \
#         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
#     # mycursor.execute(manifest_id_insert, (name, date.today()))
#     # df['id'] = name
#     # df.to_sql(con=conn, name='manifest_data_test', if_exists='append', index=False)
#     # mycursor.executemany(manifest_data_insert, (name, date.today()))
#     # mydb.commit()
#     # table = Table(
#     #     'manifest',
#     #     metadata,
#     #     autoload=True,
#     #     autoload_with=engine
#     # )
#     # stmt = select([table.columns.id]).where(table.columns.name == 'test')
#     # results = conn.execute(stmt).fetchone()
#     # print(results)
#     summary = None
#     # engine.execute(manifest_id_insert, (name, date.today()))
#     subset = ['service', 'weight threshold', 'country']
#     df.loc[df['zone'].str.len() != 7, 'country'] = 'Intl'
#     df_unique_services = df[['service', 'weight threshold', 'country',
#                              'sugg. service']].drop_duplicates(subset=subset, inplace=False)
#     df_unique_services.sort_values(by=['country', 'weight threshold', 'service'],
#                                    ascending=[False, True, True], inplace=True)
#     date_range = [df['shipdate'].min(), df['shipdate'].max()]
#     print(df.head(20))
#     df = df.replace({np.nan: None})
#     pd.set_option('display.max_columns', None)
#     print(df.head(20))
#     response = jsonify({'filtered shipments': df.to_dict(orient='list'),
#                         'summary': summary,
#                         'suggested services': df_unique_services.to_dict(orient='records'),
#                         'service options': service_options,
#                         'date range': date_range})
#     df.columns = df.columns.str.replace(' ', '_')
#     df.columns = df.columns.str.replace('.', '')
#     # engine.execute(manifest_id_insert, (name, date.today()))
#     # df.to_sql(con=conn, name='manifest_data', if_exists='append', index=False)
#     return response

    # sql_insert = 'INSERT INTO manifest \
    # (name, init_date, order_no, ship_date, weight, service, address, address1, insured, \
    # dim1, dim2, dim3) \
    # VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    # init_date = date.today()
    # headers = [_ fo]
    # mycursor.execute(sql_insert, [id, init_date]+shipments[0])

    # if 'location' in request_data:
    #     loc = request_data['location']
    #     map[c][loc] = {}


@ app.route('/current_services/')
@ cross_origin()  # REMOVE
def current_services():
    dom_service_names = []
    intl_service_names = []
    for v in service.values():
        if v[-1] == 'domestic':
            dom_service_names.append(v[3])
        elif v[-1] == 'international':
            intl_service_names.append(v[3])
    return jsonify({'domestic services': dom_service_names, 'international services': intl_service_names})


@ app.route('/manifest_lookup/')
@ cross_origin()  # REMOVE
def manifest_lookup():
    return jsonify({'message': 'No name chosen'}), 400


# @app.route('/manifest_iter/', methods=['POST'])
# def manifest_iter():
#     request_data = request.get_json()
#
#     response = jsonify({})
#     response.headers.add('Access-Control-Allow-Origin', '*')

api.add_resource(CarrierItem, '/maptest')
api.add_resource(User, '/user')
api.add_resource(Manifest, '/manifest')
api.add_resource(ManifestFilter, '/manifest-filter')
api.add_resource(ManifestNames, '/previous-manifests')
api.add_resource(ManifestColumns, '/column-headers')
api.add_resource(ManifestAuthTest, '/manifest-auth-test')
api.add_resource(ManifestManual, '/manifest-manual')
# manifest-manual
# manifest-manual-submit

if __name__ == '__main__':
    from db import db
    # app.run(host='192.168.50.101', threaded=True, port=5000, debug=True, ssl_context=('cert.pem', 'key.pem'))
    # app.run(host='192.168.1.24', threaded=True, port=5000, debug=True, ssl_context=('cert.pem', 'key.pem'))
    db.init_app(app)
    app.run(threaded=True)
    app.run(host='192.168.50.101', threaded=True, port=5000, debug=True)
    # app.run(host='192.168.1.24', threaded=True, port=5000, debug=True)
