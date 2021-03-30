from flask_cors import CORS, cross_origin
import pandas as pd
from app_lib import service
# uncomment sqlalchemy
# from sqlalchemy import create_engine, select, insert, MetaData, Table, and_
# import mysql.connector
# import pickle
from resources.carrieritem import CarrierItem
from resources.user import User, UserLogin, UserLogout, TokenRefresh
from resources.manifest import Manifest, ManifestFilter, ManifestNames, ManifestColumns, ManifestManual, ManifestAuthTest, ManifestFormat
# from models.manifest import ManifestModel
# from security import identity, authenticate
from flask import Flask, jsonify  # , flash, redirect
from flask_restful import Api
from blocklist import BLOCKLIST
from flask_jwt_extended import JWTManager, jwt_required
import os
# from werkzeug.utils import secure_filename
# from flask_wtf import FlaskForm
# import mysql.connector
# service_options = [v[3] for v in service.values()]

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


def vali_date(date_text):
    try:
        datetime.strptime(date_text, '%Y-%m-%d')
        return str(datetime.strptime(date_text, '%Y-%m-%d').date())
    except ValueError:
        return
# mydb = mysql.connector.connect(
#     host="162.241.219.134",
#     user="allinoy4_user0",
#     password="+3mp0r@ry",
#     database="allinoy4_allin1ship"
# )
# mycursor = mydb.cursor()

#uncomment: engine
# engine = create_engine(
#     'mysql+mysqlconnector://allinoy4_user0:+3mp0r@ry@162.241.219.134:3306/allinoy4_allin1ship', pool_pre_ping=True)
# metadata = MetaData(bind=None)
# conn = engine.connect()


# with open(r'dependencies/charges_by_zone/carrier_charges111.pkl', 'rb') as f:
#     map = pickle.load(f)
# format_hash = mflib.ManifestFormat.format_hash
type_conv = {'str': str, 'float': float, 'int': pd.Int64Dtype(), 'bool': bool}

# mycursor = mydb.cursor()
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get(
    'DATABASE_URL', 'mysql+mysqlconnector://allinoy4_user0:+3mp0r@ry@162.241.219.134:3306/allinoy4_allin1ship')
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {'pool_size': 100, 'pool_recycle': 280, 'pool_pre_ping': True}
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['MAX_CONTENT_LENGTH'] = 20 * 1024 * 1024
app.config['PROPAGATE_EXCEPTIONS'] = True
app.config['JWT_BLACKLIST_TOKEN_CHECKS'] = ['access', 'refresh']
# app.config['UPLOAD_FOLDER'] = Manifest.upload_directory
app.config['UPLOAD_FOLDER'] = 'api_uploads'
app.config['JWT_BLACKLIST_ENABLED'] = True
app.config['JWT_BLACKLIST_TOKEN_CHECKS'] = ['access', 'refresh']
app.secret_key = 'roy'
api = Api(app)
# jwt = JWT(app, authenticate, identity)
jwt = JWTManager(app)


@jwt.token_in_blocklist_loader
def check_if_token_in_blocklist(jwt_header, jwt_payload):
    jti = jwt_payload['jti']
    return jti in BLOCKLIST


@jwt.expired_token_loader
def expired_token_callback(self, callback):
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
def token_not_fresh_callback(self, callback):
    return jsonify({'description': 'The token is not fresh.',
                    'error': 'fresh_token_required'}), 401


@jwt.revoked_token_loader
def revoked_token_callback(self, callback):
    return jsonify({'description': 'The token has been revoked.',
                    'error': 'token_revoked'}), 401


cors = CORS(app, resources={r"/*": {"origins": "*"}})


@app.route('/')
def index():
    return "<h1>Welcome to our server !!</h1>"


@app.route('/map/')
@jwt_required()
@ cross_origin()
def get_carriers():
    return 'ok'


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
api.add_resource(UserLogin, '/login')
api.add_resource(TokenRefresh, '/refresh')
api.add_resource(UserLogout, '/logout')
api.add_resource(ManifestFormat, '/manifest-format')
# manifest-manual
# manifest-manual-submit

if __name__ == '__main__':
    from db import db
    # app.run(host='192.168.50.101', threaded=True, port=5000, debug=True, ssl_context=('cert.pem', 'key.pem'))
    # app.run(host='192.168.1.24', threaded=True, port=5000, debug=True, ssl_context=('cert.pem', 'key.pem'))
    db.init_app(app)
    app.run(threaded=True, debug=True)
    # app.run(host='192.168.50.101', threaded=True, port=5000, debug=True)
    # app.run(host='192.168.1.24', threaded=True, port=5000, debug=True)
