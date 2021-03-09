from flask_cors import CORS, cross_origin
import pandas as pd
from app_lib import service
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


type_conv = {'str': str, 'float': float, 'int': pd.Int64Dtype(), 'bool': bool}


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL')
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {'pool_size': 100, 'pool_recycle': 280, 'pool_pre_ping': True}
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['MAX_CONTENT_LENGTH'] = 20 * 1024 * 1024
app.config['PROPAGATE_EXCEPTIONS'] = True
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


cors = CORS(app, resources={r"/*": {"origins": "*"}})


@app.route('/')
def index():
    return "<h1>Welcome to our server !!</h1>"


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
    # app.run(host='192.168.50.101', threaded=True, port=5000, debug=True)
    # app.run(host='192.168.1.24', threaded=True, port=5000, debug=True)
