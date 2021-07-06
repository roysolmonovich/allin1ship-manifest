from flask_cors import CORS, cross_origin
# import pandas as pd
from app_lib import service
from c import db_URL
# import random
# import time
# from sqlalchemy import create_engine, select, insert, MetaData, Table, and_
# import mysql.connector
# import pickle
# import urllib
# from werkzeug.exceptions import BadRequest
# import config
from resources.carrieritem import CarrierItem
from resources.user import User, UserLogin, UserLogout, TokenRefresh
from resources import ManifestNames, \
                      ManifestManual
from resources.manifest import Manifest, ManifestFilter, ManifestColumns, ManifestAuthTest, ManifestFormat, ManifestServiceUpdate
from flask import Flask, jsonify  # , request, redirect, url_for, session, g, flash, render_template
# , flash, redirect
from flask_restful import Api  # , Resource
from blocklist import BLOCKLIST
from flask_jwt_extended import JWTManager, jwt_required
import os
# from celery import Celery
# from werkzeug.utils import secure_filename
# from flask_wtf import FlaskForm
# import mysql.connector
# service_options = [v[3] for v in service.values()]

app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', db_URL)
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {'pool_size': 100, 'pool_recycle': 280, 'pool_pre_ping': True}
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['MAX_CONTENT_LENGTH'] = 20 * 1024 * 1024
app.config['PROPAGATE_EXCEPTIONS'] = True
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = False
app.config['JWT_BLACKLIST_TOKEN_CHECKS'] = ['access', 'refresh']
# app.config['UPLOAD_FOLDER'] = Manifest.upload_directory
app.config['UPLOAD_FOLDER'] = 'api_uploads'
app.config['JWT_BLACKLIST_ENABLED'] = True
# app.config['JWT_BLACKLIST_TOKEN_CHECKS'] = ['access', 'refresh']
if os.environ.get('REDIS_URL'):
    app.config['REDIS_URL'] = os.environ.get('REDIS_URL')
else:
    from redis_cred import redis_url
    app.config['REDIS_URL'] = redis_url
app.secret_key = 'roy'
api = Api(app)
# jwt = JWT(app, authenticate, identity)
jwt = JWTManager(app)
# app.config['CELERY_BROKER_URL'] = app.config['REDIS_URL']
# app.config['CELERY_RESULT_BACKEND'] = app.config['REDIS_URL']
# celery = Celery(app.name, backend='redis', broker=app.config['REDIS_URL'])
# celery.conf.update(app.config)


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


# inefficient - the current services do not need to be retrieved every time, just sent
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


# @celery.task(bind=True)
# def long_task(self):
#     """Background task that runs a long function with progress reports."""
#     verb = ['Starting up', 'Booting', 'Repairing', 'Loading', 'Checking']
#     adjective = ['master', 'radiant', 'silent', 'harmonic', 'fast']
#     noun = ['solar array', 'particle reshaper', 'cosmic ray', 'orbiter', 'bit']
#     message = ''
#     total = random.randint(10, 50)
#     for i in range(total):
#         if not message or random.random() < 0.25:
#             message = '{0} {1} {2}...'.format(random.choice(verb),
#                                               random.choice(adjective),
#                                               random.choice(noun))
#         self.update_state(state='PROGRESS',
#                           meta={'current': i, 'total': total,
#                                 'status': message})
#         time.sleep(1)
#     return {'current': 100, 'total': 100, 'status': 'Task completed!',
#             'result': 42}
#
#
# @app.route('/longtask', methods=['POST'])
# def longtask():
#     task = long_task.apply_async()
#     return jsonify({'message': 'In progress.'}), 202, {'Location': url_for('taskstatus', task_id=task.id)}
#
#
# @app.route('/status/<task_id>')
# def taskstatus(task_id):
#     task = long_task.AsyncResult(task_id)
#     if task.state == 'PENDING':
#         # job did not start yet
#         response = {
#             'state': task.state,
#             'current': 0,
#             'total': 1,
#             'status': 'Pending...'
#         }
#     elif task.state != 'FAILURE':
#         response = {
#             'state': task.state,
#             'current': task.info.get('current', 0),
#             'total': task.info.get('total', 1),
#             'status': task.info.get('status', '')
#         }
#         if 'result' in task.info:
#             response['result'] = task.info['result']
#     else:
#         # something went wrong in the background job
#         response = {
#             'state': task.state,
#             'current': 1,
#             'total': 1,
#             'status': str(task.info),  # this is the exception raised
#         }
#     return jsonify(response)


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
api.add_resource(ManifestServiceUpdate, '/manifest-service-update')
# api.add_resource(Customer, '/read-customer')
# manifest-manual
# manifest-manual-submit

if __name__ == '__main__':
    from db import db
    # app.run(host='192.168.50.101', threaded=True, port=5000, debug=True, ssl_context=('cert.pem', 'key.pem'))
    # app.run(host='192.168.1.24', threaded=True, port=5000, debug=True, ssl_context=('cert.pem', 'key.pem'))
    db.init_app(app)
    # app.run(threaded=True, debug=True)
    app.run(host='192.168.50.112', threaded=True, port=5000, debug=True)
    # app.run(host='192.168.1.24', threaded=True, port=5000, debug=True)
