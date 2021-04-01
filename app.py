from flask_cors import CORS, cross_origin
import pandas as pd
from app_lib import service
import random
import time
# uncomment sqlalchemy
# from sqlalchemy import create_engine, select, insert, MetaData, Table, and_
# import mysql.connector
# import pickle
import urllib
from werkzeug.exceptions import BadRequest
from QBOService import create_customer, get_companyInfo, query_customer_by_name, query_customer_by_id
from utils import excel, context, OAuth2Helper
import config
from resources.carrieritem import CarrierItem
from resources.user import User, UserLogin, UserLogout, TokenRefresh
from resources.manifest import Manifest, ManifestFilter, ManifestNames, ManifestColumns, ManifestManual, ManifestAuthTest, ManifestFormat
from flask import Flask, jsonify, request, redirect, url_for, session, g, flash, render_template
# , flash, redirect
from flask_restful import Api, Resource
from blocklist import BLOCKLIST
from flask_jwt_extended import JWTManager, jwt_required
import os
from celery import Celery
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
if os.environ.get('REDIS_URL'):
    app.config['REDIS_URL'] = os.environ.get('REDIS_URL')
else:
    from redis_cred import redis_url
    app.config['REDIS_URL'] = redis_url
app.secret_key = 'roy'
api = Api(app)
# jwt = JWT(app, authenticate, identity)
jwt = JWTManager(app)
app.config['CELERY_BROKER_URL'] = app.config['REDIS_URL']
app.config['CELERY_RESULT_BACKEND'] = app.config['REDIS_URL']
celery = Celery(app.name, backend='redis', broker=app.config['REDIS_URL'])
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


# @app.route('/')
# def index():
#     return "<h1>Welcome to our server !!</h1>"


@app.route('/')
def index():
    """Index route"""
    global customer_list
    customer_list = excel.load_excel()
    return render_template(
        'index.html',
        customer_dict=customer_list,
        title="QB Customer Leads",
    )


@app.route('/', methods=['POST'])
def update_table():
    """Update Excel file after customer is added in QBO"""
    customer_id = request.form['id']

    request_context = context.RequestContext(session['realm_id'], session['access_token'], session['refresh_token'])

    for customer in customer_list:
        if customer['Id'] == customer_id:
            # Create customer object
            response = create_customer(customer, request_context)

            # If customer added successfully, remove them from html and excel file
            if (response.status_code == 200):
                font_color = 'green'
                new_customer_list = excel.remove_lead(customer_list, customer_id)
                flash('Customer successfully added!')
                return render_template(
                    'index.html',
                    customer_dict=new_customer_list,
                    title='QB Customer Leads',
                    text_color=font_color
                )
            else:
                font_color = 'red'
                flash('Something went wrong: ' + response.text)
    return redirect('https://allin1ship-app.herokuapp.com/')


@app.route('/company-info')
def company_info():
    """Gets CompanyInfo of the connected QBO account"""
    request_context = context.RequestContext(session['realm_id'], session['access_token'], session['refresh_token'])

    response = get_companyInfo(request_context)
    if (response.status_code == 200):
        return render_template(
            'index.html',
            customer_dict=customer_list,
            company_info='Company Name: ' + response.json()['CompanyInfo']['CompanyName'],
            title='QB Customer Leads',
        )
    else:
        return render_template(
            'index.html',
            customer_dict=customer_list,
            company_info=response.text,
            title='QB Customer Leads',
        )


class Customer(Resource):
    def get(self):
        request_context = context.RequestContext(session['realm_id'], session['access_token'], session['refresh_token'])
        name = request.args.get('name')
        id = request.args.get('id')
        if name:
            response = query_customer_by_name(name, request_context)
            print(response)
            if (response.status_code == 200):
                return response.json()
            else:
                print(response.json())
                return 'error'
        elif id:
            response = query_customer_by_id(id, request_context)
            print(response)
            if (response.status_code == 200):
                return response.json()
            else:
                print(response.json())
                return 'error'


@app.route('/auth')
def auth():
    """Initiates the Authorization flow after getting the right config value"""
    params = {
        'scope': 'com.intuit.quickbooks.accounting',
        'redirect_uri': config.REDIRECT_URI,
        'response_type': 'code',
        'client_id': config.CLIENT_ID,
        'state': csrf_token()
    }
    url = OAuth2Helper.get_discovery_doc()['authorization_endpoint'] + '?' + urllib.parse.urlencode(params)
    return redirect(url)


@app.route('/reset-session')
def reset_session():
    """Resets session"""
    session.pop('qbo_token', None)
    session['is_authorized'] = False
    return redirect('https://allin1ship-app.herokuapp.com/')


@app.route('/callback')
def callback():
    """Handles callback only for OAuth2"""
    #session['realmid'] = str(request.args.get('realmId'))
    state = str(request.args.get('state'))
    error = str(request.args.get('error'))
    if error == 'access_denied':
        return redirect(index)
    if state is None:
        return BadRequest()
    elif state != csrf_token():  # validate against CSRF attacks
        return BadRequest('unauthorized')

    auth_code = str(request.args.get('code'))
    if auth_code is None:
        return BadRequest()

    bearer = OAuth2Helper.get_bearer_token(auth_code)
    realmId = str(request.args.get('realmId'))

    # update session here
    session['is_authorized'] = True
    session['realm_id'] = realmId
    session['access_token'] = bearer['access_token']
    session['refresh_token'] = bearer['refresh_token']

    return redirect('https://allin1ship-app.herokuapp.com/')


def csrf_token():
    token = session.get('csrfToken', None)
    if token is None:
        token = OAuth2Helper.secret_key()
        session['csrfToken'] = token
    return token


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


@celery.task(bind=True)
def long_task(self):
    """Background task that runs a long function with progress reports."""
    verb = ['Starting up', 'Booting', 'Repairing', 'Loading', 'Checking']
    adjective = ['master', 'radiant', 'silent', 'harmonic', 'fast']
    noun = ['solar array', 'particle reshaper', 'cosmic ray', 'orbiter', 'bit']
    message = ''
    total = random.randint(10, 50)
    for i in range(total):
        if not message or random.random() < 0.25:
            message = '{0} {1} {2}...'.format(random.choice(verb),
                                              random.choice(adjective),
                                              random.choice(noun))
        self.update_state(state='PROGRESS',
                          meta={'current': i, 'total': total,
                                'status': message})
        time.sleep(1)
    return {'current': 100, 'total': 100, 'status': 'Task completed!',
            'result': 42}


@app.route('/longtask', methods=['POST'])
def longtask():
    task = long_task.apply_async()
    return jsonify({'message': 'In progress.'}), 202, {'Location': url_for('taskstatus', task_id=task.id)}


@app.route('/status/<task_id>')
def taskstatus(task_id):
    task = long_task.AsyncResult(task_id)
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
    return jsonify(response)


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
api.add_resource(Customer, '/read-customer')
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
