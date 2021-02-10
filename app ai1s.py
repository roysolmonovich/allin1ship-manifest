from flask import Flask, jsonify, request, render_template
import pickle
import json
from lib import CarrierCharge, Customer
from datetime import date, datetime
app = Flask(__name__)

with open(r'hashes\charges by zone\carrier_charges111.pkl', 'rb') as f:
    map = pickle.load(f)


@app.route('/')
def home():
    return render_template('index.html')

# @app.route('/store', methods=['POST'])
# def create_store():
#     print('Here!')
#     request_data = request.get_json()
#     # print(request_data)
#     new_store = {'name': request_data['name'],
#     'items': []
#     }
#     stores.append(new_store)
#     return jsonify(new_store)

# @app.route('/store/')
# def get_stores():
#     return jsonify(stores)

# @app.route('/store/<string:store>')
# def get_store(store):
#     store = stores.get(store)
#     if store:
#         return jsonify({'store': store})
#     else:
#         return jsonify({'store': 'store not found'})

@app.route('/mapadd/', methods=['POST'])
def create_carrier_or_tier():
    request_data = request.get_json()
    if 'carrier' in request_data:
        c = request_data['carrier']
        map[c] = {}
    if 'location' in request_data:
        l = request_data['location']
        map[c][l] = {}
    if 'date' in request_data:
        # d = datetime.strptime(request_data['date'], '%Y-%m-%d').date()
        d = request_data['date']
        map[c][l][d] = {}
    if 'service' in request_data:
        sv = request_data['service']
        map[c][l][d][sv] = {}
    if 'zone' in request_data:
        z = request_data['zone']
        map[c][l][d][sv][z] = {}
    if 'weight' in request_data:
        w = request_data['weight']
        map[c][l][d][sv][z][w] = None
    if 'charge' in request_data:
        ch = request_data['charge']
        map[c][l][d][sv][z][w] = ch
    print(request_data)
    with open(r'..\hashes\charges by zone\carrier_charges.pkl', 'wb') as f:
        pickle.dump(map, f, pickle.HIGHEST_PROTOCOL)
    return jsonify(request_data)

@app.route('/map/')
def get_carriers():
    return jsonify({'carriers': list(map.keys())})

@app.route('/map/<string:carrier>/')
def get_locations(carrier):
    if carrier.isnumeric():
        carrier = int(carrier)
    return jsonify({'locations': map[carrier]})

@app.route('/map/<string:carrier>/<string:location>/')
def get_dates(carrier, location):
    if carrier.isnumeric():
        carrier = int(carrier)
    return jsonify({'dates': list(map[carrier][location].keys())})

@app.route('/map/<string:carrier>/<string:location>/<string:date>/')
def get_serivces(carrier, location, date):
    date = datetime.strptime(date, '%Y-%m-%d').date()
    if carrier.isnumeric():
        carrier = int(carrier)
    return jsonify({'services': list(map[carrier][location][date].keys())})

@app.route('/map/<string:carrier>/<string:location>/<string:date>/<int:service>/')
def get_zones(carrier, location, date, service):
    date = datetime.strptime(date, '%Y-%m-%d').date()
    if carrier.isnumeric():
        carrier = int(carrier)
    return jsonify({'zones': list(map[carrier][location][date][service].keys())})

@app.route('/map/<string:carrier>/<string:location>/<string:date>/<int:service>/<string:zone>/')
def get_weights(carrier, location, date, service, zone):
    date = datetime.strptime(date, '%Y-%m-%d').date()
    if carrier.isnumeric():
        carrier = int(carrier)
    return jsonify({'weights': list(map[carrier][location][date][service][zone].keys())})

@app.route('/map/<string:carrier>/<string:location>/<string:date>/<int:service>/<string:zone>/<float:weight>/')
def get_charge(carrier, location, date, service, zone, weight):
    # return jsonify({'stores': stores})
    weight=float(weight)
    date = datetime.strptime(date, '%Y-%m-%d').date()
    if carrier.isnumeric():
        carrier = int(carrier)
    return jsonify({'charge': CarrierCharge.charge_rate(carrier, location, date, service, zone, weight)})
    # return jsonify({'charge': map[carrier][location][date][service][zone][weight]})


# for c in map:
#     for loc in map[c]:
#         for d in map[c][loc]:
#             for sv in map[c][loc][d]:
#                 for z in map[c][loc][d][sv]:
#                     for w in map[c][loc][d][sv][z]:
#                         charge = map[c][loc][d][sv][z][w]
#                         if charge and not isinstance(charge, float):
#                             print(c,loc,d,sv,z,w,charge)

app.run(port=5000, debug=1)
