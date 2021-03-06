from flask import request
from lib import CarrierCharge
from flask_restful import Resource


class CarrierItem(Resource):
    def get(self):
        request_data = request.get_json()
        if 'carrier' not in request_data:
            return {'message': 'Carrier object is missing.'}, 400
        carrier = request_data['carrier']
        if carrier not in CarrierCharge.map.keys():
            return {'message': f'Carrier {carrier} not found in our system.'}, 400
        return {'location': list(CarrierCharge.map[carrier].keys())}
