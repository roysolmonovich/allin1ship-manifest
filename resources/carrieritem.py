from flask import request
from flask_jwt_extended.view_decorators import jwt_required
from app_lib import CarrierCharge
from flask_restful import Resource


class CarrierItem(Resource):
    @jwt_required()
    def get(self):
        request_data = request.get_json()
        if not request_data:
            return {'message': 'you must include a body in this request'}
        if 'carrier' not in request_data:
            return {'message': 'Carrier object is missing.'}, 400
        carrier = request_data['carrier']
        if carrier not in CarrierCharge.map.keys():
            return {'message': f'Carrier {carrier} not found in our system.'}, 400
        return {'location': list(CarrierCharge.map[carrier].keys())}
