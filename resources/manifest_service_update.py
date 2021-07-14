from flask_restful import Resource
from models.manifest import ManifestModel
from schemas.manifest import ManifestServiceUpdateSchema
from flask import request
from flask_jwt_extended import jwt_required

manifest_service_update_schema = ManifestServiceUpdateSchema()

class ManifestServiceUpdate(Resource):
    # @jwt_required()
    def post(self):
        errors = manifest_service_update_schema.validate(request.form)
        if errors:
            return errors, 400
        if ManifestModel.update_services('post', **request.form) is 0:
            return {'message': 'Service parameters already exist.'}, 400
        return {'message': 'Service successfully inserted.'}

    # @jwt_required()
    def put(self):
        errors = manifest_service_update_schema.validate(request.form)
        if errors:
            return errors, 400
        if ManifestModel.update_services('put', **request.form) is 0:
            return {'message': 'An error occured.'}, 400
        return {'message': 'Service successfully updated.'}