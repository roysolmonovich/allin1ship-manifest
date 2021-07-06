from flask_restful import Resource
from models.manifest import ManifestModel
from flask_jwt_extended import jwt_required


class ManifestColumns(Resource):
    @jwt_required()
    def get(self):
        return {'headers': sorted(list(ManifestModel.ai1s_headers_required))}