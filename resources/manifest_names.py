from flask_restful import Resource
from models.manifest import ManifestModel
from schemas.manifest import ManifestSchema, ManifestUpdateSchema
from flask import request
from flask_jwt_extended import jwt_required

manifest_update_schema = ManifestUpdateSchema()
manifest_schema = ManifestSchema()

class ManifestNames(Resource):
    # @jwt_required()
    def get(self):
        all_manifests = ManifestModel.find_all()
        json_manifests = {manifest.name: str(manifest.init_time)[:10] for manifest in all_manifests}
        return json_manifests
    # @jwt_required()
    def put(self):
        args = request.args
        print(args.keys())
        errors = manifest_update_schema.validate(request.args)
        if errors:
            return errors, 400
        old_name = args['old_name']
        new_name = args['new_name']
        existing = ManifestModel.find_by_name(name=old_name)
        if not existing:
            return {'message': f'Old name {old_name} not found in system'}, 400
        if new_name == old_name:
            return {'message': f'Old name {old_name} has to be different than new name.'}, 400
        check_new = ManifestModel.find_by_name(name=new_name)
        if check_new:
            return {'message': f'New name {new_name} already taken.'}, 400
        existing.name = new_name
        existing.save_to_db()
        return {'message': 'Name successfully updated.'}
    
    # @jwt_required()
    def delete(self):
        args = request.args
        print(args.keys())
        errors = manifest_schema.validate(request.args)
        if errors:
            return errors, 400
        name = args['name']
        existing = ManifestModel.find_by_name(name=name)
        if not existing:
            return {'message': f'Name {name} not found in system.'}, 400
        existing.delete_from_db()
        return {'message': f'Name {name} successfully deleted.'}