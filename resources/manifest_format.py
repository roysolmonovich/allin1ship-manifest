from flask_restful import Resource
from models.manifest import ManifestFormatModel
from schemas.manifest import ManifestFormatSchema
from flask import request
from flask_jwt_extended import jwt_required

manifest_format_schema = ManifestFormatSchema()

class ManifestFormat(Resource):
    # @jwt_required()
    def get(self):
        args = request.args
        platform = args.get('platform')
        if not platform:
            return {'message': 'platform field missing.'}, 400
        platformat = ManifestFormatModel.find_platformat(platform)
        if not platformat:
            return {'message': f'"{platform}" not found.'}, 400
        description = "A new column name field can be added at the end of an existing list for the app to recognize in future manifests. All fields are case sensitive."
        service_msg = "Before updating a service field, check first if there is a 'header_alt' header. If the field contains both service prodiver (e.g. 'Fedex') and service name (e.g. 'Ground') in the same column, add it to the regular 'header'. 'header_alt' is used if service provider and service name are not found in one column. In that case, they might be found in separate service provider and service name columns. For service provider only, enter field into the first header_alt list, and for service name only, enter field into the second header_alt list."
        address_msg = "Before updating an address field, check first if there are separate 'address' and 'address_country' fields. In that case, the platform normally only includes separate zip code and country columns, and not the full address. For zip code columns, enter field into the address header, and for country name or country code, enter field into the address_country header."
        return {'format': platformat, 'description': description, 'service_message': service_msg, 'address_message': address_msg}

    def post(self):
        args = request.form
        errors = manifest_format_schema.validate(args)
        if errors:
            return errors, 400
        # return 'ok'
        (platform, field, header, value, index) = (args.get(_)
                                                   for _ in ('platform', 'field', 'header', 'value', 'index'))
        if field not in ManifestFormatModel.format:
            return {'message': f"'{field}' field not found in format."}, 400
        if platform not in ManifestFormatModel.format[field]:
            return {'message': f"'{platform}' platform not found in format."}, 400
        if header not in ManifestFormatModel.format[field][platform]:
            return {'message': f"'{header}' header not found for '{platform}' platform."}, 400
        if len(value) > 45:
            return {'message': 'value length cannot exceed 45.'}, 400
        if len(value) < 2:
            return {'message': 'value length must exceed 1'}, 400
        starting_letter_or_num, contains_letter = False, False
        if value[0].isalpha():
            starting_letter_or_num, contains_letter = True, True
        elif value[0].isnumeric():
            starting_letter_or_num = True
            for char in value[1:]:
                if char.isalpha():
                    contains_letter = True
                    break
            if not contains_letter:
                return {'message': 'value must contain at least one letter.'}, 400
        if not starting_letter_or_num or not contains_letter:
            return {'message': 'value has to begin with an alphanumeric character.'}, 400
        if header == 'header_alt':
            if index is None:
                return {'message': "index is required for 'header_alt' header."}, 400
            index = int(index)
            if index >= len(ManifestFormatModel.format[field][platform][header]) or index < 0:
                return {'message': "Array index out of range."}, 400
            if value in ManifestFormatModel.format[field][platform][header][index]:
                return {'message': f"'{value}' value already exists for header '{header}' within {platform}.{field}.{index}"}, 400
        elif value in ManifestFormatModel.format[field][platform][header]:
            return {'message': f"'{value}' value already exists for header '{header}' within {platform}.{field}."}, 400
        ManifestFormatModel.add_format_fields(platform, field, header, value, index)
        if header == 'header_alt':
            return {'message': f"'{value}' value successfully added to header '{header}' within {platform}.{field}.{index}"}
        else:
            return {'message': f"'{value}' value successfully added to header '{header}' within {platform}.{field}."}