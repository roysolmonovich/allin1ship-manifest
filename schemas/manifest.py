from marshmallow import Schema, fields


class ManifestSchema(Schema):
    name = fields.Str(required=True)


class ManifestUpdateSchema(Schema):
    old_name = fields.Str(required=True)
    new_name = fields.Str(required=True)
