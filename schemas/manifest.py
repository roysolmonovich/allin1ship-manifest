from marshmallow import Schema, fields


class ManifestSchema(Schema):
    name = fields.Str(required=True)


class ManifestUpdateSchema(Schema):
    old_name = fields.Str(required=True)
    new_name = fields.Str(required=True)


class ManifestFormatSchema(Schema):
    platform = fields.Str(required=True)
    field = fields.Str(required=True)
    header = fields.Str(required=True)
    value = fields.Str(required=True)
    index = fields.Int(required=False)


class ManifestServiceUpdateSchema(Schema):
    service = fields.Str(required=True)
    country = fields.Str(required=True)
    weight_threshold = fields.Str(required=True)
    sugg_service = fields.Str(required=True)
