from flask_restful import Resource
from models.manifest import ManifestModel, ManifestDataModel, ManifestMissingModel
from flask import request
from flask_jwt_extended import jwt_required

class ManifestFilter(Resource):
    @jwt_required()
    def post(self):
        request_data = request.get_json()
        print(request_data.keys())
        if 'name' not in request_data:
            return {'message': 'No name chosen'}, 400
        manifest = ManifestModel.find_by_name(name=request_data['name'])
        if not manifest:
            return {'message': 'Name not found in system'}, 400
        if 'filters' not in request_data:
            return {'message': 'No filters selected'}, 400
        filters = request_data['filters']
        id = manifest.id
        print(filters)
        missing_columns = ManifestMissingModel.json(_id=id)
        service_replacements = {}
        existing_headers_ordered = [v if v
                                    not in missing_columns else v+'_gen' for v in ManifestModel.ai1s_headers_ordered]
        for service_override in filters.get('services', []):
            if service_override.get('service') is not None:
                service_replacements[(service_override['service name'], service_override['location'],
                                      '>=' if service_override['weight threshold'][:5] == 'Over ' else '<')] = service_override['service']
        shipments = []
        page = request_data.get('page', 1)
        per_page = request_data.get('per_page', 20)
        include_loss = request_data.get('include_loss', True)
        filter_queries = ManifestDataModel.filtered_query_builder(id, filters)
        send_report = request_data.get('send_report', True)
        print('\n\nhere\n\n')
        print(filter_queries)
        response = {}
        if send_report:
            response['Report'] = ManifestDataModel.shipment_report(
                filter_query=filter_queries[1], service_replacements=service_replacements, include_loss=include_loss)
        if per_page != 0:
            paginated_result = ManifestDataModel.find_filtered_shipments(filter_queries[0], page, per_page)
            for shipment_item in paginated_result.items:
                if (shipment_item.service, shipment_item.country, shipment_item.weight_threshold) in service_replacements:
                    print('here', shipment_item.service, shipment_item.country, shipment_item.weight_threshold)
                    shipment_item = shipment_item.correct_service_rates(service_replacements[(
                        shipment_item.service, shipment_item.country, shipment_item.weight_threshold)])
                shipment = shipment_item.__dict__
                print(shipment)
                del shipment['_sa_instance_state']
                shipment['shipdate'] = str(shipment['shipdate'])
                for missing_column in missing_columns:
                    shipment[missing_column+'_gen'] = shipment.pop(missing_column)
                shipments.append(shipment)
            response.update({'ordered headers': existing_headers_ordered,
                             'filtered shipments': shipments, 'curr_page': paginated_result.page,
                             'has_prev': paginated_result.has_prev, 'has_next': paginated_result.has_next,
                             'pages': paginated_result.pages, 'total': paginated_result.total})
        return response
        # shipments = ManifestModel.manifest_shipments(_id=id, filter=None)
        # print(id)
        # print(shipments)
        # for shipment in shipments:
        #     print(shipment.shipdate)
        # df = pd.read_sql(query.statement, query.session.bind)
        # return request_data
