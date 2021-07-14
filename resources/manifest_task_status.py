from utils.context import RequestContext
from flask_jwt_extended.view_decorators import jwt_required
from flask_restful import Resource


# Not in use - could be used for celery app to make function calls async
class ManifestTaskStatus(Resource):
    # @jwt_required()
    def get(self):
        task_id = RequestContext.args.get('task_id')
        print(self)
        print(task_id)
        task = get_task.AsyncResult(task_id)
        if task.state == 'PENDING':
            # job did not start yet
            response = {
                'state': task.state,
                'current': 0,
                'total': 1,
                'status': 'Pending...'
            }
        elif task.state != 'FAILURE':
            response = {
                'state': task.state,
                'current': task.info.get('current', 0),
                'total': task.info.get('total', 1),
                'status': task.info.get('status', '')
            }
            if 'result' in task.info:
                response['result'] = task.info['result']
        else:
            # something went wrong in the background job
            response = {
                'state': task.state,
                'current': 1,
                'total': 1,
                'status': str(task.info),  # this is the exception raised
            }
        return response
