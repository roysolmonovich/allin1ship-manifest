# from sqlalchemy import create_engine, select, MetaData, Table
from argon2 import PasswordHasher, exceptions
from flask_restful import Resource, reqparse
from models.user import UserModel
from flask_jwt import jwt_required


class User(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument(
        'username',
        type=str,
        required=True,
        help="This field cannot be blank."
    )
    parser.add_argument(
        'password',
        type=str,
        required=True,
        help="This field cannot be blank."
    )
    ph = PasswordHasher()

    def post(self):
        data = User.parser.parse_args()
        existing = UserModel.find_by_username(username=data['username'])
        if existing:
            return {'message': f'Username {data["username"]} already taken.'}, 400
        if len(data['username']) > 45:
            return {'message': f'Invalid username - cannot exceed 45 characters.'}, 400
        user = UserModel(**data)
        print(user, user.username, user.hashed_pw)
        user.save_to_db()
        return {'message': f'User {data["username"]} added successfully.'}, 201

    @jwt_required()
    def delete(self):
        data = User.parser.parse_args()
        existing = UserModel.find_by_username(username=data['username'])
        if not existing:
            return {'message': f'Username {data["username"]} not found.'}, 400
        try:
            User.ph.verify(existing.hashed_pw, data['password'])
        except exceptions.VerifyMismatchError:
            return {'message': 'Wrong password entered.'}, 400
        existing.delete_from_db()
        return{'message': f'User {data["username"]} deleted successfully.'}, 200


# user1 = User.find_by_username('rsolmonovich')
# user1 = User.add_user('rsolmonovich', '5392295Ai1S')
# print(user1, len(user1))
