from argon2 import PasswordHasher, exceptions
from flask_restful import Resource, reqparse
from models.user import UserModel
from flask_jwt_extended import create_access_token, create_refresh_token, jwt_required, get_jwt_identity, get_jwt
from blocklist import BLOCKLIST

ph = PasswordHasher()


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
    parser.add_argument(
        'new_password',
        type=str,
    )

    # @jwt_required() # lechoureh, you shouldn't need a jwt to register, but I don't want anyone to register right now.
    def post(self): # endpoint to register a new user
        data = User.parser.parse_args()
        existing = UserModel.find_by_username(username=data['username'])
        if existing:
            return {'message': f'Username {data["username"]} already taken.'}, 400
        if len(data['username']) > 45:
            return {'message': 'Invalid username - cannot exceed 45 characters.'}, 400
        if len(data['password']) < 8:
            return {'message': 'Password has to contain at least 8 characters and at least 1 uppercase and 1 lowercase letters.'}, 400
        upper, lower = False, False
        for char in data['password']:
            upper = True if char.isupper() else upper
            lower = True if char.islower() else lower
        if not (upper and lower):
            return {'message': 'Password has to contain at least 8 characters and at least 1 uppercase and 1 lowercase letters.'}, 400
        user = UserModel(data['username'], ph.hash(data['password']))
        print(user, user.username, user.hashed_pw)
        user.save_to_db()
        return {'message': f'User {data["username"]} added successfully.'}, 201

    # @jwt_required()
    def put(self):
        data = User.parser.parse_args()
        print(data)
        existing = UserModel.find_by_username(username=data['username'])
        if not existing:
            return {'message': f'Username {data["username"]} not found.'}, 400
        try:
            ph.verify(existing.hashed_pw, data['password'])
        except exceptions.VerifyMismatchError:
            return {'message': 'Wrong password entered.'}, 400
        if len(data['new_password']) < 8:
            return {'message': 'Password has to contain at least 8 characters and at least 1 uppercase and 1 lowercase letters.'}, 400
        upper, lower = False, False
        for char in data['new_password']:
            upper = True if char.isupper() else upper
            lower = True if char.islower() else lower
        if not (upper and lower):
            return {'message': 'Password has to contain at least 8 characters and at least 1 uppercase and 1 lowercase letters.'}, 400
        existing.hashed_pw = ph.hash(data['new_password'])
        existing.save_to_db()
        return {'message': 'Password changed successfully.'}, 201

    # @jwt_required()
    def delete(self):
        data = User.parser.parse_args()
        existing = UserModel.find_by_username(username=data['username'])
        if not existing:
            return {'message': f'Username {data["username"]} not found.'}, 400
        try:
            ph.verify(existing.hashed_pw, data['password'])
        except exceptions.VerifyMismatchError:
            return {'message': 'Wrong password entered.'}, 400
        existing.delete_from_db()
        return{'message': f'User {data["username"]} deleted successfully.'}, 200


class UserLogin(Resource):
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

    @classmethod
    def post(cls):
        data = cls.parser.parse_args()
        user = UserModel.find_by_username(data['username'])
        if user:
            try:
                ph.verify(user.hashed_pw, data['password'])
                access_token = create_access_token(identity=user.id, fresh=True)
                refresh_token = create_refresh_token(user.id)
                return {'access_token': access_token, 'refresh_token': refresh_token}
            except exceptions.VerifyMismatchError:
                return {'message': 'Wrong password entered.'}, 401
        return {'message': 'Username not found'}, 401


class TokenRefresh(Resource):
    # @jwt_required(refresh=True)
    def post(self):
        current_user = get_jwt_identity()
        new_token = create_access_token(identity=current_user, fresh=False)
        return {'access_token': new_token}


class UserLogout(Resource):
    # @jwt_required()
    def post(self):
        jti = get_jwt()['jti']  # jti is JWT ID - the unique JWT identifier
        BLOCKLIST.add(jti)
        return {'message': 'Successfully logged out.'}, 200
