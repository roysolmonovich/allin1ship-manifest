from models.user import UserModel
from passlib.hash import argon2


def authenticate(username, password):
    user = UserModel.find_by_username(username)
    if not user:
        print(f'User {username} not found.')
        return
    if not argon2.verify(password, user.hashed_pw):
        print('Wrong password entered.')
        return
    return user


def identity(payload):
    user_id = payload['identity']
    return UserModel.find_by_user_id(user_id)
# user1 = User.find_by_username('rsolmonovich')
# print(user1.password, len(user1.password))
