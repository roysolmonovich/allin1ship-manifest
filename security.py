from models.user import UserModel
from argon2 import PasswordHasher, exceptions
ph = PasswordHasher()


def authenticate(username, password):
    print('here')
    user = UserModel.find_by_username(username)
    if not user:
        print(f'User {username} not found.')
        return
    try:
        ph.verify(user.hashed_pw, password)
        return user
    except exceptions.VerifyMismatchError:
        print('Wrong password entered.')
        return


def identity(payload):
    user_id = payload['identity']
    return UserModel.find_by_user_id(user_id)
# user1 = User.find_by_username('rsolmonovich')
# print(user1.password, len(user1.password))
