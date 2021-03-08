from passlib.hash import argon2
from db import db

# engine = create_engine('mysql+mysqlconnector://allinoy4_user0:+3mp0r@ry@162.241.219.134:3306/allinoy4_allin1ship', pool_pre_ping=True)
# metadata = MetaData(bind=None)
# table = Table(
#     'users',
#     metadata,
#     autoload=True,
#     autoload_with=engine
# )
# conn = engine.connect()


class UserModel(db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(45))
    hashed_pw = db.Column(db.String(87))
    # engine = create_engine('mysql+mysqlconnector://allinoy4_user0:+3mp0r@ry@162.241.219.134:3306/allinoy4_allin1ship')
    # metadata = MetaData(bind=None)
    # table = Table(
    #     'users',
    #     metadata,
    #     autoload=True,
    #     autoload_with=engine
    # )
    # conn = engine.connect()

    def __init__(self, username, password):
        self.username = username
        self.hashed_pw = argon2.hash(password)

    @classmethod
    def find_by_username(cls, username):
        # stmt = select([table.columns.username, table.columns.hashed_pw, table.columns.id]).where(table.columns.username == username)
        # conn = engine.connect()
        # res = conn.execute(stmt).fetchone()
        # conn.close()
        # if res:
        #     user = cls(*res)
        # else:
        #     user = None
        # return user
        return cls.query.filter_by(username=username).first()

    @classmethod
    def find_by_user_id(cls, _id):
        # stmt = select([table.columns.username, table.columns.hashed_pw, table.columns.id]).where(table.columns.id == _id)
        # conn = engine.connect()
        # res = conn.execute(stmt).fetchone()
        # conn.close()
        # if res:
        #     user = cls(*res)
        # else:
        #     user = None
        # return user
        return cls.query.filter_by(id=_id).first()

    def save_to_db(self):
        db.session.add(self)
        db.session.commit()

    def delete_from_db(self):
        db.session.delete(self)
        db.session.commit()
