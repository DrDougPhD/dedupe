from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

engine = create_engine('sqlite:///:memory:', echo=True)

from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String

class User(Base):
     __tablename__ = 'users'

     id = Column(Integer, primary_key=True)
     name = Column(String)
     fullname = Column(String)
     password = Column(String)

     def __repr__(self):
        return "<User(name='%s', fullname='%s', password='%s')>" % (
                             self.name, self.fullname, self.password)

if __name__ == '__main__':
    from sqlalchemy.orm import sessionmaker
    Session = sessionmaker(bind=engine)
    session = Session()

    ed_user = User(name='ed', fullname='Ed Jones', password='edspassword')
    session.add(ed_user)

    session.add_all([])

    our_user = session.query(User).filter_by(name='ed').first()

    session.commit()