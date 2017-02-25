import os

from sqlalchemy import Binary
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

engine = create_engine('sqlite:///:memory:', echo=True)

from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String

class FileInformation(Base):
     __tablename__ = 'files'

     id = Column(Integer, primary_key=True)
     path = Column(String, nullable=False)
     bytesize = Column(Integer, nullable=False)
     checksum = Column(String)
     first_block = Column(Binary)

     def __repr__(self):
        return "<File(size={size}, path={path})>".format(
            size=self.bytesize, path=self.path
        )

if __name__ == '__main__':
    Base.metadata.create_all(engine)
    from sqlalchemy.orm import sessionmaker
    Session = sessionmaker(bind=engine)
    session = Session()

    sample_file_path = '/etc/fstab'
    sample_file = FileInformation(path=sample_file_path,
                                  bytesize=os.path.getsize(sample_file_path))
    session.add(sample_file)
    session.commit()

    queried_file = session.query(FileInformation)\
                          .filter_by(path=sample_file_path).first()
    print(queried_file)

