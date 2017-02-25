import os

from sqlalchemy import Binary
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String

import logging
logger = logging.getLogger(__name__)

def insert_files(filesizes, into):
    db_filepath = os.path.abspath(into)
    logger.debug('.'*80)
    logger.debug('Adding file and bytesizes to database')

    engine = create_engine('sqlite:///{}'.format(db_filepath), echo=True)
    if not os.path.exists(into):
        logger.debug("{} doesn't exist, and will be initialized".format(into))
        Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    for filesize in filesizes:
        logger.debug('Adding {}-byte files'.format(filesize))
        session.add_all([
            FileInformation(path=filepath, bytesize=filesize)
            for filepath in filesizes[filesize]
        ])

    session.commit()
    return session


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
    engine = create_engine('sqlite:///:memory:', echo=True)
    Base.metadata.create_all(engine)
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

