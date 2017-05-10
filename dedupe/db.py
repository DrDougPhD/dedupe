import os

import progressbar
from sqlalchemy import BigInteger
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

    engine = create_engine('sqlite:///{}'.format(db_filepath), echo=False)
    if not os.path.exists(into):
        logger.debug("{} doesn't exist, and will be initialized".format(into))
        Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    file_count = sum([len(files_matching_size)
                      for files_matching_size in filesizes.values()])
    bar = progressbar.ProgressBar(max_value=file_count)

    i = 0
    for filesize, files_matching_size in filesizes.items():
        logger.debug('Adding {0}-byte files ({1} files)'.format(
            filesize,
            len(files_matching_size)))

        for file in files_matching_size:
            possibly_non_existent_record = session.query(FileInformation)\
                                                  .filter_by(path=file.path)\
                                                  .first()

            if possibly_non_existent_record is None:
                session.add(FileInformation(path=file.path, bytesize=filesize))

            else:
                if possibly_non_existent_record.bytesize != filesize:
                    possibly_non_existent_record.bytesize = filesize

            if i % 2000 == 0:
                session.commit()

            bar.update(i)
            i += 1

    session.commit()
    return session

def update_with_checksums(partitions, db):
    for checksum, files in partitions.items():
        for f in files:
            current_record = db.query(FileInformation) \
                               .filter_by(path=f.path) \
                               .first()
            current_record.checksum = checksum

    db.commit()


class FileInformation(Base):
     __tablename__ = 'files'

     path = Column(String, primary_key=True, nullable=False)
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
    print('File found: {}'.format(queried_file))

    non_existing_file = session.query(FileInformation) \
                               .filter_by(path='/file/doesnt/exist').first()
    print("This file doesn't exist: {}".format(non_existing_file))

    print(queried_file)
