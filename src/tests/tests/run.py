import os
import random
import string
import unittest
import logging
logger = logging.getLogger(__name__)
print(__name__)
import dedupe
from src.filesystem import FileFinder


class FileFinderTest(unittest.TestCase):
    test_samples_directory = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'samples'
    )

    # file sizes
    sample_filesize = 1000
    sample_random_string = ''.join(
        random.choice(string.ascii_uppercase + string.digits)
        for _ in range(sample_filesize))

    # single file directory
    single_sample_directory = os.path.join(test_samples_directory, 'single')
    sample_filepath = os.path.join(single_sample_directory,
                                   'sample.txt')

    @classmethod
    def setUpClass(cls):
        os.makedirs(FileFinderTest.single_sample_directory, exist_ok=True)

        # create a random string file
        with open(FileFinderTest.sample_filepath, 'w') as f:
            f.write(FileFinderTest.sample_random_string)

    def setUp(self):
        pass

    def test_finding_sample_file(self):
        finder = FileFinder(within=FileFinderTest.single_sample_directory)

        # finder should only find one path in the specified directory
        found_file = next(finder._next_filepath())

        self.assertEqual(str(found_file), FileFinderTest.sample_filepath)


def setup_logger():
    logger.setLevel(logging.DEBUG)
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter and add it to the handlers
    ch.setFormatter(logging.Formatter(
        "%(levelname)s [%(filename)s:%(lineno)s - %(funcName)20s() ]"
        " %(message)s"
    ))

    # add the handlers to the logger
    logger.addHandler(ch)


if __name__ == '__main__':
    unittest.main()