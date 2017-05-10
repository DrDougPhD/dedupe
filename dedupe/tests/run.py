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

    # two file directory, same size
    twin_file_sample_directory = os.path.join(test_samples_directory,
                                           'double_same_size')
    twin_1_filepath = os.path.join(twin_file_sample_directory,
                                   'sample1.txt')
    twin_2_filepath = os.path.join(twin_file_sample_directory,
                                   'sample2.txt')
    twin_files = [twin_1_filepath, twin_2_filepath]

    # two file directory, different size


    @classmethod
    def setUpClass(cls):
        os.makedirs(FileFinderTest.single_sample_directory,
                    exist_ok=True)
        os.makedirs(FileFinderTest.twin_file_sample_directory,
                    exist_ok=True)

        # create a random string file
        with open(FileFinderTest.sample_filepath, 'w') as f:
            f.write(FileFinderTest.sample_random_string)

        for twin_file in FileFinderTest.twin_files:
            with open(twin_file, 'w') as f:
                f.write(FileFinderTest.sample_random_string)

    def setUp(self):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def test_finding_sample_file(self):
        finder = FileFinder(within=FileFinderTest.single_sample_directory)

        # finder should only find one path in the specified directory
        found_file = next(finder._next_filepath())

        self.assertEqual(str(found_file), FileFinderTest.sample_filepath)

    def test_finding_sample_file_size(self):
        logger.debug('='*100)
        logger.debug('Verifying file size returned for one file')
        finder = FileFinder(within=FileFinderTest.single_sample_directory)

        # finder should only find one path in the specified directory
        found_file = finder.find()
        found_file_size = list(found_file.keys())[0]
        found_file_path = list(found_file.values())[0][0]

        self.assertEqual(found_file_path, FileFinderTest.sample_filepath)
        self.assertEqual(found_file_size,
                         os.path.getsize(FileFinderTest.sample_filepath))

    def test_find_two_files(self):
        logger.debug('='*100)
        logger.debug('Verifying two files are found')
        finder = FileFinder(within=FileFinderTest.twin_file_sample_directory)

        # finder should only find one path in the specified directory
        found_files = []
        found_files.append(next(finder._next_filepath()))
        found_files.append(next(finder._next_filepath()))

        for f in found_files:
            self.assertTrue(f in FileFinderTest.twin_files)


    @unittest.skip("functionality not yet implemented")
    def test_integration_of_all_parts(self):
        finder = FileFinder(within=FileFinderTest.test_samples_directory)
        files_and_sizes = finder.find()

        # assert returned object is a dictionary
        self.assertIsInstance(files_and_sizes, dict, msg='Returned value is '
                                                         'not a dictionary')

        # assert keys are integers
        [self.assertIsInstance(key, int, msg='Keys returned by finder are '
                                             'not integers')
          for key in files_and_sizes]

        # assert for each key a list is returned
        [self.assertIsInstance(value, list, msg='Values returned are not '
                                                'lists')
         for _, value in files_and_sizes.items()]

        # assert for each file associated with a filesize key that the file
        # is actually the size specified by the key
        for key, files in files_and_sizes:
            for f in files:
                actual_filesize = os.path.getsize(f)
                self.assertEqual(key, actual_filesize,
                                 msg='File is not of size {0} bytes. Actual '
                                     'size: {1} bytes. File: "{2}"'.format(
                                        key, actual_filesize, key))


if __name__ == '__main__':
    unittest.main()