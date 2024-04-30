import unittest
from unittest.mock import MagicMock

from aodn_cloud_optimised.lib.s3Tools import s3_ls


class TestS3Ls(unittest.TestCase):
    def test_s3_ls(self):
        # Mocking boto3 client
        boto3_client_mock = MagicMock()
        boto3_client_mock.get_paginator.return_value.paginate.return_value = [
            {'Contents': [{'Key': 'prefix/file1.nc'}, {'Key': 'prefix/file2.nc'}, {'Key': 'prefix/file3.txt'}]}
        ]

        with unittest.mock.patch('boto3.client', return_value=boto3_client_mock):
            # Call the function
            result = s3_ls('test-bucket', 'prefix')

            # Assert the result
            self.assertEqual(result, ['prefix/file1.nc', 'prefix/file2.nc'])


if __name__ == '__main__':
    unittest.main()