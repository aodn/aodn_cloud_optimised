import json
import os
import unittest

import boto3
import pandas as pd
import pyarrow as pa
import s3fs
from moto import mock_aws
from moto.moto_server.threaded_moto_server import ThreadedMotoServer
from shapely import wkb
from shapely.geometry import Polygon

from aodn_cloud_optimised.lib.GenericParquetHandler import GenericHandler
from aodn_cloud_optimised.lib.config import load_dataset_config
from aodn_cloud_optimised.lib.s3Tools import s3_ls
from unittest.mock import patch

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# Specify the filename relative to the current directory
TEST_FILE_NC_ANMN = os.path.join(
    ROOT_DIR,
    "resources",
    "IMOS_ANMN-NSW_CDSTZ_20210429T015500Z_SYD140_FV01_SYD140-2104-SBE37SM-RS232-128_END-20210812T011500Z_C-20210827T074819Z.nc",
)

TEST_FILE_NC_ARDC = os.path.join(
    ROOT_DIR, "resources", "BOM_20240301_CAPE-SORELL_RT_WAVE-PARAMETERS_monthly.nc"
)

TEST_FILE_NC_SOOP_SST = os.path.join(
    ROOT_DIR,
    "resources",
    "IMOS_SOOP-SST_MT_20110101T000000Z_9HA2479_FV01_C-20120528T071958Z.nc",
)

DUMMY_FILE = os.path.join(ROOT_DIR, "resources", "DUMMY.nan")
DUMMY_NC_FILE = os.path.join(ROOT_DIR, "resources", "DUMMY.nc")
TEST_CSV_FILE = os.path.join(
    ROOT_DIR, "resources", "A69-1105-135_107799906_130722039.csv"
)
DATASET_CONFIG_CSV_AATAMS_JSON = os.path.join(
    ROOT_DIR, "resources", "receiver_animal_acoustic_tagging_delayed_qc.json"
)

DATASET_CONFIG_NC_ANMN_JSON = os.path.join(
    ROOT_DIR, "resources", "anmn_ctd_ts_fv01.json"
)

DATASET_CONFIG_NC_ARDC_JSON = os.path.join(
    ROOT_DIR, "resources", "wave_buoy_realtime_nonqc.json"
)

DATASET_CONFIG_NC_SOOP_SST_JSON = os.path.join(
    ROOT_DIR, "resources", "vessel_sst_delayed_qc.json"
)


@mock_aws
class TestGenericHandler(unittest.TestCase):
    def setUp(self):

        # Create a mock S3 service
        self.BUCKET_OPTIMISED_NAME = "imos-data-lab-optimised"
        self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH = "testing"
        self.s3 = boto3.client("s3", region_name="us-east-1")
        self.s3.create_bucket(Bucket="imos-data")
        self.s3.create_bucket(Bucket=self.BUCKET_OPTIMISED_NAME)

        # create moto server; needed for s3fs and parquet
        self.server = ThreadedMotoServer(ip_address="127.0.0.1", port=5555)

        self.s3_fs = s3fs.S3FileSystem(
            anon=False,
            client_kwargs={
                "endpoint_url": "http://127.0.0.1:5555/",
                "region_name": "us-east-1",
            },
        )

        self.server.start()

        # Make the "imos-data" bucket public
        public_policy_imos_data = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::imos-data/*",
                }
            ],
        }

        public_policy_cloud_optimised_data = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{self.BUCKET_OPTIMISED_NAME}/*",
                }
            ],
        }

        self.s3.put_bucket_policy(
            Bucket="imos-data", Policy=json.dumps(public_policy_imos_data)
        )

        self.s3.put_bucket_policy(
            Bucket=self.BUCKET_OPTIMISED_NAME,
            Policy=json.dumps(public_policy_cloud_optimised_data),
        )

        # Copy files to the mock S3 bucket
        self.s3.put_object(
            Bucket=self.BUCKET_OPTIMISED_NAME, Key="testing", Body=""
        )  # empty file
        self._upload_to_s3(
            "imos-data",
            f"good_nc_anmn/{os.path.basename(TEST_FILE_NC_ANMN)}",
            TEST_FILE_NC_ANMN,
        )
        self._upload_to_s3(
            "imos-data", f"dummy/{os.path.basename(DUMMY_FILE)}", DUMMY_FILE
        )
        self._upload_to_s3(
            "imos-data", f"dummy_nc/{os.path.basename(DUMMY_NC_FILE)}", DUMMY_NC_FILE
        )
        self._upload_to_s3(
            "imos-data", f"good_csv/{os.path.basename(TEST_CSV_FILE)}", TEST_CSV_FILE
        )
        self._upload_to_s3(
            "imos-data",
            f"good_nc_ardc/{os.path.basename(TEST_FILE_NC_ARDC)}",
            TEST_FILE_NC_ARDC,
        )

        self._upload_to_s3(
            "imos-data",
            f"good_nc_soop_sst/{os.path.basename(TEST_FILE_NC_SOOP_SST)}",
            TEST_FILE_NC_SOOP_SST,
        )

        dataset_anmn_netcdf_config = load_dataset_config(DATASET_CONFIG_NC_ANMN_JSON)
        self.handler_nc_anmn_file = GenericHandler(
            optimised_bucket_name=self.BUCKET_OPTIMISED_NAME,
            root_prefix_cloud_optimised_path=self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH,
            dataset_config=dataset_anmn_netcdf_config,
            clear_existing_data=True,
            force_previous_parquet_deletion=True,
            cluster_mode="local",
        )

        dataset_ardc_netcdf_config = load_dataset_config(DATASET_CONFIG_NC_ARDC_JSON)
        self.handler_nc_ardc_file = GenericHandler(
            optimised_bucket_name=self.BUCKET_OPTIMISED_NAME,
            root_prefix_cloud_optimised_path=self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH,
            dataset_config=dataset_ardc_netcdf_config,
            clear_existing_data=True,
            force_previous_parquet_deletion=True,
            cluster_mode="local",
        )

        dataset_soop_sst_netcdf_config = load_dataset_config(
            DATASET_CONFIG_NC_SOOP_SST_JSON
        )
        self.handler_nc_soop_sst_file = GenericHandler(
            optimised_bucket_name=self.BUCKET_OPTIMISED_NAME,
            root_prefix_cloud_optimised_path=self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH,
            dataset_config=dataset_soop_sst_netcdf_config,
            clear_existing_data=True,
            force_previous_parquet_deletion=True,
            cluster_mode="local",
        )

        dataset_aatams_csv_config = load_dataset_config(DATASET_CONFIG_CSV_AATAMS_JSON)
        self.handler_csv_file = GenericHandler(
            optimised_bucket_name=self.BUCKET_OPTIMISED_NAME,
            root_prefix_cloud_optimised_path=self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH,
            dataset_config=dataset_aatams_csv_config,
            clear_existing_data=True,
            cluster_mode="local",
        )

    def _upload_to_s3(self, bucket_name, key, file_path):
        with open(file_path, "rb") as f:
            self.s3.upload_fileobj(f, bucket_name, key)

    def tearDown(self):
        # Delete all objects and buckets in the mock S3
        bucket_list = self.s3.list_buckets()["Buckets"]
        for bucket in bucket_list:
            bucket_name = bucket["Name"]
            objects = self.s3.list_objects_v2(Bucket=bucket_name).get("Contents", [])
            for obj in objects:
                self.s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
            self.s3.delete_bucket(Bucket=bucket_name)

        self.server.stop()

    def test_parquet_nc_anmn_handler(self):
        nc_obj_ls = s3_ls("imos-data", "good_nc_anmn")

        # 1st pass
        with patch.object(self.handler_nc_anmn_file, "s3_fs", new=self.s3_fs):
            self.handler_nc_anmn_file.to_cloud_optimised([nc_obj_ls[0]])

        # 2nd pass, process the same file a second time. Should be deleted
        # TODO: Not a big big deal breaker, but got an issue which should be fixed in the try except only for the unittest
        #       2024-07-01 16:04:54,721 - INFO - GenericParquetHandler.py:824 - delete_existing_matching_parquet - No files to delete: GetFileInfo() yielded path 'imos-data-lab-optimised/testing/anmn_ctd_ts_fv01.parquet/site_code=SYD140/timestamp=1625097600/polygon=01030000000100000005000000000000000020624000000000008041C0000000000060634000000000008041C0000000000060634000000000000039C0000000000020624000000000000039C0000000000020624000000000008041C0/IMOS_ANMN-NSW_CDSTZ_20210429T015500Z_SYD140_FV01_SYD140-2104-SBE37SM-RS232-128_END-20210812T011500Z_C-20210827T074819Z.nc-0.parquet', which is outside base dir 's3://imos-data-lab-optimised/testing/anmn_ctd_ts_fv01.parquet/'
        with patch.object(self.handler_nc_anmn_file, "s3_fs", new=self.s3_fs):
            self.handler_nc_anmn_file.to_cloud_optimised_single(nc_obj_ls[0])

        # read parquet
        dataset_config = load_dataset_config(DATASET_CONFIG_NC_ANMN_JSON)
        dataset_name = dataset_config["dataset_name"]
        dname = f"s3://{self.BUCKET_OPTIMISED_NAME}/{self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH}/{dataset_name}.parquet/"

        parquet_dataset = pd.read_parquet(
            dname,
            engine="pyarrow",
            storage_options={
                "client_kwargs": {"endpoint_url": "http://127.0.0.1:5555"}
            },
        )

        self.assertNotIn("station_name", parquet_dataset.columns)
        self.assertAlmostEqual(parquet_dataset["TEMP"][0], 13.2773, delta=1e-2)

        # Check if the column are added with the correct values
        self.assertIn("site_code", parquet_dataset.columns)
        self.assertEqual(parquet_dataset["site_code"].iloc[0], "SYD140")

        self.assertEqual(
            parquet_dataset["filename"].iloc[0], os.path.basename(nc_obj_ls[0])
        )
        self.assertEqual(parquet_dataset["timestamp"].iloc[0], 1617235200.0)

        # The following section shows how the created polygon variable can be used to perform data queries. this adds significant overload, but is worth it
        parquet_dataset["converted_polygon"] = parquet_dataset["polygon"].apply(
            lambda x: wkb.loads(bytes.fromhex(x))
        )

        # Define the predefined polygon
        predefined_polygon_coords_out = [(150, -40), (155, -40), (155, -45), (150, -45)]
        predefined_polygon_coords_in = [(150, -32), (155, -32), (155, -45), (150, -45)]

        predefined_polygon_out = Polygon(predefined_polygon_coords_out)
        predefined_polygon_in = Polygon(predefined_polygon_coords_in)

        df_unique_polygon = parquet_dataset["converted_polygon"].unique()[0]
        self.assertFalse(df_unique_polygon.intersects(predefined_polygon_out))
        self.assertTrue(df_unique_polygon.intersects(predefined_polygon_in))

        # Testing the metadata sidecar file
        # Reading the metadata file of the dataset (at the root)
        parquet_meta_file_path = os.path.join(
            self.handler_nc_anmn_file.cloud_optimised_output_path, "_common_metadata"
        )
        parquet_meta = pa.parquet.read_schema(
            parquet_meta_file_path, filesystem=self.s3_fs
        )

        # horrible ... but got to be done. The dictionary of metadata has to be a dictionnary with byte keys and byte values.
        # meaning that we can't have nested dictionaries ...
        decoded_meta = {
            key.decode("utf-8"): json.loads(value.decode("utf-8").replace("'", '"'))
            for key, value in parquet_meta.metadata.items()
        }

        self.assertEqual(decoded_meta["LONGITUDE"]["axis"], "X")
        self.assertEqual(decoded_meta["NOMINAL_DEPTH"]["standard_name"], "depth")

        # alternative way to access the metadata
        # Create a dictionary where keys are the names and values are the elements
        schema_dict = {obj.name: obj for obj in parquet_meta}
        self.assertEqual(
            schema_dict["TEMP"].metadata.get(b"standard_name"), b"sea_water_temperature"
        )
        # other way to access the metadata
        schema_dict = {obj.name: obj.metadata for obj in parquet_meta}
        self.assertEqual(
            schema_dict["TEMP"][b"standard_name"], b"sea_water_temperature"
        )

    def test_parquet_nc_generic_handler_h5netcdf(self):
        # test with the h5netcdf engine
        nc_obj_ls = s3_ls("imos-data", "good_nc_ardc")

        # 1st pass
        with patch.object(self.handler_nc_ardc_file, "s3_fs", new=self.s3_fs):
            self.handler_nc_ardc_file.to_cloud_optimised([nc_obj_ls[0]])

        # 2nd pass, process the same file a second time. Should be deleted
        # TODO: Not a big big deal breaker, but got an issue which should be fixed in the try except only for the unittest
        #       2024-07-01 16:04:54,721 - INFO - GenericParquetHandler.py:824 - delete_existing_matching_parquet - No files to delete: GetFileInfo() yielded path 'imos-data-lab-optimised/testing/anmn_ctd_ts_fv01.parquet/site_code=SYD140/timestamp=1625097600/polygon=01030000000100000005000000000000000020624000000000008041C0000000000060634000000000008041C0000000000060634000000000000039C0000000000020624000000000000039C0000000000020624000000000008041C0/IMOS_ANMN-NSW_CDSTZ_20210429T015500Z_SYD140_FV01_SYD140-2104-SBE37SM-RS232-128_END-20210812T011500Z_C-20210827T074819Z.nc-0.parquet', which is outside base dir 's3://imos-data-lab-optimised/testing/anmn_ctd_ts_fv01.parquet/'
        with patch.object(self.handler_nc_ardc_file, "s3_fs", new=self.s3_fs):
            self.handler_nc_ardc_file.to_cloud_optimised_single(nc_obj_ls[0])

        # read parquet
        dataset_config = load_dataset_config(DATASET_CONFIG_NC_ARDC_JSON)
        dataset_name = dataset_config["dataset_name"]
        dname = f"s3://{self.BUCKET_OPTIMISED_NAME}/{self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH}/{dataset_name}.parquet/"

        parquet_dataset = pd.read_parquet(
            dname,
            engine="pyarrow",
            storage_options={
                "client_kwargs": {"endpoint_url": "http://127.0.0.1:5555"}
            },
        )

        self.assertEqual(parquet_dataset["timestamp"][0], 1709251200.0)

        self.assertNotIn(
            "DUMMY_VAR_NOT_IN", parquet_dataset.columns
        )  # make sure the variable is removed
        self.assertNotIn(
            "WHTH", parquet_dataset.columns
        )  # removed on purpose to trigger "missing variable from provided pyarrow_schema config, please add to dataset config"
        self.assertIn("WPMH", parquet_dataset.columns)

        self.assertEqual(parquet_dataset["timestamp"][0], 1709251200.0)
        self.assertEqual(
            parquet_dataset["TIME"][0], pd.Timestamp("2024-03-01 01:30:00")
        )

        parquet_meta_file_path = os.path.join(
            self.handler_nc_ardc_file.cloud_optimised_output_path, "_common_metadata"
        )
        parquet_meta = pa.parquet.read_schema(
            parquet_meta_file_path, filesystem=self.s3_fs
        )

        # horrible ... but got to be done. The dictionary of metadata has to be a dictionnary with byte keys and byte values.
        # meaning that we can't have nested dictionaries ...
        decoded_meta = {
            key.decode("utf-8"): json.loads(value.decode("utf-8").replace("'", '"'))
            for key, value in parquet_meta.metadata.items()
        }

        self.assertEqual(
            decoded_meta["dataset_metadata"]["metadata_uuid"],
            "2807f3aa-4db0-4924-b64b-354ae8c10b58",
        )
        self.assertEqual(decoded_meta["dataset_metadata"]["title"], "ARDC")

    def test_parquet_nc_generic_handler_scipy(self):
        # test with the scipy engine
        nc_obj_ls = s3_ls("imos-data", "good_nc_soop_sst")

        # 1st pass
        with patch.object(self.handler_nc_soop_sst_file, "s3_fs", new=self.s3_fs):
            self.handler_nc_soop_sst_file.to_cloud_optimised([nc_obj_ls[0]])

        # 2nd pass, process the same file a second time. Should be deleted
        with patch.object(self.handler_nc_soop_sst_file, "s3_fs", new=self.s3_fs):
            self.handler_nc_soop_sst_file.to_cloud_optimised_single(nc_obj_ls[0])

        # read parquet
        dataset_config = load_dataset_config(DATASET_CONFIG_NC_SOOP_SST_JSON)
        dataset_name = dataset_config["dataset_name"]
        dname = f"s3://{self.BUCKET_OPTIMISED_NAME}/{self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH}/{dataset_name}.parquet/"

        parquet_dataset = pd.read_parquet(
            dname,
            engine="pyarrow",
            storage_options={
                "client_kwargs": {"endpoint_url": "http://127.0.0.1:5555"}
            },
        )

        self.assertEqual(parquet_dataset["timestamp"][0], 1293840000)

        self.assertEqual(
            parquet_dataset["TIME"][0], pd.Timestamp("2011-01-01 00:00:00")
        )

    def test_parquet_csv_generic_handler(self):  # , MockS3FileSystem):
        csv_obj_ls = s3_ls("imos-data", "good_csv", suffix=".csv")
        # with patch('s3fs.S3FileSystem', lambda anon, client_kwargs: s3fs.S3FileSystem(anon=False, client_kwargs={"endpoint_url": "http://127.0.0.1:5555/"})):
        # MockS3FileSystem.return_value = s3fs.S3FileSystem(anon=False, client_kwargs={"endpoint_url": "http://127.0.0.1:5555"})

        # with mock_aws(aws_credentials):
        # 1st pass, could have some errors distributed.worker - ERROR - Failed to communicate with scheduler during heartbeat.
        # Solution is the rerun the unittest
        with patch.object(self.handler_csv_file, "s3_fs", new=self.s3_fs):
            self.handler_csv_file.to_cloud_optimised([csv_obj_ls[0]])

        # 2nd pass
        with patch.object(self.handler_csv_file, "s3_fs", new=self.s3_fs):
            self.handler_csv_file.to_cloud_optimised_single(csv_obj_ls[0])

        # Read parquet dataset and check data is good!
        dataset_config = load_dataset_config(DATASET_CONFIG_CSV_AATAMS_JSON)
        dataset_name = dataset_config["dataset_name"]
        dname = f"s3://{self.BUCKET_OPTIMISED_NAME}/{self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH}/{dataset_name}.parquet/"

        parquet_dataset = pd.read_parquet(
            dname,
            engine="pyarrow",
            storage_options={
                "client_kwargs": {"endpoint_url": "http://127.0.0.1:5555"}
            },
        )

        self.assertIn("station_name", parquet_dataset.columns)


if __name__ == "__main__":
    unittest.main()
