import os
from aodn_cloud_optimised.lib.config import load_variable_from_config
from aodn_cloud_optimised.lib.schema import generate_json_schema_from_s3_netcdf

BUCKET_RAW_DEFAULT = load_variable_from_config('BUCKET_RAW_DEFAULT')
obj_key = 'IMOS/OceanCurrent/GSLA/NRT/2017/IMOS_OceanCurrent_HV_20170104T000000Z_GSLA_FV02_NRT.nc'
nc_file = os.path.join('s3://', BUCKET_RAW_DEFAULT, obj_key)

generate_json_schema_from_s3_netcdf(nc_file)