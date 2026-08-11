"""Unit tests for optional chunks= on ZarrDataSource / GetAodn.get_dataset."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from aodn_cloud_optimised.lib.DataQuery import GetAodn, ZarrDataSource


class TestZarrChunksParam(unittest.TestCase):
    def _open_with_chunks(self, chunks, open_zarr_mock: MagicMock) -> ZarrDataSource:
        ds = MagicMock(name="zarr_dataset")
        ds.unify_chunks.return_value = ds
        ds.sortby.return_value = ds
        open_zarr_mock.return_value = ds

        with patch(
            "aodn_cloud_optimised.lib.DataQuery.s3fs.S3FileSystem"
        ) as s3_cls, patch(
            "aodn_cloud_optimised.lib.DataQuery.find_coord_var",
            return_value="TIME",
        ):
            s3_cls.return_value.get_mapper.return_value = MagicMock(name="mapper")
            return ZarrDataSource(
                "aodn-cloud-optimised",
                "",
                "example.zarr",
                chunks=chunks,
            )

    @patch("aodn_cloud_optimised.lib.DataQuery.xr.open_zarr")
    def test_default_chunks_is_auto(self, open_zarr_mock: MagicMock) -> None:
        source = self._open_with_chunks("auto", open_zarr_mock)
        self.assertEqual(source.chunks, "auto")
        _, kwargs = open_zarr_mock.call_args
        self.assertEqual(kwargs.get("chunks"), "auto")
        open_zarr_mock.return_value.unify_chunks.assert_called_once()

    @patch("aodn_cloud_optimised.lib.DataQuery.xr.open_zarr")
    def test_chunks_none_disables_dask_and_skips_unify(
        self, open_zarr_mock: MagicMock
    ) -> None:
        source = self._open_with_chunks(None, open_zarr_mock)
        self.assertIsNone(source.chunks)
        _, kwargs = open_zarr_mock.call_args
        self.assertIsNone(kwargs.get("chunks"))
        open_zarr_mock.return_value.unify_chunks.assert_not_called()

    @patch("aodn_cloud_optimised.lib.DataQuery.ZarrDataSource")
    def test_get_dataset_forwards_chunks(self, zarr_cls: MagicMock) -> None:
        zarr_cls.return_value = MagicMock(name="ZarrDataSource")
        aodn = GetAodn()
        aodn.get_dataset("example.zarr", chunks=None)
        _, kwargs = zarr_cls.call_args
        self.assertIsNone(kwargs.get("chunks"))
        self.assertEqual(zarr_cls.call_args.args[2], "example.zarr")

    @patch("aodn_cloud_optimised.lib.DataQuery.ZarrDataSource")
    def test_get_dataset_default_chunks_auto(self, zarr_cls: MagicMock) -> None:
        zarr_cls.return_value = MagicMock(name="ZarrDataSource")
        aodn = GetAodn()
        aodn.get_dataset("example.zarr")
        _, kwargs = zarr_cls.call_args
        self.assertEqual(kwargs.get("chunks"), "auto")


if __name__ == "__main__":
    unittest.main()
