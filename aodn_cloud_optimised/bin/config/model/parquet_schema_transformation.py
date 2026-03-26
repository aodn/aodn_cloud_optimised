import re
import typing

import pydantic
from cfunits import Units

import aodn_cloud_optimised.lib.schema


class ParquetSchemaTransformation(pydantic.BaseModel):
    drop_variables: list[str] | None = pydantic.Field(
        default=None,
        description="List of variables to remove from the schema before publishing.",
    )
    add_variables: dict[str, typing.Any] | None = pydantic.Field(
        default=None,
        description="Variables to add to the schema, specified as a dictionary.",
    )
    global_attributes: dict[str, typing.Any] = pydantic.Field(
        default_factory=dict,
        description="Global attributes to modify. Supports 'delete' and 'set' keys.",
    )
    partitioning: list[dict[str, typing.Any]] = pydantic.Field(
        ..., description="Partitioning information for the dataset."
    )

    functions: dict[str, typing.Any] | None = pydantic.Field(
        default=None,
        description="Custom functions used to extract metadata from object keys and turn into variables, required if @function: is used in add_variables.",
    )
    skip_partitioning_validation: bool = pydantic.Field(
        False, description="Set to true to skip required partitioning validation."
    )

    @pydantic.field_validator("add_variables")
    @classmethod
    def validate_add_variables(cls, value):
        if value is None:
            return value

        allowed_sources = {
            "@filename",
            "@variable_attribute:",
            "@global_attribute:",
            "@partitioning:",
            "@function:",
        }

        # TODO: add test for @function (only used by autonomous_underwater_vehicle config)
        if not isinstance(value, dict):
            raise ValueError("add_variables must be a dictionary.")

        for var_name, var_config in value.items():
            if not isinstance(var_config, dict):
                raise ValueError(
                    f"Value for variable '{var_name}' must be a dictionary."
                )

            # Check 'source' key
            source = var_config.get("source")
            if not (
                source in allowed_sources
                or any(source.startswith(allowed) for allowed in allowed_sources)
            ):
                raise ValueError(
                    f"Invalid or missing 'source' for variable '{var_name}'. "
                    f"Expected to start with one of {allowed_sources}, got {source}."
                )

            # Check 'schema' key
            schema = var_config.get("schema")
            if not isinstance(schema, dict):
                raise ValueError(
                    f"Missing or invalid 'schema' for variable '{var_name}'."
                )

            valid_types = set(
                aodn_cloud_optimised.lib.schema.get_pyarrow_type_map().keys()
            )
            if "type" not in schema:
                raise ValueError(
                    f"'schema' for variable '{var_name}' must contain a 'type' key."
                )
            else:
                if schema["type"] not in valid_types:
                    raise ValueError(
                        f"Invalid type '{schema['type']}' for variable '{var_name}'. "
                        f"Must be one of: {', '.join(sorted(valid_types))}"
                    )

            if "units" not in schema:
                raise ValueError(
                    f"'schema' for variable '{var_name}' must contain a 'units' key."
                )
            else:
                unit_str = schema["units"]
                unit = Units(unit_str, calendar="gregorian")
                if not unit.isvalid:
                    raise ValueError(
                        f"Invalid CF unit '{unit_str}' for variable '{var_name}'."
                    )

        return value

    @pydantic.model_validator(mode="after")
    def validate_required_patitions(self):
        if self.skip_partitioning_validation:
            return self

        if not self.partitioning:
            raise ValueError("'partitioning' key missing")

        partition_keys = [x["source_variable"] for x in self.partitioning]

        required_partitioning_keys = ["polygon", "timestamp"]
        if not all(key in partition_keys for key in required_partitioning_keys):
            raise ValueError(
                f"Required variables {required_partitioning_keys} must be present in the 'partitioning' key. Only {partition_keys} available.\n If you think those partitions shouldn't exist, set '\"skip_partitioning_validation\" : true' in the schema_transformation configuration"
            )

        return self

    @pydantic.model_validator(mode="after")
    def validate_time_extent_partitioning(self):
        if not self.add_variables:
            return self

        valid_partition_values = ["time_extent", "spatial_extent"]

        for var_name, definition in self.add_variables.items():
            source = definition.get("source")
            schema = definition.get("schema", {})

            if source == "@partitioning:time_extent":
                if schema.get("type") != "int64":
                    raise ValueError(
                        f"When using source '@partitioning:time_extent', schema type for variable '{var_name}' must be 'int64'."
                    )

                if not self.partitioning:
                    raise ValueError(
                        f"'partitioning' must be defined when using '@partitioning:time_extent'."
                    )

                matching_parts = [
                    part
                    for part in self.partitioning
                    if isinstance(part, dict) and part.get("type") == "time_extent"
                ]

                if not matching_parts:
                    raise ValueError(
                        f"No partitioning entry of type 'time_extent' found under the partitioning section, required for variable '{var_name}'."
                    )

                for part in matching_parts:
                    time_extent = part.get("time_extent", {})
                    period = time_extent.get("partition_period")
                    valid_periods = {"M", "Q", "Y", "h", "min", "s", "ms", "us", "ns"}
                    if period not in valid_periods:
                        raise ValueError(
                            f"Invalid partition_period '{period}' in time_extent. "
                            f"Must be one of {sorted(valid_periods)}"
                        )
            elif source == "@partitioning:spatial_extent":
                pass
            elif source.startswith("@partitioning:") and not any(
                x in source for x in valid_partition_values
            ):
                raise ValueError(
                    f"Source: {source} is not valid. Must be one of {valid_partition_values}"
                )

        return self

    @pydantic.model_validator(mode="after")
    def validate_spatial_extent_partitioning(self):
        if not self.add_variables:
            return self

        valid_partition_values = ["time_extent", "spatial_extent"]

        for var_name, definition in self.add_variables.items():
            source = definition.get("source")
            schema = definition.get("schema", {})

            if source == "@partitioning:spatial_extent":
                if schema.get("type") != "string":
                    raise ValueError(
                        f"When using source '@partitioning:spatial_extent', schema type for variable '{var_name}' must be 'string'."
                    )

                if not self.partitioning:
                    raise ValueError(
                        f"'partitioning' must be defined when using '@partitioning:spatial_extent'."
                    )

                matching_parts = [
                    part
                    for part in self.partitioning
                    if isinstance(part, dict) and part.get("type") == "spatial_extent"
                ]

                if not matching_parts:
                    raise ValueError(
                        f"No partitioning entry of type 'spatial_extent' found, required for added variable '{var_name}'."
                    )

                for part in matching_parts:
                    spatial = part.get("spatial_extent", {})

                    lat_varname = spatial.get("lat_varname")
                    lon_varname = spatial.get("lon_varname")
                    resolution = spatial.get("spatial_resolution")

                    if not lat_varname or not isinstance(lat_varname, str):
                        raise ValueError(
                            "spatial_extent must define a non-empty string lat_varname."
                        )

                    if not lon_varname or not isinstance(lon_varname, str):
                        raise ValueError(
                            "spatial_extent must define a non-empty string lon_varname."
                        )

                    if not isinstance(resolution, int):
                        raise ValueError(
                            "spatial_extent must define an integer spatial_resolution."
                        )
            elif source == "@partitioning:time_extent":
                pass
            elif source.startswith("@partitioning:") and not any(
                x in source for x in valid_partition_values
            ):
                raise ValueError(
                    f"Source: {source} is not valid. Must be one of {valid_partition_values}"
                )

        return self

    @pydantic.model_validator(mode="after")
    def validate_global_attributes(self):
        if not self.global_attributes:
            return self

        if not isinstance(self.global_attributes, dict):
            raise ValueError("'global_attributes' must be a dictionary if present.")

        valid_keys = {"delete", "set"}
        if self.global_attributes:
            invalid_keys = [k for k in self.global_attributes if k not in valid_keys]
            if invalid_keys:
                raise ValueError(
                    f"Invalid global_attributes keys: {invalid_keys}. "
                    f"Only {valid_keys} are allowed."
                )

        if "delete" in self.global_attributes:
            delete = self.global_attributes["delete"]
            if not isinstance(delete, list) or not all(
                isinstance(item, str) for item in delete
            ):
                raise ValueError(
                    "'delete' under 'global_attributes' must be a list of strings."
                )

        if "set" in self.global_attributes:
            set_attrs = self.global_attributes["set"]
            if not isinstance(set_attrs, dict):
                raise ValueError(
                    "'set' under 'global_attributes' must be a dictionary."
                )

        return self

    @pydantic.model_validator(mode="after")
    def validate_function_sources(self):
        if not self.add_variables:
            return self

        # Only validate if any function source exists
        function_sources = {
            var_name: var_config.get("source")
            for var_name, var_config in self.add_variables.items()
            if isinstance(var_config, dict)
            and isinstance(var_config.get("source"), str)
            and var_config["source"].startswith("@function:")
        }

        if function_sources:
            if not hasattr(self, "functions") or not isinstance(self.functions, dict):
                raise ValueError(
                    f"'functions' section must be defined when using @function: sources (used in variables: {list(function_sources.keys())})"
                )

            for var_name, source in function_sources.items():
                function_name = source.split(":", 1)[-1]
                if function_name not in self.functions:
                    raise ValueError(
                        f"Function '{function_name}' (referenced in variable '{var_name}') not found in 'functions' section."
                    )

                function_def = self.functions[function_name]
                extract_method = function_def.get("extract_method")

                if extract_method == "object_key":
                    method = function_def.get("method", {})
                    code = method.get("extraction_code", "")

                    if (
                        not isinstance(code, str)
                        or "def " not in code
                        or "return " not in code
                    ):
                        raise ValueError(
                            f"'extraction_code' for function '{function_name}' must contain a function definition and return statement."
                        )
                    # Optional: enforce `return {` or `return dict(...)`
                    if (
                        not re.search(r"return\s+{", code)
                        and "return dict(" not in code
                    ):
                        raise ValueError(
                            f"'extraction_code' for function '{function_name}' must return a dictionary."
                        )

                elif extract_method == "from_variables":
                    method = function_def.get("method", {})
                    creation_code = method.get("creation_code", "")
                    if (
                        not isinstance(creation_code, str)
                        or "def " not in creation_code
                        or "return " not in creation_code
                    ):
                        raise ValueError(
                            f"'creation_code' for function '{function_name}' must contain a function definition and return statement."
                        )

                else:
                    raise ValueError(
                        f"Unsupported extract_method '{extract_method}' for function '{function_name}'"
                    )
        return self
