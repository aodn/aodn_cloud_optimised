import typing
import warnings

import numpy as np
import pydantic
from cfunits import Units


class ZarrSchemaTransformation(pydantic.BaseModel):
    dataset_schema: dict[str, typing.Any]
    dataset_name: str
    add_variables: dict[str, typing.Any] | None = pydantic.Field(
        default=None,
        description="Variables to add to the schema, specified as a dictionary.",
    )
    global_attributes: dict[str, typing.Any] = pydantic.Field(
        default_factory=dict,
        description="Global attributes to modify. Supports 'delete' and 'set' keys.",
    )
    var_template_shape: str | list[str] = pydantic.Field(
        ..., description="Variable name(s) used to define the template shape."
    )
    dimensions: dict[str, dict[str, typing.Any]] | None = None
    dataset_sort_by: list[str] | None = None
    vars_incompatible_with_region: list[str] | None = None

    @pydantic.model_validator(mode="after")
    def validate_dataset_sortby(self):
        if self.dataset_sort_by:
            valid_names = {
                dim["name"] for dim in self.dimensions.values() if "name" in dim
            }
            for var in self.dataset_sort_by:
                if var not in valid_names:
                    raise ValueError(
                        f"{var} does not exist in the dimensions list and can't be used to sort the dataset"
                    )
        return self

    @pydantic.model_validator(mode="after")
    def validate_dimensions_config(self):
        if not self.dimensions:
            return self  # Nothing to check

        # Check how many dimensions have "append_dim" set
        has_append_flag = [
            (dim_key, props.get("append_dim"))
            for dim_key, props in self.dimensions.items()
            if "append_dim" in props
        ]

        if has_append_flag:
            # Ensure exactly one is True
            true_dims = [k for k, v in has_append_flag if v is True]
            if len(true_dims) != 1:
                raise ValueError(
                    f"Exactly one dimension must have 'append_dim: true'. Found: {true_dims}"
                )
        else:
            # No dimension has an append_dim key
            warnings.warn(
                f"{self.dataset_name}\n"
                "No 'append_dim' key was found in any dimension config. "
                "will default to using dimensions[\"time\"]. Consider adding 'append_dim: true' "
                "to one dimension explicitly for clarity.",
                stacklevel=1,
            )

        return self

    @pydantic.model_validator(mode="after")
    def validate_vars_incompatible_with_region(self):
        if self.vars_incompatible_with_region:
            missing = [
                var
                for var in self.vars_incompatible_with_region
                if var not in self.dataset_schema
            ]
            if missing:
                raise ValueError(
                    f"The following vars_incompatible_with_region are not defined in schema configuration or mispelled: {missing}"
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

    @classmethod
    def is_valid_dtype(cls, attr_type: str) -> bool:
        try:
            np.dtype(attr_type)
            return True
        except TypeError:
            return False

    @classmethod
    def not_implemented_dtype(cls, attr_type: str) -> bool:
        # Only support string types, i.e. Unicode kind 'U'
        if np.dtype(attr_type).kind != "U":
            raise ValueError(
                f"{attr_type} is not supported for global attribute conversion. "
                "Only Unicode string types (e.g., '<U49') are supported due to Dask limitations."
            )
        return False

    @pydantic.field_validator("add_variables", mode="after")
    def validate_add_variables(cls, value):
        if value is None:
            return value

        allowed_sources = {
            "@filename",
            "@global_attribute:",
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
            #
            # 'type' pydantic.Field check
            dtype = schema.get("type")
            if dtype is None:
                raise ValueError(
                    f"'schema' for variable '{var_name}' must contain a 'type' key."
                )

            if var_name != "filename":
                if not cls.is_valid_dtype(dtype):
                    raise ValueError(
                        f"'{dtype}' for variable '{var_name}' is not a valid NumPy dtype."
                    )

                cls.not_implemented_dtype(dtype)

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
    def validate_var_template_shape(self) -> "ZarrSchemaTransformation":
        # 1. Ensure it's not empty
        if not self.var_template_shape:
            raise ValueError("var_template_shape cannot be empty.")

        # 2. Coerce to a list for easier validation logic
        vars_to_check = (
            [self.var_template_shape]
            if isinstance(self.var_template_shape, str)
            else self.var_template_shape
        )

        # 3. Check that every variable exists in the dataset_schema
        schema_keys = self.dataset_schema.keys()
        missing = [v for v in vars_to_check if v not in schema_keys]

        if missing:
            raise ValueError(
                f"var_template_shape variable(s) {missing} not found in 'schema'. "
                f"Available variables: {list(schema_keys)}"
            )

        return self
