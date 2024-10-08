import json
from datetime import date, datetime
from pathlib import Path, PosixPath
from typing import Any, NamedTuple
from pydantic import BaseModel, AnyUrl
from enum import Enum
from cumulusci.utils.version_strings import LooseVersion, StepVersion
from cumulusci.core.declarations import (
    TemplateAwareDirectoryPath,
    TemplateAwareFilePath,
)


class JSONSerializer(NamedTuple):
    type: type  # The python type to serialize
    from_json: callable  # A function to deserialize the type from JSON
    to_key: callable = str  # A function to serialize the type as a key
    to_json: callable = str  # A function to serialize the type as a value
    skip_type: bool = (
        False  # If True, the type will not be included in the serialized output
    )

    @property
    def name(self):
        return self.type.__name__


string_serializers = [
    JSONSerializer(
        datetime,
        to_key=lambda x: x.isoformat(),
        to_json=lambda x: x.isoformat(),
        from_json=datetime.fromisoformat,
    ),
    JSONSerializer(
        date,
        to_key=lambda x: x.isoformat(),
        to_json=lambda x: x.isoformat(),
        from_json=date.fromisoformat,
    ),
    JSONSerializer(
        bytes,
        to_key=lambda x: x.decode("unicode_escape"),
        to_json=lambda x: x.decode("unicode_escape"),
        from_json=lambda x: x.encode("unicode_escape"),
    ),
    JSONSerializer(
        Path,
        from_json=Path,
        skip_type=True,
    ),
    JSONSerializer(
        PosixPath,
        from_json=PosixPath,
        skip_type=True,
    ),
    JSONSerializer(
        AnyUrl,
        from_json=lambda x: AnyUrl(x, scheme=x.split("://")[0]),
        skip_type=True,
    ),
    JSONSerializer(
        Enum,
        to_key=lambda x: x.value,
        to_json=lambda x: x.value,
        from_json=lambda x: x,  # This will be handled in decode_typed_value
    ),
    JSONSerializer(
        LooseVersion,
        to_key=lambda x: x.vstring,
        to_json=lambda x: x.vstring,
        from_json=StepVersion,
    ),
    JSONSerializer(
        StepVersion,
        from_json=StepVersion,
    ),
]


def decode_value(x: Any):
    """Decode a single value recursively"""
    if isinstance(x, dict):
        # If the dict is a typed value, decode it
        if "$type" in x:
            return decode_typed_value(x)

        # Otherwise, decode the dict's values
        new_dict = {}
        for key, value in x.items():
            new_key = decode_value(key)
            new_dict[new_key] = decode_value(value)
        return new_dict
    if isinstance(x, list):
        return [decode_value(value) for value in x]
    return x


def encode_value(x: dict | list, to_key=False, value_only=False):
    """Encode a dict or list that JSON does not support natively"""
    encoder = CumulusJSONEncoder()

    # Handle lists by encoding each value
    if isinstance(x, list):
        new_list = []
        for value in x:
            try:
                encoded = encode_value(
                    value,
                    to_key=to_key,
                    value_only=value_only,
                )
            except TypeError:
                encoded = value
            new_list.append(encoded)
        return new_list

    # Attempt to convert with a custom serializer
    for serializer in string_serializers:
        if isinstance(x, serializer.type):
            if to_key:
                return serializer.to_key(x)
            value = serializer.to_json(x)
            if value_only or serializer.skip_type:
                return value
            return {"$type": serializer.name, "$value": serializer.to_json(x)}

    if isinstance(x, BaseModel):
        if to_key:
            raise TypeError(f"Cannot encode Pydantic model {x} as a key")
        value = encode_value(x.dict(), to_key=to_key, value_only=value_only)
        if value_only:
            return value
        else:
            return {"$type": x.__class__.__name__, "$value": value}

    if isinstance(x, Enum):
        return {
            "$type": "Enum",
            "$value": {"name": x.__class__.__name__, "value": x.value},
        }

    if not isinstance(x, dict):
        return x

    # Handle dicts by encoding each key and value

    new_dict = {}
    for key, value in x.items():
        new_key = encode_value(
            key,
            to_key=True,
            value_only=True,
        )
        try:
            new_value = encode_value(value, to_key=to_key, value_only=value_only)
        except TypeError:
            new_value = value

        new_dict[new_key] = new_value
    return new_dict


def decode_typed_value(x: dict):
    """Decode a value that JSON does not support natively"""
    for serializer in string_serializers:
        if x["$type"] == serializer.name:
            return serializer.from_json(x["$value"])
    if x["$type"] == "Enum":
        enum_class = globals()[x["$value"]["name"]]
        return enum_class(x["$value"]["value"])
    # Attempt to lookup Pydantic models
    if x["$type"] in globals():
        model_class = globals()[x["$type"]]
        if isinstance(model_class, BaseModel):
            return model_class.parse_obj(x["$value"])
    raise TypeError(f"Unknown $type: {x['$type']}")  # pragma: no cover


class CumulusJSONEncoder(json.JSONEncoder):
    def default(self, obj, to_key=False, value_only=False):
        try:
            return encode_value(obj, to_key=to_key, value_only=value_only)
        except TypeError:
            # Let the base class default method raise the TypeError
            return super().default(obj)


def encode_keys(obj):
    if isinstance(obj, list):
        return [encode_keys(i) for i in obj]
    if not isinstance(obj, dict):
        return obj
    new_obj = {}
    for k, v in obj.items():
        key = encode_value(k, to_key=True) if not isinstance(k, str) else k
        new_obj[key] = encode_keys(v)
    return new_obj


def json_dumps(obj, **kwargs):
    obj = encode_keys(obj)
    if "value_only" in kwargs:
        value_only = kwargs.pop("value_only")
        if value_only:
            obj = encode_value(obj, value_only=True)
    return json.dumps(obj, cls=CumulusJSONEncoder, **kwargs)


def json_loads(s, **kwargs):
    return json.loads(s, object_hook=decode_value, **kwargs)
