"""Type definitions for the realestate project"""

import json
import xml.etree.ElementTree as ET
from dagster import (
    Field,
    String,
    make_python_type_usable_as_dagster_type,
    Dict,
    check,
    dagster_type_loader,
    Permissive,
    List,
    Int,
    DagsterType,
    Any,
)
from dagster.core.types.dagster_type import PythonObjectDagsterType, create_string_type


def is_json(_, value):
    try:
        json.loads(value)
        return True
    except ValueError:
        return False


JsonType = DagsterType(
    name="JsonType",
    description="A valid representation of a JSON, validated with json.loads().",
    type_check_fn=is_json,
)


def is_xml(_, value):
    try:
        ET.fromstring(value)
        return True
    except ValueError:
        return False


XmlType = DagsterType(
    name="XmlType",
    description="A valid representation of a XML, validated with import xml.etree.ElementTree.",
    type_check_fn=is_xml,
)
