import io
import os
import re
import xml.etree.ElementTree as ET
from collections import defaultdict
from tempfile import TemporaryDirectory
from typing import List
from zipfile import ZipFile

from objectstore.objectstore import get_object
from osgeo import ogr

from gobcore.datastore.datastore import Datastore
from gobcore.datastore.objectstore import ObjectDatastore
from gobcore.exceptions import GOBException


def _extract_nested_zip(zip_file, nested_zip_files: List[str], destination_dir: str):
    """Extracts nested zip file from zip_file.

    Example:
    _extract_nested_zip('a.zip', ['b.zip', 'c.zip'], '/tmp_dstdir')

    with:
    a.zip
        somefile_in_a
        other_file_in_a
        b.zip
            some_file_in_b
            c.zip
                some_file_in_c
                some_other_file_in_c
            some_other_file_in_b

    results in:
    /tmp_dst_dir/some_file_in_c
    /tmp_dst_dir/some_other_file_in_c

    :param zip_file:
    :param nested_zip_files:
    :param destination_dir:
    :return:
    """
    with ZipFile(zip_file, 'r') as f:
        if len(nested_zip_files) == 0:
            f.extractall(destination_dir)
        else:
            with f.open(nested_zip_files[0], 'r') as nested_zip_file:
                nested_zip_file_data = io.BytesIO(nested_zip_file.read())
                _extract_nested_zip(nested_zip_file_data, nested_zip_files[1:], destination_dir)


class BagExtractDatastore(Datastore):
    MUTATIONS_MODE = "mutations"
    FULL_MODE = "full"

    ns_pattern = re.compile(r'{.*}')
    namespaces = {
        # We could extract namespaces from the file, but this way we're sure they won't change in the source.
        'DatatypenNEN3610': 'www.kadaster.nl/schemas/lvbag/imbag/datatypennen3610/v20200601',
        'Objecten': 'www.kadaster.nl/schemas/lvbag/imbag/objecten/v20200601',
        'gml': 'http://www.opengis.net/gml/3.2',
        'Historie': 'www.kadaster.nl/schemas/lvbag/imbag/historie/v20200601',
        'Objecten-ref': 'www.kadaster.nl/schemas/lvbag/imbag/objecten-ref/v20200601',
        'ml': 'http://www.kadaster.nl/schemas/mutatielevering-generiek/1.0',
        'mlm': 'http://www.kadaster.nl/schemas/lvbag/extract-deelbestand-mutaties-lvc/v20200601',
        'nen5825': 'www.kadaster.nl/schemas/lvbag/imbag/nen5825/v20200601',
        'KenmerkInOnderzoek': 'www.kadaster.nl/schemas/lvbag/imbag/kenmerkinonderzoek/v20200601',
        'selecties-extract': 'http://www.kadaster.nl/schemas/lvbag/extract-selecties/v20200601',
        'sl-bag-extract': 'http://www.kadaster.nl/schemas/lvbag/extract-deelbestand-lvc/v20200601',
        'sl': 'http://www.kadaster.nl/schemas/standlevering-generiek/1.0',
        'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
        'xs': 'http://www.w3.org/2001/XMLSchema',
    }

    def __init__(self, connection_config: dict, read_config: dict = None):
        super().__init__(connection_config, read_config)

        self.tmp_dir = TemporaryDirectory()
        self.files = []

        self._check_config()

        xml_object = self.read_config.get('xml_object')
        self.full_xml_path = f"./sl:standBestand/sl:stand/sl-bag-extract:bagObject/Objecten:{xml_object}"
        self.mutation_xml_paths = [
            f"./ml:mutatieBericht/ml:mutatieGroep/ml:toevoeging/ml:wordt/mlm:bagObject/Objecten:{xml_object}",
            f"./ml:mutatieBericht/ml:mutatieGroep/ml:wijziging/ml:wordt/mlm:bagObject/Objecten:{xml_object}",
        ]

        self.mode = self.read_config['mode']

    def _check_config(self):
        for key in ('object_type', 'xml_object', 'mode', 'gemeentes'):
            if not self.read_config.get(key):
                raise GOBException(f"Missing {key} in read_config")

        if self.read_config['mode'] not in (self.FULL_MODE, self.MUTATIONS_MODE):
            raise GOBException(f"Invalid mode: {self.read_config['mode']}")

    def _download_and_extract_full_file(self, file_location: str):
        filename = file_location.split('/')[-1]
        match = re.match(r'^BAGGEM(\d{4})L-(\d{8}).zip$', filename)

        if not match:
            raise GOBException(f"Unexpected filename format: {filename}")

        gemeente = match.group(1)
        datestr = match.group(2)

        _extract_nested_zip(file_location, [
            f"{gemeente}GEM{datestr}.zip",
            f"{gemeente}{self.read_config['object_type']}{datestr}.zip",
        ], self.tmp_dir.name)

        return [os.path.join(self.tmp_dir.name, f)
                for f in sorted(os.listdir(self.tmp_dir.name)) if f.endswith('.xml')]

    def _download_and_extract_mutations_file(self, file_location: str):
        filename = file_location.split('/')[-1]
        match = re.match(r'^BAGNLDM-(\d{8}-\d{8}).zip$', filename)

        if not match:
            raise GOBException(f"Unexpected filename format: {filename}")

        daterange = match.group(1)
        _extract_nested_zip(file_location, [
            f"9999MUT{daterange}.zip",
        ], self.tmp_dir.name)

        return [os.path.join(self.tmp_dir.name, f)
                for f in sorted(os.listdir(self.tmp_dir.name)) if f.endswith('.xml')]

    def connect(self):

        if self.mode == self.FULL_MODE:
            file_location = self._download_file('tmp_weesp/BAGGEM0457L-15112020.zip')
            self.files = self._download_and_extract_full_file(file_location)
        else:
            file_location = self._download_file('tmp_weesp/BAGNLDM-31122020-01012021.zip')
            self.files = self._download_and_extract_mutations_file(file_location)

    def disconnect(self):
        pass  # pragma: no cover

    def _download_file(self, file_location: str):
        """Downloads source file and returns file location

        TODO: This file is now temporarily downloaded from a fixed location from the object store. This file will later
        come from a web source or something similar.

        :return:
        """

        # ObjectDatastore is hardcoded for now, as Core does not have direct access to Config, this is the easy temp
        # solution.
        config = {
            'type': 'objectstore',
            "VERSION": '2.0',
            "AUTHURL": 'https://identity.stack.cloudvps.com/v2.0',
            "TENANT_NAME": os.getenv("GOB_OBJECTSTORE_TENANT_NAME"),
            "TENANT_ID": os.getenv("GOB_OBJECTSTORE_TENANT_ID"),
            "USER": os.getenv("GOB_OBJECTSTORE_USER"),
            "PASSWORD": os.getenv("GOB_OBJECTSTORE_PASSWORD"),
            "REGION_NAME": 'NL'
        }

        read_config = {
            "file_filter": file_location,
            "container": "acceptatie",
        }

        objectstore = ObjectDatastore(config, read_config)
        objectstore.connect()

        file = next(objectstore.query(None))
        obj = get_object(objectstore.connection, file, read_config['container'])

        fname = file_location.split('/')[-1]
        download_location = os.path.join(self.tmp_dir.name, fname)
        with open(download_location, 'wb') as f:
            f.write(obj)

        return download_location

    def _gml_to_wkt(self, elm):
        gml_str = ET.tostring(elm).decode('utf-8')
        gml = ogr.CreateGeometryFromGML(gml_str)
        gml.FlattenTo2D()
        return gml.ExportToWkt()

    def _flatten_nested_list(self, lst: list, key_prefix: str):
        """Flattens list, called from the _flatten_dict method. Pulls the dict keys in the list out.

        For example, when called with list [{'some_key': 'A'}, {'some_key': 'B'}] and key_prefix 'prefix', the result
        is dict of the form:

        { 'prefix/some_key': ['A', 'B'] }

        :param lst:
        :param key_prefix:
        :return:
        """
        result = {}
        for item in lst:
            if isinstance(item, dict):
                # We have something of the form { ..., "key": [{"subkey": "A"}, {"subkey": "B"}], ... }
                # We transform this to { ..., "key/subkey": ["A", "B"], ... }
                d_item = self._flatten_dict(item)

                for d_key, d_value in d_item.items():
                    sub_key = f"{key_prefix}/{d_key}"
                    if sub_key not in result:
                        result[sub_key] = []

                    result[sub_key].append(d_value)
            else:
                result[key_prefix] = lst
        return result

    def _flatten_dict(self, d: dict):
        """Flattens dictionary, separates keys by a / character.

        {
            'a': {
                'b': {
                    'c': 'd',
                },
                'e': 'f',
            'g': [{'h': 4}, {'h': 5}]
        }

        will become:

        {
            'a/b/c': 'd',
            'a/e': 'f',
            'g/h': [4, 5]
        }

        :param d:
        :return:
        """

        def flatten(dct: dict):
            result = {}
            for key, value in dct.items():
                if isinstance(value, dict):
                    # Recursively traverse dictionaries
                    result.update({f"{key}/{k}": v for k, v in flatten(value).items()})
                elif isinstance(value, list):
                    result.update(self._flatten_nested_list(value, key))
                else:
                    result[key] = value
            return result

        return flatten(d)

    def _element_to_dict(self, element: ET.Element):
        """Transforms an XML element to a dictionary.

        :param element:
        :return:
        """
        childs = list(element)

        if len(childs) == 1 and self.namespaces['gml'] in childs[0].tag:
            return self._gml_to_wkt(childs[0])
        elif childs:
            child_dicts = defaultdict(list)

            for child in childs:
                child_dicts[re.sub(self.ns_pattern, '', child.tag)].append(self._element_to_dict(child))

            return {k: v[0] if len(v) == 1 else v for k, v in child_dicts.items()}

        else:
            return element.text.strip()

    def _get_elements_full(self, xmlroot):
        yield from xmlroot.iterfind(self.full_xml_path, self.namespaces)

    def _get_elements_mutations(self, xmlroot):
        gemeentes = self.read_config.get('gemeentes', [])
        for path in self.mutation_xml_paths:

            for element in xmlroot.iterfind(path, self.namespaces):
                identificatie = element.find("./Objecten:identificatie", self.namespaces)

                if identificatie is not None and identificatie.text.strip()[:4] in gemeentes:
                    # Filter only gemeentes we are interested in
                    yield element

    def query(self, query):
        get_elements_fn = self._get_elements_full if self.mode == self.FULL_MODE else self._get_elements_mutations

        for file in self.files:
            tree = ET.parse(file)
            root = tree.getroot()

            elements = get_elements_fn(root)

            for element in elements:
                yield self._flatten_dict(self._element_to_dict(element))
