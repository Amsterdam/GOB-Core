"""GOB Header

The GOB Header holds the header values of a workflow message.

Todo: Name of the module (message) should correspond to the contents of the file (GOBHeader)

"""
from gobcore.exceptions import GOBException


class MessageMetaData():

    source_id_column = '_source_id'

    def __init__(self, **header):
        self._header = header

        if self.source is None or \
                self.timestamp is None or \
                self.id_column is None or \
                self.entity is None or \
                self.version is None or \
                self.process_id is None or \
                self.model is None:
            raise GOBException("Incomplete metadata, all of source, timestamp, id_column, entity, version "
                               "process_id and model need to be defined")

    @property
    def source(self):
        return self._header['source']

    @property
    def timestamp(self):
        return self._header['timestamp']

    @property
    def id_column(self):
        return self._header['id_column']

    @property
    def entity(self):
        return self._header['entity']

    @property
    def version(self):
        return self._header['version']

    @property
    def process_id(self):
        return self._header['process_id']

    @property
    def model(self):
        return self._header['model']

    @property
    def as_header(self):
        return {
            "source": self.source,
            "timestamp": self.timestamp,
            "id_column": self.id_column,
            "entity": self.entity,
            "version": self.version,
            "process_id": self.process_id,
            "model": self.model
        }


class ImportMessage():

    def __init__(self, msg):
        self._metadata = MessageMetaData(**msg["header"])

        self._summary = msg["summary"]
        self._contents = msg["contents"]

    @property
    def metadata(self):
        return self._metadata

    @property
    def summary(self):
        return self._summary

    @property
    def contents(self):
        return self._contents

    @classmethod
    def create_import_message(cls, header, summary, contents):
        return {
            "header": header,
            "summary": summary,
            "contents": contents,
        }

# Todo, version should be a part of gob_model?
