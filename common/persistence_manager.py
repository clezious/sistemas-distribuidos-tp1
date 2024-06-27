import os
import logging
import uuid
import json

KEYS_INDEX_KEY_PREFIX = 'keys_index_'
LENGTH_BYTES = 6
TEMP_FILE = '_temp'


class PersistenceManager:
    def __init__(self, storage_path='./'):
        if not os.path.exists(storage_path):
            logging.debug(f"Creating storage path: {storage_path}")
            os.makedirs(storage_path)
        self.storage_path = storage_path
        self._keys_index: dict[str, dict[str, str]] = {}
        self._init_state()

    def _append(self, path, data: str):
        try:
            logging.debug(f"Appending to {path}")
            data = data.encode('unicode_escape')
            data += b'\n'
            length_bytes = len(data).to_bytes(
                LENGTH_BYTES, byteorder='big')
            with open(path, 'ab') as f:
                f.write(length_bytes + data)
                f.flush()
        except Exception as e:
            logging.error(f"Error appending to {path}: {e}")

    def _write(self, path, data: str):
        try:
            logging.debug(f"Writing to {path}")
            data = data.encode('unicode_escape')
            data += b'\n'
            length_bytes = len(data).to_bytes(
                LENGTH_BYTES, byteorder='big')
            temp_path = f'{self.storage_path}/{TEMP_FILE}'
            with open(temp_path, 'wb') as f:
                f.write(length_bytes + data)
                f.flush()
            os.replace(temp_path, path)
        except Exception as e:
            logging.error(f"Error writing to {path}: {e}")

    def _read(self, path):
        try:
            logging.debug(f"Reading from {path}")
            with open(path, 'rb') as f:
                data = ''
                while (length := f.read(LENGTH_BYTES)):
                    length = int.from_bytes(length, byteorder='big')
                    content = f.readline()
                    if len(content) == length:
                        data += content.decode('unicode_escape')
                    else:
                        logging.error(f"Corrupted data in {path} expected {length} bytes, got {len(content)} bytes")
                        logging.error(f"Content: {content}")
                return data
        except OSError as e:
            if e.errno == 2:  # File not found
                return ''
            logging.error(f"Error reading from {path}: {e}")

    def _delete(self, path):
        try:
            logging.debug(f"Deleting {path}")
            os.remove(path)
        except Exception as e:
            logging.error(f"Error deleting {path}: {e}")

    def put(self, key: str, value: str, secondary_key: str = 'default'):
        try:
            path = f'{self.storage_path}/{self._get_internal_key(key, secondary_key)}'
            logging.debug(f"Putting value: {value} for key: {key}")
            self._write(path, value)
        except Exception as e:
            logging.error(f"Error putting value: {value} for key: {key}: {e}")

    def get(self, key: str, secondary_key: str = 'default') -> str:
        if key not in self._keys_index.get(secondary_key, {}):
            return ''
        path = f'{self.storage_path}/{self._get_internal_key(key, secondary_key)}'
        return self._read(path).removesuffix('\n')

    def append(self, key: str, value: str, secondary_key: str = 'default'):
        try:
            path = f'{self.storage_path}/{self._get_internal_key(key, secondary_key)}'
            logging.debug(f"Appending value: {value} for key: {key}")
            self._append(path, value)
        except Exception as e:
            logging.error(f"Error appending value: {value} for key: {key}: {e}")

    def get_keys(self, prefix='', secondary_key: str = None) -> list[tuple[str, str]]:
        try:
            logging.debug(f"Getting keys with prefix: {prefix}, secondary_key: {secondary_key}")
            keys = []
            for _secondary_key in [secondary_key] if secondary_key else self._keys_index.keys():
                keys.extend([(key, _secondary_key) for key in self._keys_index.get(_secondary_key, {}) if key.startswith(prefix)])
            return keys
        except Exception as e:
            logging.error(f"Error getting keys with prefix: {prefix}: {e}")
            return []

    def delete_keys(self, prefix: str = '', secondary_key: str = 'default'):
        logging.debug(f"Deleting keys by prefix: {prefix}, secondary_key: {secondary_key}")
        try:
            keys_to_delete = self.get_keys(prefix, secondary_key)
            for (key, secondary_key) in keys_to_delete:
                path = f'{self.storage_path}/{self._keys_index[secondary_key][key]}'
                self._delete(path)
                self._keys_index[secondary_key].pop(key)
                logging.debug(f"Deleted key: {key}")
            new_keys = [json.dumps([key, value]) for key, value in self._keys_index[secondary_key].items()]
            if len(new_keys) == 0:
                self._delete(f'{self.storage_path}/{KEYS_INDEX_KEY_PREFIX}{secondary_key}')
            else:
                self._write(f'{self.storage_path}/{KEYS_INDEX_KEY_PREFIX}{secondary_key}', '\n'.join(new_keys))
        except Exception as e:
            logging.error(f"Error deleting keys by prefix: {prefix}: {e}")

    def _get_internal_key(self, key: str, secondary_key: str = 'default') -> str:
        internal_key = self._keys_index.get(secondary_key, {}).get(key)
        if internal_key is None:
            logging.debug(f"Generating internal key for key: {key}, secondary_key: {secondary_key}")
            if secondary_key not in self._keys_index:
                self._keys_index[secondary_key] = {}
            internal_key = str(uuid.uuid4())
            self._keys_index[secondary_key][key] = internal_key
            self._append(f'{self.storage_path}/{KEYS_INDEX_KEY_PREFIX}{secondary_key}',
                         json.dumps([key, internal_key]))
        return internal_key

    def _init_state(self):
        for file_name in [file.name for file in os.scandir(self.storage_path)
                          if file.is_file() and file.name.startswith(KEYS_INDEX_KEY_PREFIX)]:
            secondary_key = file_name.removeprefix(f'{KEYS_INDEX_KEY_PREFIX}')
            self._keys_index[secondary_key] = {}
            for entry in self._read(f'{self.storage_path}/{file_name}').splitlines():
                [key, value] = json.loads(entry)
                self._keys_index[secondary_key][key] = value
        logging.debug(
            f"Initialized PersistenceManager with state: {self._keys_index}")
