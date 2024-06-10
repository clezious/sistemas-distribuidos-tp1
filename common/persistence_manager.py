import os
import logging
import uuid
import json

KEYS_INDEX_KEY = 'keys_index'


class PersistenceManager:
    def __init__(self, storage_path='./'):
        if not os.path.exists(storage_path):
            logging.debug(f"Creating storage path: {storage_path}")
            os.makedirs(storage_path)
        self.storage_path = storage_path
        self._keys_index: dict[str, str] = {}
        self._init_state()

    def _append(self, path, data):
        try:
            logging.debug(f"Appending to {path}")
            with open(path, 'a') as f:
                f.write(data)
                f.write('\n')
                f.flush()
        except Exception as e:
            logging.error(f"Error appending to {path}: {e}")

    def _write(self, path, data):
        try:
            logging.debug(f"Writing to {path}")
            with open(path, 'w') as f:
                f.write(data)
                f.flush()
        except Exception as e:
            logging.error(f"Error writing to {path}: {e}")

    def _read(self, path):
        try:
            logging.debug(f"Reading from {path}")
            with open(path, 'r') as f:
                return f.read()
        except OSError as e:
            if e.errno == 2:  # File not found
                return ''
            logging.error(f"Error reading from {path}: {e}")

    def put(self, key: str, value: str):
        try:
            path = f'{self.storage_path}/{self._get_internal_key(key)}'
            logging.debug(f"Putting value: {value} for key: {key}")
            self._write(path, value)
        except Exception as e:
            logging.error(f"Error putting value: {value} for key: {key}: {e}")

    def get(self, key: str) -> str:
        if key not in self._keys_index:
            return ''
        path = f'{self.storage_path}/{self._get_internal_key(key)}'
        return self._read(path)

    def append(self, key: str, value: str):
        try:
            path = f'{self.storage_path}/{self._get_internal_key(key)}'
            logging.debug(f"Appending value: {value} for key: {key}")
            self._append(path, value)
        except Exception as e:
            logging.error(f"Error appending value: {value} for key: {key}: {e}")

    def get_keys(self, prefix='') -> list[str]:
        try:
            logging.debug(f"Getting keys with prefix: {prefix}")
            return [key for key in self._keys_index if key.startswith(prefix)]
        except Exception as e:
            logging.error(f"Error getting keys with prefix: {prefix}: {e}")
            return []

    def _get_internal_key(self, key: str) -> str:
        internal_key = self._keys_index.get(key)
        if internal_key is None:
            logging.debug(f"Generating internal key for key: {key}")
            internal_key = str(uuid.uuid4())
            self._keys_index[key] = internal_key
            self._append(f'{self.storage_path}/{KEYS_INDEX_KEY}',
                         json.dumps([key, internal_key]))
        return internal_key

    def _init_state(self):
        keys_index = [
            json.loads(entry)
            for entry in
            self._read(f'{self.storage_path}/{KEYS_INDEX_KEY}').splitlines()]
        for entry in keys_index:
            self._keys_index[entry[0]] = entry[1]
        logging.debug(
            f"Initialized PersistenceManager with state: {self._keys_index}")
