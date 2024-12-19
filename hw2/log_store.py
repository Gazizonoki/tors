import os
import threading
import logging


class Storage:
    def __init__(self, data_dir):
        flags = os.O_RDWR | os.O_CREAT | os.O_SYNC

        self.log_file = os.open(os.path.join(data_dir, 'wal.txt'), flags, 0o666)
        self.term_file = os.open(os.path.join(data_dir, 'term.txt'), flags, 0o666)
        self.voted_for_file = os.open(os.path.join(data_dir, 'voted_for.txt'), flags, 0o666)
        self.commit_index_file = os.open(os.path.join(data_dir, 'commit_index.txt'), flags, 0o666)
        self.last_applied = 0
        self.data_store = dict()
        self.data_dir = data_dir
        self.log_len = 0
        self.log_lock = threading.Lock()
        self.term_lock = threading.Lock()
        self.voted_lock = threading.Lock()
        self.commit_index_lock = threading.Lock()

        with self.log_lock:
            while True:
                entry = self.__get_entry_unsafe()
                if len(entry) == 0:
                    break
                self.log_len += 1
                if self.log_len <= self.commit_index():
                    if entry['op_code'] == 0:
                        logging.info(f"REC PUT: {entry['key']}, {entry['value']}")
                        self.data_store[entry['key']] = entry['value']
                    else:
                        logging.info(f"REC DEL: {entry['key']}")
                        del self.data_store[entry['key']]
        logging.info(f"Recovered {self.log_len} entries")

    def set_term(self, term: int):
        with self.term_lock:
            os.lseek(self.term_file, 0, os.SEEK_SET)
            os.truncate(self.term_file, 0)
            os.write(self.term_file, str(term).encode())

    def term(self):
        with self.term_lock:
            os.lseek(self.term_file, 0, os.SEEK_SET)
            data = os.read(self.term_file, os.path.getsize(self.term_file)).decode()
            if len(data) == 0:
                return 0
            return int(data)

    def set_voted_for(self, voted_for: str|None):
        with self.voted_lock:
            os.lseek(self.voted_for_file, 0, os.SEEK_SET)
            os.truncate(self.voted_for_file, 0)
            if voted_for == None:
                os.truncate(self.voted_for_file, 0)
            else:
                os.write(self.voted_for_file, voted_for.encode())

    def voted_for(self):
        with self.voted_lock:
            os.lseek(self.voted_for_file, 0, os.SEEK_SET)
            data = os.read(self.voted_for_file, os.path.getsize(self.voted_for_file)).decode()
            if len(data) == 0:
                return None
            return data

    def commit_index(self):
        with self.commit_index_lock:
            os.lseek(self.commit_index_file, 0, os.SEEK_SET)
            data = os.read(self.commit_index_file, os.path.getsize(self.commit_index_file)).decode()
            if len(data) == 0:
                return 0
            return int(data)

    def last_index(self):
        return self.log_len
    
    def get_last_term(self):
        with self.log_lock:
            if self.log_len == 0:
                return (0, 0)
            os.lseek(self.log_file, 0, os.SEEK_SET)
            cur_index = 1
            while cur_index < self.log_len:
                if len(self.__get_entry_unsafe()) == 0:
                    raise RuntimeError(f"Couldn't apply entries to store: log too small ({self.log_len})")
                cur_index += 1
            entry = self.__get_entry_unsafe()
            if len(entry) == 0:
                raise RuntimeError(f"Couldn't apply entries to store: log too small ({self.log_len})")
            os.lseek(self.log_file, 0, os.SEEK_END)
            return (entry['term'], self.log_len)

    def append_put_to_log(self, term: int, key: bytes, value: bytes):
        with self.log_lock:
            os.write(self.log_file, int(0).to_bytes(1, "big"))
            os.write(self.log_file, term.to_bytes(8, "big"))
            os.write(self.log_file, len(key).to_bytes(8, "big"))
            os.write(self.log_file, key)
            os.write(self.log_file, len(value).to_bytes(8, "big"))
            os.write(self.log_file, value)
            self.log_len += 1
            return self.log_len

    def append_delete_to_log(self, term: int, key: bytes):
        with self.log_lock:
            os.write(self.log_file, int(1).to_bytes(1, "big"))
            os.write(self.log_file, term.to_bytes(8, "big"))
            os.write(self.log_file, len(key).to_bytes(8, "big"))
            os.write(self.log_file, key)
            self.log_len += 1
            return self.log_len

    def __get_entry_unsafe(self):
        result = dict()
        op_code = os.read(self.log_file, 1)
        if len(op_code) == 0:
            return dict()
        result['op_code'] = int.from_bytes(op_code, "big")
        result['term'] = int.from_bytes(os.read(self.log_file, 8), "big")
        key_len = int.from_bytes(os.read(self.log_file, 8), "big")
        result['key'] = os.read(self.log_file, key_len)
        if result['op_code'] == 0:
            value_len = int.from_bytes(os.read(self.log_file, 8), "big")
            result['value'] = os.read(self.log_file, value_len)
        return result

    def __apply_entries_unsafe(self, new_commit_index):
        with self.commit_index_lock:
            os.lseek(self.commit_index_file, 0, os.SEEK_SET)
            os.truncate(self.commit_index_file, 0)
            os.write(self.commit_index_file, str(new_commit_index).encode())
        cur_index = 0
        while cur_index < self.last_applied:
            if len(self.__get_entry_unsafe()) == 0:
                raise RuntimeError(f"Couldn't apply entries to store: log too small ({self.log_len})")
            cur_index += 1
        while cur_index < self.commit_index():
            entry = self.__get_entry_unsafe()
            if len(entry) == 0:
                raise RuntimeError(f"Couldn't apply entries to store: log too small ({self.log_len})")
            if entry['op_code'] == 0:
                self.data_store[entry['key']] = entry['value']
            else:
                del self.data_store[entry['key']]
            cur_index += 1
        self.last_applied = self.commit_index()

    def apply_entries(self, new_commit_index):
        with self.log_lock:
            os.lseek(self.log_file, 0, os.SEEK_SET)
            self.__apply_entries_unsafe(new_commit_index)
            os.lseek(self.log_file, 0, os.SEEK_END)

    def rewrite_log_tail(self, prev_log_index: int, prev_log_term: int, leader_commit: int, entries: bytes):
        with self.log_lock:
            if prev_log_index > self.log_len:
                return False
            os.lseek(self.log_file, 0, os.SEEK_SET)
            cur_index = 1
            while cur_index < prev_log_index:
                if len(self.__get_entry_unsafe()) == 0:
                    os.lseek(self.log_file, 0, os.SEEK_END)
                    return False
                cur_index += 1
            pos = 0
            if prev_log_index > 0:
                entry = self.__get_entry_unsafe()
                if entry['term'] != prev_log_term:
                    os.lseek(self.log_file, 0, os.SEEK_END)
                    return False
                pos = os.lseek(self.log_file, 0, os.SEEK_CUR)
            os.truncate(self.log_file, pos)
            self.log_len = prev_log_index
            os.write(self.log_file, entries)
            os.lseek(self.log_file, pos, os.SEEK_SET)
            while True:
                if len(self.__get_entry_unsafe()) == 0:
                    break
                self.log_len += 1
            new_commit_index = min(self.log_len, leader_commit)
            if self.commit_index() < new_commit_index:
                os.lseek(self.log_file, 0, os.SEEK_SET)
                self.__apply_entries_unsafe(new_commit_index)
            os.lseek(self.log_file, 0, os.SEEK_END)
            return True

    def get_log_tail(self, prev_log_index: int) -> tuple[int, bytes]:
        with self.log_lock:
            os.lseek(self.log_file, 0, os.SEEK_SET)
            cur_index = 0
            term = 0
            while cur_index < prev_log_index:
                entry = self.__get_entry_unsafe()
                term = entry['term']
                if len(entry) == 0:
                    os.lseek(self.log_file, 0, os.SEEK_END)
                    raise RuntimeError(f"Couldn't apply entries to store: log too small ({self.log_len})")
                cur_index += 1
            size = os.path.getsize(self.log_file) - os.lseek(self.log_file, 0, os.SEEK_CUR)
            return term, os.read(self.log_file, size)
