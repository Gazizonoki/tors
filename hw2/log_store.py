import os


class Storage:
    def __init__(self, data_dir):
        flags = os.O_RDWR | os.O_CREAT | os.O_SYNC | os.O_DIRECT

        self.log_file = os.open(os.path.join(data_dir, 'wal.txt'), flags | os.O_APPEND, 0666)
        self.term_file = os.open(os.path.join(data_dir, 'term.txt'), flags, 0666)
        self.voted_for_file = os.open(os.path.join(data_dir, 'term.txt'), flags, 0666)
        self.commit_index = 0
        self.last_applied = 0
        self.data_store = dict()
        self.log_len = 0 # get real log len

    def set_term(self, term: int):
        os.write(self.term_file, str(term).encode())

    def term(self):
        return int(os.read(self.term_file, os.path.getsize(self.term_file)).decode())

    def set_voted_for(self, voted_for: str|None):
        if voted_for == None:
            os.truncate(self.voted_for_file, 0)
        else:
            os.write(self.voted_for_file, voted_for.encode())

    def voted_for(self):
        data = os.read(self.voted_for_file, os.path.getsize(self.voted_for_file)).decode()
        if len(data) == 0:
            return None
        return data
    
    def last_index(self):
        return self.log_len

    def append_put_to_log(self, term: int, key: bytes, value: bytes):
        os.write(self.log_file, b'0')
        os.write(self.log_file, term.to_bytes(8))
        os.write(self.log_file, len(key).to_bytes(8))
        os.write(self.log_file, len(value).to_bytes(8))
        os.write(self.log_file, key)
        os.write(self.log_file, value)
        self.log_len += 1

    def append_delete_to_log(self, term: int, key: bytes):
        os.write(self.log_file, b'1')
        os.write(self.log_file, term.to_bytes(8))
        os.write(self.log_file, len(key).to_bytes(8))
        os.write(self.log_file, key)
        self.log_len += 1

    def rewrite_log_tail(self, prev_log_index: int, prev_log_term: int, entries: bytes):
        return True

    def get_log_tail(self, prev_log_index: int) -> tuple[int, bytes]:
        pass
