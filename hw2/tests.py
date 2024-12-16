import unittest
import os
import subprocess
import shutil
import requests
import uuid
import time
import logging

from log_store import Storage

class TestLogStore(unittest.TestCase):
    def setUp(self):
        self.base_dir = './test_dir/store_test/'
        shutil.rmtree(self.base_dir, ignore_errors=True)
        os.makedirs(self.base_dir, exist_ok=True)
    
    def __make_storage(self):
        path = self.base_dir + str(uuid.uuid4())
        os.makedirs(path)
        return Storage(path)

    def __append_puts(self, storage: Storage):
        storage.append_put_to_log(1, b'key1', b'1')
        storage.append_put_to_log(1, b'key2', b'2')
        storage.append_put_to_log(1, b'key1', b'3')

    def __append_mixed(self, storage: Storage):
        storage.append_put_to_log(1, b'key1', b'1')
        storage.append_put_to_log(1, b'key2', b'2')
        storage.append_delete_to_log(1, b'key1')
        storage.append_put_to_log(1, b'key1', b'3')
        storage.append_put_to_log(1, b'key3', b'4')
        storage.append_put_to_log(1, b'key4', b'5')
        storage.append_delete_to_log(1, b'key3')

    def test_term(self):
        storage = self.__make_storage()
        self.assertEqual(storage.term(), 0)
        storage.set_term(10)
        self.assertEqual(storage.term(), 10)
        storage.set_term(11)
        self.assertEqual(storage.term(), 11)

    def test_voted_for(self):
        storage = self.__make_storage()
        self.assertEqual(storage.voted_for(), None)
        storage.set_voted_for('first_node:8080')
        self.assertEqual(storage.voted_for(), 'first_node:8080')
        storage.set_voted_for('second_node:8080')
        self.assertEqual(storage.voted_for(), 'second_node:8080')
        storage.set_voted_for(None)
        self.assertEqual(storage.voted_for(), None)

    def test_puts_full_commit(self):
        storage = self.__make_storage()
        self.__append_puts(storage)
        storage.apply_entries(3)
        self.assertEqual(storage.data_store, {b'key1': b'3', b'key2': b'2'})

    def test_puts_part_commit(self):
        storage = self.__make_storage()
        self.__append_puts(storage)
        storage.apply_entries(2)
        self.assertEqual(storage.data_store, {b'key1': b'1', b'key2': b'2'})

    def test_mixed_full_commit(self):
        storage = self.__make_storage()
        self.__append_mixed(storage)
        storage.apply_entries(7)
        self.assertEqual(storage.data_store, {b'key1': b'3', b'key2': b'2', b'key4': b'5'})

    def test_mixed_part_commit(self):
        storage = self.__make_storage()
        self.__append_mixed(storage)
        storage.apply_entries(5)
        self.assertEqual(storage.data_store, {b'key1': b'3', b'key2': b'2', b'key3': b'4'})

    def test_replicate_log_simple(self):
        leader = self.__make_storage()
        replica = self.__make_storage()
        self.__append_mixed(leader)
        leader.apply_entries(7)
        term, entries = leader.get_log_tail(0)
        self.assertTrue(replica.rewrite_log_tail(0, term, 7, entries))

        data = {b'key1': b'3', b'key2': b'2', b'key4': b'5'}
        self.assertEqual(leader.data_store, data)
        self.assertEqual(replica.data_store, data)

    def test_replicate_log_common_prefix(self):
        leader = self.__make_storage()
        replica = self.__make_storage()
        self.__append_mixed(leader)
        leader.append_put_to_log(2, b'key1', b'4')
        leader.append_put_to_log(3, b'key3', b'0')
        leader.append_delete_to_log(4, b'key4')
        leader.apply_entries(10)

        self.__append_mixed(replica)
        replica.apply_entries(7)
        term, entries = leader.get_log_tail(7)
        self.assertTrue(replica.rewrite_log_tail(7, term, 10, entries))

        data = {b'key1': b'4', b'key2': b'2', b'key3': b'0'}
        self.assertEqual(leader.data_store, data)
        self.assertEqual(replica.data_store, data)

    def test_replicate_log_fail(self):
        leader = self.__make_storage()
        replica = self.__make_storage()
        self.__append_mixed(leader)
        leader.append_put_to_log(2, b'key1', b'4')
        leader.append_put_to_log(3, b'key3', b'0')
        leader.append_delete_to_log(4, b'key4')
        leader.apply_entries(10)

        self.__append_mixed(replica)
        replica.append_put_to_log(3, b'key5', b'11')
        replica.apply_entries(8)
        term, entries = leader.get_log_tail(8)
        self.assertFalse(replica.rewrite_log_tail(8, term, 10, entries))

        self.assertEqual(leader.data_store, {b'key1': b'4', b'key2': b'2', b'key3': b'0'})
        self.assertEqual(replica.data_store, {b'key1': b'3', b'key2': b'2', b'key4': b'5', b'key5': b'11'})


class TestCluster(unittest.TestCase):
    def setUp(self):
        self.base_dir = './test_dir/cluster_test/'
        shutil.rmtree(self.base_dir, ignore_errors=True)
        os.makedirs(self.base_dir, exist_ok=True)
        subprocess.call(["docker", "compose", "up" ,"-d"])
        time.sleep(2)

    def test_simple(self):
        resp = requests.post("http://localhost:8081/items", b'some_data')
        self.assertEqual(resp.status_code, 201, f"Message body: {resp.content}")
        id = resp.content
        logging.info(id)

    def tearDown(self):
        subprocess.call(["docker", "compose", "down"])


if __name__ == "__main__":
    unittest.main()
