import unittest
import os
import subprocess
import shutil
import requests
import uuid
import time
import logging
import sys
from urllib.parse import urlparse

from log_store import Storage

class TestLogStore(unittest.TestCase):
    def setUp(self):
        self.base_dir = './test_dir/store_test/'
        shutil.rmtree(self.base_dir, ignore_errors=True)
        os.makedirs(self.base_dir, exist_ok=True)
    
    def __make_storage(self, store=None):
        path = self.base_dir
        if store is None:
            path += str(uuid.uuid4())
            os.makedirs(path)
        else:
            path = store
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


    def test_recovery(self):
        prev_store = self.__make_storage()
        self.__append_mixed(prev_store)
        prev_store.apply_entries(7)

        cur_store = self.__make_storage(store=prev_store.data_dir)
        self.assertEqual(cur_store.data_store, {b'key1': b'3', b'key2': b'2', b'key4': b'5'})


class TestCluster(unittest.TestCase):
    def setUp(self):
        self.base_dir = './test_dir/cluster_test/'
        shutil.rmtree(self.base_dir, ignore_errors=True)
        os.makedirs(self.base_dir, exist_ok=True)
        subprocess.call(["docker", "compose", "up" ,"-d"])
        time.sleep(5)

    def __fix_location(self, host):
        o = urlparse(host)
        return o._replace(netloc="localhost:" + str(o.port)).geturl()

    def __create_item(self, host, data):
        resp = requests.post(host + "/items", data, allow_redirects=False)
        if resp.status_code == 302:
            new_loc = self.__fix_location(resp.headers['Location'])
            resp = requests.post(new_loc, data, allow_redirects=False)
        self.assertEqual(resp.status_code, 201, f"Message body: {resp.content}")
        return resp.content.decode()

    def __get_item(self, host, id):
        resp = requests.get(host + "/items/" + id, allow_redirects=False)
        if resp.status_code == 302:
            new_loc = self.__fix_location(resp.headers['Location'])
            resp = requests.get(new_loc, allow_redirects=False)
        return resp.content, resp.status_code

    def __put_item(self, host, id, data):
        resp = requests.put(host + "/items/" + id, data=data, allow_redirects=False)
        if resp.status_code == 302:
            new_loc = self.__fix_location(resp.headers['Location'])
            resp = requests.put(new_loc, data=data, allow_redirects=False)
        self.assertEqual(resp.status_code, 200, f"Message body: {resp.content}, id: {id}")

    def __delete_item(self, host, id):
        resp = requests.delete(host + "/items/" + id, allow_redirects=False)
        if resp.status_code == 302:
            new_loc = self.__fix_location(resp.headers['Location'])
            resp = requests.delete(new_loc, allow_redirects=False)
        self.assertEqual(resp.status_code, 200, f"Message body: {resp.content}, id: {id}")

    def test_election(self):
        resp_1 = requests.get("http://localhost:8081/debug/leader")
        self.assertEqual(resp_1.status_code, 200, f"Message body: {resp_1.content}")

        resp_2 = requests.get("http://localhost:8082/debug/leader")
        self.assertEqual(resp_2.status_code, 200, f"Message body: {resp_2.content}")

        resp_3 = requests.get("http://localhost:8082/debug/leader")
        self.assertEqual(resp_3.status_code, 200, f"Message body: {resp_3.content}")

        self.assertIn(resp_1.content, [b"kv-1:8081", b"kv-2:8082", b"kv-3:8083"])
        self.assertEqual(resp_1.content, resp_2.content)
        self.assertEqual(resp_2.content, resp_3.content)

    def test_simple(self):
        id = self.__create_item("http://localhost:8081", b'some data')
        time.sleep(0.5)

        data, code = self.__get_item("http://localhost:8081", id)
        self.assertEqual(code, 200, f"Message body: {data}, id: {id}")
        self.assertEqual(data, b'some data')

        self.__put_item("http://localhost:8081", id, b'another data')
        time.sleep(0.5)

        data, code = self.__get_item("http://localhost:8081", id)
        self.assertEqual(code, 200, f"Message body: {data}, id: {id}")
        self.assertEqual(data, b'another data')

        self.__delete_item("http://localhost:8081", id)
        time.sleep(0.5)

        data, code = self.__get_item("http://localhost:8081", id)
        self.assertEqual(code, 404, f"Message body: {data}, id: {id}")

    def test_recovery(self):
        id = self.__create_item("http://localhost:8081", b'some data')
        subprocess.call(["docker", "compose", "stop"])
        subprocess.call(["docker", "compose", "start"])
        time.sleep(5)

        data, code = self.__get_item("http://localhost:8081", id)
        self.assertEqual(code, 200, f"Message body: {data}, id: {id}")
        self.assertEqual(data, b'some data')

    def tearDown(self):
        subprocess.call(["docker", "compose", "down"])


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stderr, level=logging.WARN)
    unittest.main(failfast=True)
