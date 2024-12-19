from flask import Flask, request, jsonify, redirect
import logging
import requests
import threading
import os
import json
import uuid
import time
import random

from log_store import Storage

class Node:
    def __init__(self, address, port, config):
        self.app = Flask(__name__)
        self.address = address
        self.port = port

        self.other_nodes = config["nodes"].copy()
        self.other_nodes.remove(self.address)

        self.election_timeout = config["election_timeout_ms"]
        self.heartbeat_timeout = config["heartbeat_timeout_ms"]
        self.request_timeout = float(config["request_timeout_ms"]) / 1000
        self.uncertainty_multiplier = float(config["uncertainty_multiplier"])

        self.storage = Storage(config["data_dir"])
        self.state = 'follower' # Possible states: 'leader', 'candidate', 'follower'
        self.leader_address = None

        self.next_index = dict()
        self.match_index = dict()

        self.last_heartbeat = time.monotonic()
        self.waiters = {}

        self.define_routes()

        self.heartbeat_trigger = threading.Condition()
        self.waiters_lock = threading.Lock()

        threading.Thread(target=self.start_heartbeat, daemon=True).start()
        threading.Thread(target=self.heartbeat_check, daemon=True).start()

    def dump_all(self):
        logging.info("DUMP")
        logging.info(f'state: {self.state}')
        logging.info(f'last_index: {self.storage.last_index()}')
        logging.info(f'next_index: {self.next_index}')
        logging.info(f'match_index: {self.match_index}')

    def define_routes(self):
        app = self.app

        @app.route('/debug/leader', methods=['GET'])
        def get_leader():
            return self.leader_address, 200

        @app.route('/debug/dump_store', methods=['GET'])
        def dump_data_store():
            result = {}
            for key, value in self.storage.data_store.items():
                result[key.decode()] = value.decode()
            return jsonify(result), 200

        @app.route('/items', methods=['POST'])
        def create_item():
            if self.state != 'leader':
                if self.leader_address != None:
                    return redirect(f'http://{self.leader_address}/items', code=302)
                return 'Leader is not choosed yet, try later', 503
            item_id = uuid.uuid4()
            id = self.storage.append_put_to_log(self.storage.term(), str(item_id).encode(), request.data)
            waiter = threading.Condition()
            with self.waiters_lock:
                self.waiters[id] = waiter
            with waiter:
                if waiter.wait(timeout=self.request_timeout):
                    return str(item_id), 201
            return 'Timeout', 504

        @app.route('/items/<string:item_id>', methods=['PUT'])
        def update_item(item_id: str):
            if self.state != 'leader':
                if self.leader_address != None:
                    return redirect(f'http://{self.leader_address}/items/{item_id}', code=302)
                return 'Leader is not choosed yet, try later', 503
            id = self.storage.append_put_to_log(self.storage.term(), item_id.encode(), request.data)
            waiter = threading.Condition()
            with self.waiters_lock:
                self.waiters[id] = waiter
            with waiter:
                if waiter.wait(timeout=self.request_timeout):
                    return 'Write was successful', 200
            return 'Timeout', 504

        @app.route('/items/<string:item_id>', methods=['DELETE'])
        def delete_item(item_id: str):
            if self.state != 'leader':
                if self.leader_address != None:
                    return redirect(f'http://{self.leader_address}/items/{item_id}', code=302)
                return 'Leader is not choosed yet, try later', 503
            item_id_raw = item_id.encode()
            if item_id_raw in self.storage.data_store:
                id = self.storage.append_delete_to_log(self.storage.term(), item_id_raw)
                waiter = threading.Condition()
                with self.waiters_lock:
                    self.waiters[id] = waiter
                with waiter:
                    if waiter.wait(timeout=self.request_timeout):
                        return 'Item deleted', 200
                return 'Timeout', 504
            return 'Item not found', 404

        @app.route('/items/<string:item_id>', methods=['GET'])
        def get_item(item_id: str):
            if self.state == 'leader':
                if len(self.other_nodes) != 0:
                    replica_address = random.choice(self.other_nodes)
                    return redirect(f'http://{replica_address}/items/{item_id}', code=302)
                return jsonify('No replicas available'), 500
            item_id_raw = item_id.encode()
            if item_id_raw in self.storage.data_store:
                return self.storage.data_store[item_id_raw], 200
            return jsonify('Item not found'), 404

        @app.route('/append_entries', methods=['POST'])
        def append_entries():
            self.last_heartbeat = time.monotonic()
            params = request.args
            if int(params.get('term')) < self.storage.term():
                return jsonify({'term': self.storage.term(), 'success': False}), 200
            self.validate_term(int(params.get('term')))
            if self.state == 'candidate':
                self.become_follower()
            self.leader_address = params.get('leader_id')
            success = self.storage.rewrite_log_tail(int(params.get('prev_log_index')), int(params.get('prev_log_term')), 
                                                    int(params.get('leader_commit')), request.data)
            return jsonify({'term': self.storage.term(), 'success': success}), 200

        @app.route('/request_vote', methods=['POST'])
        def request_vote():
            data = request.json
            term = data.get('term')
            if term < self.storage.term():
                logging.info(f"Vote request denied for {data.get('candidate_id')}: term too small [{term} < {self.storage.term()}]")
                return jsonify({'term': self.storage.term(), 'vote_granted': False}), 200
            self.validate_term(term)
            last_log_term = data.get('last_log_term')
            last_log_index = data.get('last_log_index')
            candidate_id = data.get('candidate_id')
            vote_granted = False
            if (self.storage.voted_for() is None or self.storage.voted_for() == candidate_id) \
               and (last_log_term, last_log_index) >= self.storage.get_last_term():
                self.become_follower()
                self.storage.set_voted_for(candidate_id)
                vote_granted = True
                logging.info(f"Vote request accepted for {data.get('candidate_id')}")
            else:
                logging.info(f"""Vote request denied for {data.get('candidate_id')}:
                             [voted_for = {self.storage.voted_for()}],
                             [candidate last log = {(last_log_term, last_log_index)}]
                             [currest last log = {self.storage.get_last_term()}]""")
            return jsonify({'term': self.storage.term(), 'vote_granted': vote_granted}), 200

    def run(self):
        self.app.run(host='0.0.0.0', port=self.port, threaded=True)

    def get_biased_timeout_seconds(self, timeout):
        return random.uniform(timeout, timeout * self.uncertainty_multiplier) / 1000

    def commit_log(self, last_index):
        for new_commit_index in range(last_index, self.storage.commit_index(), -1):
            commited_cnt = sum(map(lambda x: int(x >= new_commit_index), self.match_index.values())) + 1
            if commited_cnt > (len(self.other_nodes) + 1) // 2:
                self.storage.apply_entries(new_commit_index)
                return new_commit_index
        return 0

    def send_append_entries(self, node, timeout):
        try:
            while True:
                prev_log_term, entries = self.storage.get_log_tail(self.next_index[node] - 1)
                last_index = self.storage.last_index()
                term = self.storage.term()
                params = {
                    "term": term,
                    "leader_id": self.address,
                    "leader_commit": self.storage.commit_index(),
                    "prev_log_index": self.next_index[node] - 1,
                    "prev_log_term": prev_log_term,
                }
                response = requests.post(f'http://{node}/append_entries', params=params, data=entries, timeout=timeout)
                data = response.json()
                if not self.validate_term(data.get('term')):
                    break
                if data.get('success'):
                    self.match_index[node] = last_index
                    self.next_index[node] = self.match_index[node] + 1
                    commit_index = self.commit_log(last_index)

                    with self.waiters_lock:
                        for id, waiter in self.waiters.items():
                            if id <= commit_index:
                                logging.info(f"Notify waiter with id {id}")
                                with waiter:
                                    waiter.notify_all()
                    break
                else:
                    logging.info(f"Failed append entries with result: term = {data.get('term')}, success = {data.get('success')}")
                    if self.next_index[node] <= 1:
                        raise RuntimeError(f"Cannot replicate log from {self.address} to node at term {self.storage.term()}")
                    self.next_index[node] -= 1
        except Exception as e:
            logging.error(f"AppendEntries at term {self.storage.term()} to {node} failed: {e}")
            self.dump_all()

    def start_heartbeat(self):
        while True:
            timeout = self.get_biased_timeout_seconds(self.heartbeat_timeout)

            with self.heartbeat_trigger:
                self.heartbeat_trigger.wait(timeout)
                if self.state != "leader":
                    continue

                threads = []
                for node in self.other_nodes:
                    threads.append(threading.Thread(target=self.send_append_entries, args=(node, timeout), daemon=False))
                    threads[-1].start()

                for t in threads:
                    t.join()

    def heartbeat_check(self):
        while True:
            timeout = self.get_biased_timeout_seconds(self.election_timeout)
            time.sleep(timeout)
            if self.state == 'follower' and self.last_heartbeat + timeout < time.monotonic():
                self.become_candidate()

    def validate_term(self, term: int):
        if term > self.storage.term():
            self.storage.set_term(term)
            self.storage.set_voted_for(None)
            self.become_follower()
            return False
        return True

    def clear_state(self):
        self.match_index.clear()
        self.next_index.clear()
        self.waiters.clear()

    def become_follower(self):
        logging.info(f"{self.address} becomes the follower for term {self.storage.term()}")
        self.state = "follower"
        self.clear_state()

    def become_leader(self, votes):
        logging.info(f"{self.address} becomes the new master for term {self.storage.term()} with votes: {votes}")
        self.state = "leader"
        self.leader_address = self.address
        self.clear_state()
        for node in self.other_nodes:
            self.match_index[node] = 0
            self.next_index[node] = self.storage.last_index() + 1

        with self.heartbeat_trigger:
            self.heartbeat_trigger.notify_all()

    def become_candidate(self):
        self.state = "candidate"
        self.clear_state()
        while True:
            logging.info(f"{self.address} starting election for term {self.storage.term() + 1}")
            timeout = self.get_biased_timeout_seconds(self.election_timeout)
            start_timestamp = time.monotonic()
            self.storage.set_term(self.storage.term() + 1)
            self.storage.set_voted_for(self.address)
            votes = [self.address]
            threads = []
            for node in self.other_nodes:
                def request_vote_sender():
                    try:
                        url = f'http://{node}/request_vote'
                        last_term, last_index = self.storage.get_last_term()
                        payload = {
                            'term': self.storage.term(),
                            'candidate_id': self.address,
                            'last_log_index': last_index,
                            'last_log_term': last_term,
                        }
                        response = requests.post(url, json=payload, timeout=timeout)
                        if response.status_code != 200:
                            raise RuntimeError(f"Message body: {response.content}")
                        logging.info(f"Vote response [term: {response.json()['term']}, vote_granted: {response.json()['vote_granted']}]")
                        if self.validate_term(response.json()['term']) \
                           and response.json()['vote_granted'] and node not in votes:
                            votes.append(node)
                    except Exception as e:
                        logging.error(f"VoteRequest at term {self.storage.term()} to {node} failed: {e}")
                        self.dump_all()
                threads.append(threading.Thread(target=request_vote_sender, daemon=False))
                threads[-1].start()
            for t in threads:
                t.join()

            if self.state == 'follower':
                break

            if start_timestamp + timeout >= time.monotonic() and \
               len(votes) > (len(self.other_nodes) + 1) // 2:
                self.become_leader(votes)
                break
            logging.info(f"{self.address} failed to become master at term {self.storage.term()} with votes: {votes}")

if __name__ == '__main__':
    address = os.getenv('KV_ADDRESS')
    port = int(os.getenv('KV_PORT'))
    with open('config.json') as json_file:
        config = json.load(json_file)

    logging.basicConfig(format='%(asctime)s %(levelname)s [%(funcName)s] %(message)s', level=logging.INFO, filename=os.path.join(config["data_dir"], "log.txt"), filemode="w")

    node = Node(address, port, config)
    node.run()
