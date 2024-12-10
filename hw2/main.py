import sys
from flask import Flask, request, jsonify, redirect
import logging
import requests
import threading
import os
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
        del self.other_nodes[self.address]

        self.election_timeout = config["election_timeout"]
        self.heartbeat_timeout = config["heartbeat_timeout"]
        self.request_timeout = config["request_timeout"]
        self.uncertainty_multiplier = float(config["uncertainty_multiplier"])

        self.storage = Storage(config["data_dir"])
        self.state = 'follower' # Possible states: 'leader', 'candidate', 'follower'
        self.leader_address = None

        self.next_index = dict()
        self.match_index = dict()

        self.last_heartbeat = time.monotonic()
        self.apply_count = 0

        self.define_routes()

        self.heartbeat_trigger = threading.Condition()
        self.heartbeat_subscribe = threading.Condition()

        threading.Thread(target=self.start_heartbeat, daemon=True).start()
        threading.Thread(target=self.heartbeat_check, daemon=True).start()

    def define_routes(self):
        app = self.app

        @app.route('/items', methods=['POST'])
        def create_item():
            if self.state != 'leader':
                if self.leader_address != None:
                    return redirect(f'http://{self.leader_address}/items/{item_id}', code=302)
                return 'Leader is not choosed yet, try later', 503
            item_id = uuid.uuid4()
            with self.heartbeat_subscribe:
                self.storage.append_put_to_log(self.storage.term(), item_id.encode(), request.data)
                self.apply_count = 0
                if self.heartbeat_subscribe.wait_for(lambda : self.apply_count > (len(self.other_nodes) + 1) // 2, self.request_timeout):
                    return 'Timeout', 504
            return item_id, 201

        @app.route('/items/<string:item_id>', methods=['PUT'])
        def update_item(item_id):
            if self.state != 'leader':
                if self.leader_address != None:
                    return redirect(f'http://{self.leader_address}/items/{item_id}', code=302)
                return 'Leader is not choosed yet, try later', 503
            with self.heartbeat_subscribe:
                self.storage.append_put_to_log(self.storage.term(), item_id.encode(), request.data)
                self.apply_count = 0
                if self.heartbeat_subscribe.wait_for(lambda : self.apply_count > (len(self.other_nodes) + 1) // 2, self.request_timeout):
                    return 'Timeout', 504
            return 'Write was successful', 200

        @app.route('/items/<string:item_id>', methods=['DELETE'])
        def delete_item(item_id: str):
            if self.state != 'leader':
                if self.leader_address != None:
                    return redirect(f'http://{self.leader_address}/items/{item_id}', code=302)
                return 'Leader is not choosed yet, try later', 503
            with self.heartbeat_subscribe:
                if item_id in self.storage.data_store:
                    self.storage.append_delete_to_log(self.storage.term(), item_id.encode())
                    self.apply_count = 0
                    if self.heartbeat_subscribe.wait_for(lambda : self.apply_count > (len(self.other_nodes) + 1) // 2, self.request_timeout):
                        return 'Timeout', 504
                    return 'Item deleted', 200
            return 'Item not found', 404

        @app.route('/items/<string:item_id>', methods=['GET'])
        def get_item(item_id):
            if self.state == 'leader':
                if len(self.other_nodes) == 0:
                    replica_address = random.choice(self.other_nodes)
                    return redirect(f'http://{replica_address}/items/{item_id}', code=302)
                return 'No replicas available', 500
            if item_id in self.storage.data_store:
                return self.storage.data_store[item_id], 200
            return 'Item not found', 404

        @app.route('/append_entries', methods=['POST'])
        def append_entries():
            params = request.args
            if params.get('term') < self.storage.term():
                return jsonify({'term': self.storage.term(), 'success': False}), 200
            if self.state != 'follower':
                self.become_follower()
            self.leader_address = params.get('leader_id')
            success = False
            if self.storage.rewrite_log_tail(params.get('prev_log_index'), params.get('prev_log_term'), request.data):
                success = True
                if self.storage.commit_index < params.get('leader_commit'):
                    self.storage.commit_index = min(self.storage.last_index(), params.get('leader_commit'))
            self.storage.set_term(params.get('term'))
            return jsonify({'term': self.storage.term(), 'success': success}), 200

        @app.route('/request_vote', methods=['POST'])
        def request_vote():
            data = request.json
            if data.get('term') < self.storage.term():
                return jsonify({'term': self.storage.term(), 'vote_granted': False}), 200
            last_log_term = data.get('last_log_term')
            last_log_index = data.get('last_log_index')
            candidate_id = data.get('candidate_id')
            vote_granted = False
            if (self.storage.voted_for() is None or self.storage.voted_for() == candidate_id) \
               and (last_log_term, last_log_index) >= (self.storage.term(), self.storage.last_index()):
                self.storage.set_voted_for(candidate_id)
                vote_granted = True
            self.storage.set_term(data.get('term'))
            return jsonify({'term': self.storage.term(), 'vote_granted': vote_granted}), 200

    def run(self):
        self.app.run(host='0.0.0.0', port=self.port)

    def get_biased_timeout_seconds(self, timeout):
        return random.uniform(timeout, timeout * self.uncertainty_multiplier)

    def send_append_entries(self, node, timeout):
        try:
            while True:
                prev_log_term, entries = self.storage.get_log_tail(self.next_index[node] - 1)
                params = {
                    "term": self.storage.term(),
                    "leader_id": self.address,
                    "leader_commit": self.storage.commit_index,
                    "prev_log_index": self.next_index[node] - 1,
                    "prev_log_term": prev_log_term,
                }
                response = requests.get(f'http://{node}/append_entries', params=params, data=entries, timeout=timeout)
                data = response.json()
                if data.get('term') > self.storage.term():
                    self.storage.set_term(self.storage.term())
                if data.get('success'):
                    with self.heartbeat_subscribe:
                        self.apply_count += 1
                        self.heartbeat_subscribe.notify_all()
                    break
                else:
                    if self.next_index[node] == 0:
                        raise RuntimeError(f"Cannot replicate log from {self.address} to node at term {self.storage.term()}")
                    self.next_index[node] -= 1
        except Exception as e:
            logging.error(f"AppendEntries at term {self.storage.term()} to {node} failed: {e}")

    def start_heartbeat(self):
        while True:
            timeout = self.get_biased_timeout_seconds(self.heartbeat_timeout)

            with self.heartbeat_trigger:
                self.heartbeat_trigger.wait(timeout)
                if self.state == "follower":
                    continue

                threads = []
                for node in self.other_nodes:
                    threads += threading.Thread(target=self.send_append_entries, args=(node, timeout), daemon=False)

                for t in threads:
                    t.join()

    def heartbeat_check(self):
        while True:
            timeout = self.get_biased_timeout_seconds(self.election_timeout)
            time.sleep(timeout)
            if self.last_heartbeat + timeout < time.monotonic():
                self.become_candidate()

    def become_follower(self):
        self.state = "follower"
        self.match_index.clear()
        self.next_index.clear()

    def become_leader(self):
        logging.info(f"{self.address} becomes the new master for term {self.storage.term()}")
        self.state = "leader"
        self.master_address = self.address
        self.match_index.clear()
        self.next_index.clear()
        for node in self.other_nodes:
            self.match_index[node] = 0
            self.next_index[node] = self.storage.last_index() + 1

        with self.heartbeat_trigger:
            self.heartbeat_trigger.notify_all()

    def become_candidate(self):
        logging.info(f"{self.address} starting election")
        self.state = "candidate"
        while True:
            timeout = self.get_biased_timeout_seconds(self.election_timeout)
            start_timestamp = time.monotonic()
            self.storage.set_term(self.storage.term() + 1)
            self.storage.set_voted_for(self.address)
            votes = 1
            threads = []
            for node in self.other_nodes:
                def request_vote_sender():
                    try:
                        url = f'http://{node}/request_vote'
                        payload = {'term': self.term, 'candidate_id': self.address}
                        response = requests.post(url, json=payload, timeout=timeout)
                        if response.json().get('term') > self.storage.term():
                            self.become_follower()
                            self.storage.set_term(response.json().get('term'))
                            break
                        vote_granted = response.json().get('vote_granted')
                        if vote_granted:
                            votes += 1
                    except Exception as e:
                        logging.warning(f"VoteRequest at term {self.storage.term()} to {node} failed: {e}")
                threads += threading.Thread(target=request_vote_sender, daemon=False)
            for t in threads:
                t.join()

            if self.state == 'follower':
                break

            if start_timestamp + timeout >= time.monotonic() and \
               votes > (len(self.other_nodes) + 1) // 2:
                self.become_leader()
                break
            else:
                self.storage.set_voted_for(None)
                logging.info(f"{self.address} failed to become master at term {self.storage.term()}")

if __name__ == '__main__':
    address = sys.argv[1]
    port = int(sys.argv[2])
    config = dict()

    logging.basicConfig(level=logging.INFO, filename=os.path.join(config["data_dir"], "log.txt"), filemode="w")

    node = Node(address, port, config)
    node.run()
