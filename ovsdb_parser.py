import json
import sys

import pprint

txns = []

class Operation():
    def __init__(self, op):
        self.table = op['table']
        self.action = op['op']
        # A 3-element JSON array of the form [<column>, <mutator>, <value>]
        self.columns = [mutation[0] for mutation in op.get('mutations', [])]
        if self.table == 'NAT':
            pprint.pprint(op)

class Transaction():
    def __init__(self, cmd):
        self.db_name = cmd['params'][0]
        self._id = cmd['id']
        self.ops = [Operation(op) for op in cmd['params'][1:]]
        self.timestamp = float(cmd['time'])

class Update():
    def __init__(self, cmd):
        if cmd['method'] == 'update3':
            self.tables = cmd['params'][2].keys()
        else:
            self.tables = cmd['params'][1].keys()
        self.action = 'notif_' + cmd['method']
        self.timestamp = float(cmd['time'])

class Stats():
    # {'insert': {num_ops: 18, tables = {'ACL': 15, 'Port_Group': 7}}
    # {'ACL': {'insert': 17, 'update': 98, 'delete': 34}}
    def __init__(self):
        self._ops = {}
        self._tables = {}

    def add_operation(self, op):
        if op.table not in self._tables:
            self._tables[op.table] = {}
        self._tables[op.table][op.action] = self._tables[op.table].get(
            op.action, 0) + 1
        if op.action not in self._ops:
            self._ops[op.action] = {'num_ops': 0, 'tables': {}}
        self._ops[op.action]['num_ops'] += 1
        self._ops[op.action]['tables'][op.table] = (
            self._ops[op.action]['tables'].get(op.table, 0) + 1)

    def add_update(self, upd):
        if upd.action not in self._ops:
            self._ops[upd.action] = {'num_ops': 0, 'tables': {}}

        for table in upd.tables:
            self._ops[upd.action]['num_ops'] += 1
            if table not in self._tables:
                self._tables[table] = {}
            self._tables[table][upd.action] = self._tables[table].get(
                upd.action, 0) + 1
            self._ops[upd.action]['tables'][table] = (
                self._ops[upd.action]['tables'].get(table, 0) + 1)


stats = Stats()

def process_txn(txn):
    global txns
    global stats
    txns.append(txn)
    for op in txn.ops:
        stats.add_operation(op)


def parse_cmd(cmd):

    if 'method' not in cmd:
        return

    method = cmd['method']
    if method == 'transact':
        process_txn(Transaction(cmd))
    elif method.startswith('update'):
        stats.add_update(Update(cmd))


with open(sys.argv[1]) as fp:
    for line in fp:
        cmd = json.loads(line.strip())
        parse_cmd(cmd)
