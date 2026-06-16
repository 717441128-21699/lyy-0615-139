import threading
import time
import uuid
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple, Callable
from enum import Enum


class DataType(Enum):
    INT = "INT"
    STRING = "STRING"
    FLOAT = "FLOAT"
    BOOL = "BOOL"


class Column:
    def __init__(self, name: str, data_type: DataType, nullable: bool = True, default: Any = None):
        self.name = name
        self.data_type = data_type
        self.nullable = nullable
        self.default = default

    def validate(self, value: Any) -> bool:
        if value is None:
            return self.nullable
        if self.data_type == DataType.INT:
            return isinstance(value, int)
        elif self.data_type == DataType.STRING:
            return isinstance(value, str)
        elif self.data_type == DataType.FLOAT:
            return isinstance(value, (int, float))
        elif self.data_type == DataType.BOOL:
            return isinstance(value, bool)
        return False


class ForeignKeyAction(Enum):
    RESTRICT = "RESTRICT"
    CASCADE = "CASCADE"
    SET_NULL = "SET_NULL"


class ForeignKey:
    def __init__(self, column: str, ref_table: str, ref_column: str,
                 on_delete: ForeignKeyAction = ForeignKeyAction.RESTRICT,
                 on_update: ForeignKeyAction = ForeignKeyAction.RESTRICT):
        self.column = column
        self.ref_table = ref_table
        self.ref_column = ref_column
        self.on_delete = on_delete
        self.on_update = on_update


class TriggerTiming(Enum):
    BEFORE = "BEFORE"
    AFTER = "AFTER"


class TriggerEvent(Enum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class Trigger:
    def __init__(self, name: str, timing: TriggerTiming, event: TriggerEvent,
                 table_name: str, func: Callable):
        self.name = name
        self.timing = timing
        self.event = event
        self.table_name = table_name
        self.func = func


class Row:
    def __init__(self, row_id: int, data: Dict[str, Any]):
        self.row_id = row_id
        self.data = data.copy()


class LockMode(Enum):
    SHARED = "SHARED"
    EXCLUSIVE = "EXCLUSIVE"


class Lock:
    def __init__(self, table_name: str, row_id: int, mode: LockMode, transaction_id: str):
        self.table_name = table_name
        self.row_id = row_id
        self.mode = mode
        self.transaction_id = transaction_id


class WaitForGraph:
    def __init__(self):
        self.edges: Dict[str, Set[str]] = defaultdict(set)
        self.lock = threading.Lock()

    def add_edge(self, waiter: str, holder: str):
        with self.lock:
            self.edges[waiter].add(holder)

    def remove_edge(self, waiter: str, holder: str):
        with self.lock:
            if waiter in self.edges:
                self.edges[waiter].discard(holder)
                if not self.edges[waiter]:
                    del self.edges[waiter]

    def remove_transaction(self, txn_id: str):
        with self.lock:
            if txn_id in self.edges:
                del self.edges[txn_id]
            for waiter in list(self.edges.keys()):
                self.edges[waiter].discard(txn_id)
                if not self.edges[waiter]:
                    del self.edges[waiter]

    def detect_cycle(self) -> Optional[List[str]]:
        with self.lock:
            visited = set()
            rec_stack = set()
            path = []

            def dfs(node: str) -> Optional[List[str]]:
                visited.add(node)
                rec_stack.add(node)
                path.append(node)

                for neighbor in self.edges.get(node, set()):
                    if neighbor not in visited:
                        result = dfs(neighbor)
                        if result:
                            return result
                    elif neighbor in rec_stack:
                        idx = path.index(neighbor)
                        return path[idx:]

                path.pop()
                rec_stack.discard(node)
                return None

            for node in list(self.edges.keys()):
                if node not in visited:
                    cycle = dfs(node)
                    if cycle:
                        return cycle
            return None


class LockManager:
    def __init__(self, deadlock_detector: WaitForGraph):
        self.locks: Dict[Tuple[str, int], List[Lock]] = defaultdict(list)
        self.deadlock_detector = deadlock_detector
        self.lock = threading.Lock()
        self.transaction_priority: Dict[str, int] = {}

    def _set_priority(self, txn_id: str, priority: int):
        with self.lock:
            self.transaction_priority[txn_id] = priority

    def _can_acquire(self, new_lock: Lock, existing_locks: List[Lock]) -> bool:
        if not existing_locks:
            return True

        if new_lock.mode == LockMode.EXCLUSIVE:
            return len(existing_locks) == 1 and existing_locks[0].transaction_id == new_lock.transaction_id

        for lock in existing_locks:
            if lock.mode == LockMode.EXCLUSIVE and lock.transaction_id != new_lock.transaction_id:
                return False
        return True

    def _has_lock(self, table_name: str, row_id: int, txn_id: str, mode: LockMode) -> bool:
        key = (table_name, row_id)
        for lock in self.locks.get(key, []):
            if lock.transaction_id == txn_id:
                if mode == LockMode.SHARED:
                    return True
                if mode == LockMode.EXCLUSIVE and lock.mode == LockMode.EXCLUSIVE:
                    return True
        return False

    def _upgrade_lock(self, table_name: str, row_id: int, txn_id: str) -> bool:
        key = (table_name, row_id)
        existing = self.locks.get(key, [])
        for i, lock in enumerate(existing):
            if lock.transaction_id == txn_id and lock.mode == LockMode.SHARED:
                others = [l for l in existing if l.transaction_id != txn_id]
                if not others:
                    existing[i].mode = LockMode.EXCLUSIVE
                    return True
                return False
        return False

    def acquire_lock(self, table_name: str, row_id: int, mode: LockMode, txn_id: str,
                     timeout: float = 5.0) -> bool:
        start_time = time.time()

        while True:
            with self.lock:
                key = (table_name, row_id)
                existing = self.locks.get(key, [])

                if self._has_lock(table_name, row_id, txn_id, mode):
                    return True

                if mode == LockMode.EXCLUSIVE and self._has_lock(table_name, row_id, txn_id, LockMode.SHARED):
                    if self._upgrade_lock(table_name, row_id, txn_id):
                        self.deadlock_detector.remove_edge(txn_id, "*")
                        return True

                holders = set(l.transaction_id for l in existing if l.transaction_id != txn_id)

                if self._can_acquire(Lock(table_name, row_id, mode, txn_id), existing):
                    new_lock = Lock(table_name, row_id, mode, txn_id)
                    self.locks[key].append(new_lock)
                    for holder in holders:
                        self.deadlock_detector.remove_edge(txn_id, holder)
                    return True

                for holder in holders:
                    self.deadlock_detector.add_edge(txn_id, holder)

            cycle = self.deadlock_detector.detect_cycle()
            if cycle and txn_id in cycle:
                my_priority = self.transaction_priority.get(txn_id, 0)
                min_priority = min(self.transaction_priority.get(t, 0) for t in cycle)
                if my_priority <= min_priority:
                    self.release_all_locks(txn_id)
                    raise DeadlockError(f"Deadlock detected, transaction {txn_id} chosen as victim")

            elapsed = time.time() - start_time
            if elapsed >= timeout:
                with self.lock:
                    for holder in holders:
                        self.deadlock_detector.remove_edge(txn_id, holder)
                raise TimeoutError(f"Lock acquisition timeout for {table_name}:{row_id}")

            time.sleep(0.01)

    def release_all_locks(self, txn_id: str):
        with self.lock:
            keys_to_check = []
            for key, locks in self.locks.items():
                for lock in locks:
                    if lock.transaction_id == txn_id:
                        keys_to_check.append(key)
                        break

            for key in keys_to_check:
                self.locks[key] = [l for l in self.locks[key] if l.transaction_id != txn_id]
                if not self.locks[key]:
                    del self.locks[key]

            self.deadlock_detector.remove_transaction(txn_id)

    def get_locks_for_transaction(self, txn_id: str) -> List[Lock]:
        with self.lock:
            result = []
            for locks in self.locks.values():
                for lock in locks:
                    if lock.transaction_id == txn_id:
                        result.append(lock)
            return result


class DeadlockError(Exception):
    pass


class TransactionStatus(Enum):
    ACTIVE = "ACTIVE"
    COMMITTED = "COMMITTED"
    ABORTED = "ABORTED"


class Savepoint:
    def __init__(self, name: str, rows_snapshot: Dict[str, Dict[int, Dict[str, Any]]]):
        self.name = name
        self.rows_snapshot = rows_snapshot


class Transaction:
    def __init__(self, txn_id: str, isolation_level: str = "READ_COMMITTED"):
        self.txn_id = txn_id
        self.status = TransactionStatus.ACTIVE
        self.isolation_level = isolation_level
        self.local_changes: Dict[str, Dict[int, Dict[str, Any]]] = defaultdict(dict)
        self.deleted_rows: Dict[str, Set[int]] = defaultdict(set)
        self.inserted_rows: Dict[str, Set[int]] = defaultdict(set)
        self.savepoints: List[Savepoint] = []
        self.trigger_depth = 0
        self.trigger_stack: List[Tuple[str, TriggerEvent, TriggerTiming]] = []


class TransactionManager:
    def __init__(self, lock_manager: LockManager):
        self.transactions: Dict[str, Transaction] = {}
        self.lock_manager = lock_manager
        self.lock = threading.Lock()
        self._next_priority = 0

    def begin_transaction(self) -> str:
        txn_id = str(uuid.uuid4())
        txn = Transaction(txn_id)
        with self.lock:
            self.transactions[txn_id] = txn
            self._next_priority += 1
            self.lock_manager._set_priority(txn_id, self._next_priority)
        return txn_id

    def get_transaction(self, txn_id: str) -> Optional[Transaction]:
        with self.lock:
            return self.transactions.get(txn_id)

    def commit(self, txn_id: str):
        txn = self.get_transaction(txn_id)
        if not txn or txn.status != TransactionStatus.ACTIVE:
            raise ValueError(f"Invalid transaction: {txn_id}")
        txn.status = TransactionStatus.COMMITTED
        self.lock_manager.release_all_locks(txn_id)
        with self.lock:
            if txn_id in self.transactions:
                del self.transactions[txn_id]

    def rollback(self, txn_id: str):
        txn = self.get_transaction(txn_id)
        if not txn or txn.status != TransactionStatus.ACTIVE:
            raise ValueError(f"Invalid transaction: {txn_id}")
        txn.status = TransactionStatus.ABORTED
        txn.local_changes.clear()
        txn.deleted_rows.clear()
        txn.inserted_rows.clear()
        txn.savepoints.clear()
        self.lock_manager.release_all_locks(txn_id)
        with self.lock:
            if txn_id in self.transactions:
                del self.transactions[txn_id]

    def savepoint(self, txn_id: str, name: str):
        txn = self.get_transaction(txn_id)
        if not txn or txn.status != TransactionStatus.ACTIVE:
            raise ValueError(f"Invalid transaction: {txn_id}")
        snapshot = defaultdict(dict)
        for table, rows in txn.local_changes.items():
            snapshot[table] = rows.copy()
        deleted_snapshot = defaultdict(set)
        for table, rows in txn.deleted_rows.items():
            deleted_snapshot[table] = rows.copy()
        inserted_snapshot = defaultdict(set)
        for table, rows in txn.inserted_rows.items():
            inserted_snapshot[table] = rows.copy()
        txn.savepoints.append(Savepoint(name, {
            "changes": snapshot,
            "deleted": deleted_snapshot,
            "inserted": inserted_snapshot
        }))

    def rollback_to_savepoint(self, txn_id: str, name: str):
        txn = self.get_transaction(txn_id)
        if not txn or txn.status != TransactionStatus.ACTIVE:
            raise ValueError(f"Invalid transaction: {txn_id}")

        sp = None
        idx = -1
        for i, s in enumerate(txn.savepoints):
            if s.name == name:
                sp = s
                idx = i
                break

        if not sp:
            raise ValueError(f"Savepoint not found: {name}")

        txn.local_changes = defaultdict(dict, sp.rows_snapshot["changes"])
        txn.deleted_rows = defaultdict(set, sp.rows_snapshot["deleted"])
        txn.inserted_rows = defaultdict(set, sp.rows_snapshot["inserted"])
        txn.savepoints = txn.savepoints[:idx]


class TriggerManager:
    MAX_TRIGGER_DEPTH = 16

    def __init__(self):
        self.triggers: Dict[str, List[Trigger]] = defaultdict(list)
        self.lock = threading.Lock()

    def add_trigger(self, trigger: Trigger):
        with self.lock:
            self.triggers[trigger.table_name].append(trigger)

    def drop_trigger(self, table_name: str, trigger_name: str):
        with self.lock:
            if table_name in self.triggers:
                self.triggers[table_name] = [t for t in self.triggers[table_name] if t.name != trigger_name]

    def get_triggers(self, table_name: str, timing: TriggerTiming, event: TriggerEvent) -> List[Trigger]:
        with self.lock:
            return [t for t in self.triggers.get(table_name, [])
                    if t.timing == timing and t.event == event]

    def execute_triggers(self, table_name: str, timing: TriggerTiming, event: TriggerEvent,
                         txn: Transaction, old_row: Optional[Dict[str, Any]] = None,
                         new_row: Optional[Dict[str, Any]] = None,
                         executor=None) -> Optional[Dict[str, Any]]:
        if txn.trigger_depth >= self.MAX_TRIGGER_DEPTH:
            raise RuntimeError(f"Trigger recursion depth exceeded {self.MAX_TRIGGER_DEPTH}")

        triggers = self.get_triggers(table_name, timing, event)
        if not triggers:
            return new_row

        txn.trigger_depth += 1
        txn.trigger_stack.append((table_name, event, timing))

        try:
            current_row = new_row
            for trigger in triggers:
                ctx = TriggerContext(
                    table_name=table_name,
                    event=event,
                    timing=timing,
                    old_row=old_row,
                    new_row=current_row,
                    trigger_depth=txn.trigger_depth,
                    transaction_id=txn.txn_id
                )
                result = trigger.func(ctx, executor)
                if result is not None and timing == TriggerTiming.BEFORE and event in (TriggerEvent.INSERT, TriggerEvent.UPDATE):
                    current_row = result
            return current_row
        finally:
            txn.trigger_depth -= 1
            txn.trigger_stack.pop()


class TriggerContext:
    def __init__(self, table_name: str, event: TriggerEvent, timing: TriggerTiming,
                 old_row: Optional[Dict[str, Any]], new_row: Optional[Dict[str, Any]],
                 trigger_depth: int, transaction_id: str):
        self.table_name = table_name
        self.event = event
        self.timing = timing
        self.old_row = old_row
        self.new_row = new_row
        self.trigger_depth = trigger_depth
        self.transaction_id = transaction_id


class Table:
    def __init__(self, name: str, columns: List[Column], primary_key: str):
        self.name = name
        self.columns = {col.name: col for col in columns}
        self.primary_key = primary_key
        self.rows: Dict[int, Dict[str, Any]] = {}
        self.next_row_id = 1
        self.foreign_keys: List[ForeignKey] = []
        self.referenced_by: List[Tuple[str, ForeignKey]] = []
        self.indexes: Dict[str, Dict[Any, Set[int]]] = defaultdict(lambda: defaultdict(set))

    def _validate_row(self, data: Dict[str, Any]) -> Dict[str, Any]:
        validated = {}
        for col_name, col in self.columns.items():
            if col_name in data:
                value = data[col_name]
            elif col.default is not None:
                value = col.default
            else:
                value = None

            if not col.validate(value):
                raise ValueError(f"Invalid value for column {col_name}: {value}")

            validated[col_name] = value

        pk_value = validated.get(self.primary_key)
        if pk_value is None:
            raise ValueError(f"Primary key {self.primary_key} cannot be null")

        return validated

    def insert_row(self, row_id: int, data: Dict[str, Any]):
        validated = self._validate_row(data)
        self.rows[row_id] = validated
        self._update_indexes(row_id, validated)

    def update_row(self, row_id: int, data: Dict[str, Any]):
        if row_id not in self.rows:
            raise ValueError(f"Row {row_id} not found")

        old_data = self.rows[row_id]
        new_data = old_data.copy()
        for col_name, value in data.items():
            if col_name not in self.columns:
                raise ValueError(f"Unknown column: {col_name}")
            if not self.columns[col_name].validate(value):
                raise ValueError(f"Invalid value for column {col_name}")
            new_data[col_name] = value

        pk_changed = (self.primary_key in data and
                      old_data[self.primary_key] != new_data[self.primary_key])

        if pk_changed:
            new_pk = new_data[self.primary_key]
            for rid, row in self.rows.items():
                if rid != row_id and row[self.primary_key] == new_pk:
                    raise ValueError(f"Duplicate primary key: {new_pk}")

        self._remove_from_indexes(row_id, old_data)
        self.rows[row_id] = new_data
        self._update_indexes(row_id, new_data)

    def delete_row(self, row_id: int):
        if row_id not in self.rows:
            raise ValueError(f"Row {row_id} not found")
        old_data = self.rows[row_id]
        self._remove_from_indexes(row_id, old_data)
        del self.rows[row_id]

    def get_row(self, row_id: int) -> Optional[Dict[str, Any]]:
        return self.rows.get(row_id)

    def _update_indexes(self, row_id: int, data: Dict[str, Any]):
        for col_name, value in data.items():
            self.indexes[col_name][value].add(row_id)

    def _remove_from_indexes(self, row_id: int, data: Dict[str, Any]):
        for col_name, value in data.items():
            if col_name in self.indexes and value in self.indexes[col_name]:
                self.indexes[col_name][value].discard(row_id)
                if not self.indexes[col_name][value]:
                    del self.indexes[col_name][value]

    def find_by_pk(self, value: Any) -> Optional[int]:
        rows = self.indexes.get(self.primary_key, {}).get(value, set())
        return next(iter(rows)) if rows else None

    def find_by_column(self, column: str, value: Any) -> Set[int]:
        return self.indexes.get(column, {}).get(value, set()).copy()


class Database:
    def __init__(self):
        self.tables: Dict[str, Table] = {}
        self.deadlock_detector = WaitForGraph()
        self.lock_manager = LockManager(self.deadlock_detector)
        self.txn_manager = TransactionManager(self.lock_manager)
        self.trigger_manager = TriggerManager()
        self.db_lock = threading.Lock()
        self._next_row_id_lock = threading.Lock()
        self._next_row_ids: Dict[str, int] = defaultdict(int)

    def create_table(self, name: str, columns: List[Column], primary_key: str):
        with self.db_lock:
            if name in self.tables:
                raise ValueError(f"Table {name} already exists")

            table = Table(name, columns, primary_key)
            self.tables[name] = table
            self._next_row_ids[name] = 1
        return table

    def add_foreign_key(self, table_name: str, fk: ForeignKey):
        with self.db_lock:
            if table_name not in self.tables:
                raise ValueError(f"Table {table_name} not found")
            if fk.ref_table not in self.tables:
                raise ValueError(f"Referenced table {fk.ref_table} not found")

            table = self.tables[table_name]
            ref_table = self.tables[fk.ref_table]

            if fk.column not in table.columns:
                raise ValueError(f"Column {fk.column} not found in {table_name}")
            if fk.ref_column not in ref_table.columns:
                raise ValueError(f"Column {fk.ref_column} not found in {fk.ref_table}")

            table.foreign_keys.append(fk)
            ref_table.referenced_by.append((table_name, fk))

    def create_trigger(self, trigger: Trigger):
        self.trigger_manager.add_trigger(trigger)

    def _get_next_row_id(self, table_name: str) -> int:
        with self._next_row_id_lock:
            self._next_row_ids[table_name] += 1
            return self._next_row_ids[table_name] - 1

    def _get_table(self, table_name: str) -> Table:
        with self.db_lock:
            if table_name not in self.tables:
                raise ValueError(f"Table {table_name} not found")
            return self.tables[table_name]

    def insert(self, table_name: str, data: Dict[str, Any], txn_id: Optional[str] = None) -> int:
        txn = self._ensure_transaction(txn_id)
        auto_commit = getattr(txn, '_auto_commit', False)
        table = self._get_table(table_name)

        try:
            new_row = self.trigger_manager.execute_triggers(
                table_name, TriggerTiming.BEFORE, TriggerEvent.INSERT,
                txn, old_row=None, new_row=data.copy(), executor=self
            )
            if new_row is not None:
                data = new_row

            self._check_foreign_keys_on_insert(table, data, txn)

            row_id = self._get_next_row_id(table_name)

            self.lock_manager.acquire_lock(table_name, row_id, LockMode.EXCLUSIVE, txn.txn_id)

            validated = table._validate_row(data)

            pk_value = validated[table.primary_key]
            existing_pk_row = table.find_by_pk(pk_value)
            if existing_pk_row is not None:
                raise ValueError(f"Duplicate primary key: {pk_value}")

            txn.local_changes[table_name][row_id] = validated
            txn.inserted_rows[table_name].add(row_id)

            self.trigger_manager.execute_triggers(
                table_name, TriggerTiming.AFTER, TriggerEvent.INSERT,
                txn, old_row=None, new_row=validated, executor=self
            )

            if auto_commit:
                self.commit(txn.txn_id)

            return row_id
        except Exception:
            if auto_commit:
                try:
                    self.rollback(txn.txn_id)
                except Exception:
                    pass
            raise

    def update(self, table_name: str, row_id: int, data: Dict[str, Any],
               txn_id: Optional[str] = None):
        txn = self._ensure_transaction(txn_id)
        auto_commit = getattr(txn, '_auto_commit', False)
        table = self._get_table(table_name)

        old_row = self._get_row_for_txn(table, row_id, txn)
        if old_row is None:
            raise ValueError(f"Row {row_id} not found in {table_name}")

        try:
            self.lock_manager.acquire_lock(table_name, row_id, LockMode.EXCLUSIVE, txn.txn_id)

            new_row_data = old_row.copy()
            for k, v in data.items():
                new_row_data[k] = v

            new_row_data = self.trigger_manager.execute_triggers(
                table_name, TriggerTiming.BEFORE, TriggerEvent.UPDATE,
                txn, old_row=old_row, new_row=new_row_data, executor=self
            ) or new_row_data

            update_data = {k: new_row_data[k] for k in data.keys() if k in new_row_data}

            self._check_foreign_keys_on_update(table, row_id, old_row, new_row_data, txn)

            if table.primary_key in data:
                new_pk = new_row_data[table.primary_key]
                for rid, row in table.rows.items():
                    if rid != row_id and row[table.primary_key] == new_pk:
                        raise ValueError(f"Duplicate primary key: {new_pk}")
                for rid, row in txn.local_changes[table_name].items():
                    if rid != row_id and row and row[table.primary_key] == new_pk:
                        raise ValueError(f"Duplicate primary key: {new_pk}")

            txn.local_changes[table_name][row_id] = new_row_data
            if row_id in txn.deleted_rows[table_name]:
                txn.deleted_rows[table_name].discard(row_id)

            self.trigger_manager.execute_triggers(
                table_name, TriggerTiming.AFTER, TriggerEvent.UPDATE,
                txn, old_row=old_row, new_row=new_row_data, executor=self
            )

            if auto_commit:
                self.commit(txn.txn_id)
        except Exception:
            if auto_commit:
                try:
                    self.rollback(txn.txn_id)
                except Exception:
                    pass
            raise

    def delete(self, table_name: str, row_id: int, txn_id: Optional[str] = None):
        txn = self._ensure_transaction(txn_id)
        auto_commit = getattr(txn, '_auto_commit', False)
        table = self._get_table(table_name)

        old_row = self._get_row_for_txn(table, row_id, txn)
        if old_row is None:
            raise ValueError(f"Row {row_id} not found in {table_name}")

        try:
            self.lock_manager.acquire_lock(table_name, row_id, LockMode.EXCLUSIVE, txn.txn_id)

            self.trigger_manager.execute_triggers(
                table_name, TriggerTiming.BEFORE, TriggerEvent.DELETE,
                txn, old_row=old_row, new_row=None, executor=self
            )

            self._handle_foreign_keys_on_delete(table, row_id, old_row, txn)

            txn.deleted_rows[table_name].add(row_id)
            if row_id in txn.inserted_rows[table_name]:
                txn.inserted_rows[table_name].discard(row_id)
                if row_id in txn.local_changes[table_name]:
                    del txn.local_changes[table_name][row_id]

            self.trigger_manager.execute_triggers(
                table_name, TriggerTiming.AFTER, TriggerEvent.DELETE,
                txn, old_row=old_row, new_row=None, executor=self
            )

            if auto_commit:
                self.commit(txn.txn_id)
        except Exception:
            if auto_commit:
                try:
                    self.rollback(txn.txn_id)
                except Exception:
                    pass
            raise

    def select(self, table_name: str, where: Optional[Dict[str, Any]] = None,
               txn_id: Optional[str] = None) -> List[Tuple[int, Dict[str, Any]]]:
        txn = self._ensure_transaction(txn_id)
        auto_commit = getattr(txn, '_auto_commit', False)
        table = self._get_table(table_name)

        try:
            results = []
            all_row_ids = set(table.rows.keys())
            for rid in txn.local_changes[table_name]:
                if txn.local_changes[table_name][rid] is not None:
                    all_row_ids.add(rid)

            seen = set()
            for row_id in list(all_row_ids):
                if row_id in seen:
                    continue
                seen.add(row_id)

                if row_id in txn.deleted_rows[table_name]:
                    continue
                if row_id in txn.local_changes[table_name]:
                    row_data = txn.local_changes[table_name][row_id]
                    if row_data is None:
                        continue
                else:
                    row_data = table.rows[row_id]

                if where:
                    match = all(row_data.get(k) == v for k, v in where.items())
                    if not match:
                        continue

                results.append((row_id, row_data.copy()))

            for row_id, _ in results:
                try:
                    self.lock_manager.acquire_lock(table_name, row_id, LockMode.SHARED, txn.txn_id)
                except DeadlockError:
                    raise

            if auto_commit:
                self.commit(txn.txn_id)

            return results
        except Exception:
            if auto_commit:
                try:
                    self.rollback(txn.txn_id)
                except Exception:
                    pass
            raise

    def select_by_pk(self, table_name: str, pk_value: Any,
                     txn_id: Optional[str] = None) -> Optional[Tuple[int, Dict[str, Any]]]:
        txn = self._ensure_transaction(txn_id)
        auto_commit = getattr(txn, '_auto_commit', False)
        table = self._get_table(table_name)

        try:
            row_id = table.find_by_pk(pk_value)
            if row_id is None:
                for rid, rdata in txn.local_changes[table_name].items():
                    if rid in txn.inserted_rows[table_name] and rdata and rdata.get(table.primary_key) == pk_value:
                        if auto_commit:
                            self.commit(txn.txn_id)
                        return (rid, rdata.copy())
                if auto_commit:
                    self.commit(txn.txn_id)
                return None

            if row_id in txn.deleted_rows[table_name]:
                if auto_commit:
                    self.commit(txn.txn_id)
                return None

            row_data = txn.local_changes[table_name].get(row_id)
            if row_data is None and row_id not in txn.deleted_rows[table_name]:
                row_data = table.rows.get(row_id)

            if row_data:
                self.lock_manager.acquire_lock(table_name, row_id, LockMode.SHARED, txn.txn_id)
                result = (row_id, row_data.copy())
                if auto_commit:
                    self.commit(txn.txn_id)
                return result
            if auto_commit:
                self.commit(txn.txn_id)
            return None
        except Exception:
            if auto_commit:
                try:
                    self.rollback(txn.txn_id)
                except Exception:
                    pass
            raise

    def commit(self, txn_id: str):
        txn = self.txn_manager.get_transaction(txn_id)
        if not txn or txn.status != TransactionStatus.ACTIVE:
            raise ValueError(f"Invalid transaction: {txn_id}")

        table = None
        for table_name in txn.local_changes:
            table = self._get_table(table_name)

        for table_name, rows in txn.local_changes.items():
            table = self._get_table(table_name)
            for row_id, row_data in rows.items():
                if row_data is None:
                    if row_id in table.rows:
                        table.delete_row(row_id)
                else:
                    if row_id in table.rows:
                        old_data = table.rows[row_id]
                        table._remove_from_indexes(row_id, old_data)
                        table.rows[row_id] = row_data
                        table._update_indexes(row_id, row_data)
                    else:
                        table.rows[row_id] = row_data
                        table._update_indexes(row_id, row_data)

            for row_id in txn.deleted_rows[table_name]:
                if row_id in table.rows:
                    table.delete_row(row_id)

        self.txn_manager.commit(txn_id)

    def rollback(self, txn_id: str):
        self.txn_manager.rollback(txn_id)

    def begin(self) -> str:
        return self.txn_manager.begin_transaction()

    def savepoint(self, txn_id: str, name: str):
        self.txn_manager.savepoint(txn_id, name)

    def rollback_to_savepoint(self, txn_id: str, name: str):
        self.txn_manager.rollback_to_savepoint(txn_id, name)

    def _ensure_transaction(self, txn_id: Optional[str]) -> Transaction:
        if txn_id is None:
            new_id = self.txn_manager.begin_transaction()
            txn = self.txn_manager.get_transaction(new_id)
            txn._auto_commit = True
            return txn
        txn = self.txn_manager.get_transaction(txn_id)
        if not txn or txn.status != TransactionStatus.ACTIVE:
            raise ValueError(f"Invalid transaction: {txn_id}")
        return txn

    def _get_row_for_txn(self, table: Table, row_id: int, txn: Transaction) -> Optional[Dict[str, Any]]:
        if row_id in txn.deleted_rows[table.name]:
            return None
        if row_id in txn.local_changes[table.name]:
            data = txn.local_changes[table.name][row_id]
            return data.copy() if data else None
        if row_id in table.rows:
            return table.rows[row_id].copy()
        return None

    def _check_foreign_keys_on_insert(self, table: Table, data: Dict[str, Any], txn: Transaction):
        for fk in table.foreign_keys:
            fk_value = data.get(fk.column)
            if fk_value is None:
                if not table.columns[fk.column].nullable:
                    raise ValueError(f"Foreign key {fk.column} cannot be null")
                continue

            ref_table = self.tables[fk.ref_table]
            ref_row_id = ref_table.find_by_pk(fk_value) if fk.ref_column == ref_table.primary_key else None

            if ref_row_id is None:
                if fk.ref_column == ref_table.primary_key:
                    matches = ref_table.find_by_column(fk.ref_column, fk_value)
                else:
                    matches = ref_table.find_by_column(fk.ref_column, fk_value)
                if not matches:
                    in_txn = False
                    for rid, rdata in txn.local_changes[fk.ref_table].items():
                        if rdata and rdata.get(fk.ref_column) == fk_value and rid not in txn.deleted_rows[fk.ref_table]:
                            in_txn = True
                            break
                    if not in_txn:
                        raise ValueError(
                            f"Foreign key violation: {fk.column}={fk_value} "
                            f"references non-existent row in {fk.ref_table}.{fk.ref_column}"
                        )

    def _check_foreign_keys_on_update(self, table: Table, row_id: int,
                                       old_row: Dict[str, Any], new_row: Dict[str, Any],
                                       txn: Transaction):
        for fk in table.foreign_keys:
            if fk.column not in new_row or old_row.get(fk.column) == new_row.get(fk.column):
                continue

            fk_value = new_row.get(fk.column)
            if fk_value is None:
                if not table.columns[fk.column].nullable:
                    raise ValueError(f"Foreign key {fk.column} cannot be null")
                continue

            ref_table = self.tables[fk.ref_table]
            matches = ref_table.find_by_column(fk.ref_column, fk_value)

            if not matches:
                in_txn = False
                for rid, rdata in txn.local_changes[fk.ref_table].items():
                    if rdata and rdata.get(fk.ref_column) == fk_value and rid not in txn.deleted_rows[fk.ref_table]:
                        in_txn = True
                        break
                if not in_txn:
                    raise ValueError(
                        f"Foreign key violation: {fk.column}={fk_value} "
                        f"references non-existent row in {fk.ref_table}.{fk.ref_column}"
                    )

        for ref_table_name, fk in table.referenced_by:
            if fk.ref_column not in new_row or old_row.get(fk.ref_column) == new_row.get(fk.ref_column):
                continue

            old_ref_val = old_row.get(fk.ref_column)
            new_ref_val = new_row.get(fk.ref_column)

            ref_table = self.tables[ref_table_name]

            if fk.on_update == ForeignKeyAction.RESTRICT:
                referencing = ref_table.find_by_column(fk.column, old_ref_val)
                if referencing:
                    txn_referencing = False
                    for rid in referencing:
                        if rid not in txn.deleted_rows[ref_table_name]:
                            txn_referencing = True
                            break
                    if txn_referencing:
                        raise ValueError(
                            f"Cannot update {table.name}.{fk.ref_column}: "
                            f"referenced by {ref_table_name}.{fk.column}"
                        )
            elif fk.on_update == ForeignKeyAction.CASCADE:
                referencing_row_ids = ref_table.find_by_column(fk.column, old_ref_val)
                for rid in referencing_row_ids:
                    if rid not in txn.deleted_rows[ref_table_name]:
                        ref_row = self._get_row_for_txn(ref_table, rid, txn)
                        if ref_row:
                            self.update(ref_table_name, rid, {fk.column: new_ref_val}, txn.txn_id)
            elif fk.on_update == ForeignKeyAction.SET_NULL:
                referencing_row_ids = ref_table.find_by_column(fk.column, old_ref_val)
                for rid in referencing_row_ids:
                    if rid not in txn.deleted_rows[ref_table_name]:
                        ref_row = self._get_row_for_txn(ref_table, rid, txn)
                        if ref_row:
                            self.update(ref_table_name, rid, {fk.column: None}, txn.txn_id)

    def _handle_foreign_keys_on_delete(self, table: Table, row_id: int,
                                        row_data: Dict[str, Any], txn: Transaction):
        for ref_table_name, fk in table.referenced_by:
            ref_val = row_data.get(fk.ref_column)
            if ref_val is None:
                continue

            ref_table = self.tables[ref_table_name]
            referencing_row_ids = ref_table.find_by_column(fk.column, ref_val)

            active_referencing = []
            for rid in referencing_row_ids:
                if rid not in txn.deleted_rows[ref_table_name]:
                    current = self._get_row_for_txn(ref_table, rid, txn)
                    if current and current.get(fk.column) == ref_val:
                        active_referencing.append(rid)

            if not active_referencing:
                for rid, rdata in txn.local_changes[ref_table_name].items():
                    if rdata and rdata.get(fk.column) == ref_val and rid not in txn.deleted_rows[ref_table_name]:
                        active_referencing.append(rid)

            if not active_referencing:
                continue

            if fk.on_delete == ForeignKeyAction.RESTRICT:
                raise ValueError(
                    f"Cannot delete row from {table.name}: "
                    f"referenced by {ref_table_name}.{fk.column}"
                )
            elif fk.on_delete == ForeignKeyAction.CASCADE:
                for rid in active_referencing:
                    current = self._get_row_for_txn(ref_table, rid, txn)
                    if current:
                        self.delete(ref_table_name, rid, txn.txn_id)
            elif fk.on_delete == ForeignKeyAction.SET_NULL:
                for rid in active_referencing:
                    current = self._get_row_for_txn(ref_table, rid, txn)
                    if current:
                        self.update(ref_table_name, rid, {fk.column: None}, txn.txn_id)
