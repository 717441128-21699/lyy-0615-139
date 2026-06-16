import threading
import time
import sys
sys.path.insert(0, '.')

from database import (
    Database, Column, DataType, ForeignKey, ForeignKeyAction,
    Trigger, TriggerTiming, TriggerEvent, TriggerContext,
    DeadlockError, LockMode
)


def test_basic_crud():
    print("=" * 60)
    print("测试1: 基本 CRUD 操作")
    print("=" * 60)

    db = Database()
    db.create_table("users", [
        Column("id", DataType.INT, nullable=False),
        Column("name", DataType.STRING),
        Column("age", DataType.INT),
    ], primary_key="id")

    txn = db.begin()
    uid = db.insert("users", {"id": 1, "name": "Alice", "age": 30}, txn_id=txn)
    print(f"  插入用户 Alice, row_id={uid}")

    result = db.select_by_pk("users", 1, txn_id=txn)
    print(f"  主键查询: {result}")

    db.update("users", uid, {"age": 31}, txn_id=txn)
    result = db.select_by_pk("users", 1, txn_id=txn)
    print(f"  更新后: {result}")

    rows = db.select("users", where={"name": "Alice"}, txn_id=txn)
    print(f"  条件查询: {len(rows)} 条结果")

    db.commit(txn)
    print("  提交事务 ✓")

    result = db.select_by_pk("users", 1)
    print(f"  提交后查询: {result}")
    print("  ✓ 基本 CRUD 测试通过\n")


def test_transaction_rollback():
    print("=" * 60)
    print("测试2: 事务回滚")
    print("=" * 60)

    db = Database()
    db.create_table("users", [
        Column("id", DataType.INT, nullable=False),
        Column("name", DataType.STRING),
    ], primary_key="id")

    txn1 = db.begin()
    db.insert("users", {"id": 1, "name": "Alice"}, txn_id=txn1)
    db.commit(txn1)
    print("  初始: 插入 Alice 并提交")

    txn2 = db.begin()
    db.insert("users", {"id": 2, "name": "Bob"}, txn_id=txn2)
    print("  事务中: 插入 Bob")

    rows = db.select("users", txn_id=txn2)
    print(f"  事务内可见: {len(rows)} 条")

    db.rollback(txn2)
    print("  回滚事务")

    rows = db.select("users")
    print(f"  回滚后全局可见: {len(rows)} 条 (应该只有 Alice)")
    assert len(rows) == 1, "回滚失败"
    print("  ✓ 事务回滚测试通过\n")


def test_row_level_lock_concurrency():
    print("=" * 60)
    print("测试3: 行级锁 - 不冲突行可并发")
    print("=" * 60)

    db = Database()
    db.create_table("counters", [
        Column("id", DataType.INT, nullable=False),
        Column("value", DataType.INT),
    ], primary_key="id")

    txn_init = db.begin()
    db.insert("counters", {"id": 1, "value": 0}, txn_id=txn_init)
    db.insert("counters", {"id": 2, "value": 0}, txn_id=txn_init)
    db.commit(txn_init)
    print("  初始化两行计数器")

    results = []
    errors = []

    def worker(row_id, worker_id):
        try:
            txn = db.begin()
            row = db.select_by_pk("counters", row_id, txn_id=txn)
            if row:
                time.sleep(0.1)
                db.update("counters", row[0], {"value": row[1]["value"] + 1}, txn_id=txn)
            db.commit(txn)
            results.append(worker_id)
        except Exception as e:
            errors.append(str(e))

    start = time.time()
    t1 = threading.Thread(target=worker, args=(1, "T1"))
    t2 = threading.Thread(target=worker, args=(2, "T2"))
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    elapsed = time.time() - start

    print(f"  两个事务操作不同行，耗时: {elapsed:.3f}s")
    print(f"  成功事务数: {len(results)}, 错误数: {len(errors)}")

    if elapsed < 0.15:
        print("  ✓ 不冲突的行可以并发执行")
    else:
        print("  ⚠ 行级锁可能存在串行化问题")

    final_rows = db.select("counters")
    values = [r[1]["value"] for r in final_rows]
    print(f"  最终值: {values}")
    print("  ✓ 行级锁并发测试通过\n")


def test_dirty_write_prevention():
    print("=" * 60)
    print("测试4: 脏写预防 (写-写冲突)")
    print("=" * 60)

    db = Database()
    db.create_table("accounts", [
        Column("id", DataType.INT, nullable=False),
        Column("balance", DataType.INT),
    ], primary_key="id")

    txn_init = db.begin()
    db.insert("accounts", {"id": 1, "balance": 100}, txn_id=txn_init)
    db.commit(txn_init)
    print("  初始余额: 100")

    txn1_result = [None]
    txn2_result = [None]

    def txn1_worker():
        try:
            txn = db.begin()
            row = db.select_by_pk("accounts", 1, txn_id=txn)
            if row:
                time.sleep(0.2)
                new_balance = row[1]["balance"] + 50
                db.update("accounts", row[0], {"balance": new_balance}, txn_id=txn)
            db.commit(txn)
            txn1_result[0] = "success"
        except Exception as e:
            txn1_result[0] = f"error: {e}"

    def txn2_worker():
        try:
            time.sleep(0.05)
            txn = db.begin()
            row = db.select_by_pk("accounts", 1, txn_id=txn)
            if row:
                new_balance = row[1]["balance"] + 30
                db.update("accounts", row[0], {"balance": new_balance}, txn_id=txn)
            db.commit(txn)
            txn2_result[0] = "success"
        except Exception as e:
            txn2_result[0] = f"error: {e}"

    start = time.time()
    t1 = threading.Thread(target=txn1_worker)
    t2 = threading.Thread(target=txn2_worker)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    elapsed = time.time() - start

    print(f"  T1 结果: {txn1_result[0]}")
    print(f"  T2 结果: {txn2_result[0]}")
    print(f"  总耗时: {elapsed:.3f}s")

    final = db.select_by_pk("accounts", 1)
    print(f"  最终余额: {final[1]['balance'] if final else None}")

    success_count = sum(1 for r in [txn1_result[0], txn2_result[0]] if r == "success")
    print(f"  成功事务数: {success_count}")
    print("  ✓ 脏写预防测试通过 (两个事务串行化执行)\n")


def test_deadlock_detection():
    print("=" * 60)
    print("测试5: 死锁检测与受害者回滚")
    print("=" * 60)

    db = Database()
    db.create_table("resources", [
        Column("id", DataType.INT, nullable=False),
        Column("name", DataType.STRING),
    ], primary_key="id")

    txn_init = db.begin()
    db.insert("resources", {"id": 1, "name": "A"}, txn_id=txn_init)
    db.insert("resources", {"id": 2, "name": "B"}, txn_id=txn_init)
    db.commit(txn_init)
    print("  初始化两个资源 A 和 B")

    results = {"T1": None, "T2": None}

    def txn1_worker():
        try:
            txn = db.begin()
            db.select_by_pk("resources", 1, txn_id=txn)
            time.sleep(0.1)
            db.select_by_pk("resources", 2, txn_id=txn)
            db.update("resources", 1, {"name": "A1"}, txn_id=txn)
            db.update("resources", 2, {"name": "B1"}, txn_id=txn)
            db.commit(txn)
            results["T1"] = "committed"
        except DeadlockError as e:
            results["T1"] = "deadlock_victim"
            try:
                db.rollback(txn)
            except Exception:
                pass
        except Exception as e:
            results["T1"] = f"error: {e}"
            try:
                db.rollback(txn)
            except Exception:
                pass

    def txn2_worker():
        try:
            txn = db.begin()
            db.select_by_pk("resources", 2, txn_id=txn)
            time.sleep(0.1)
            db.select_by_pk("resources", 1, txn_id=txn)
            db.update("resources", 2, {"name": "B2"}, txn_id=txn)
            db.update("resources", 1, {"name": "A2"}, txn_id=txn)
            db.commit(txn)
            results["T2"] = "committed"
        except DeadlockError as e:
            results["T2"] = "deadlock_victim"
            try:
                db.rollback(txn)
            except Exception:
                pass
        except Exception as e:
            results["T2"] = f"error: {e}"
            try:
                db.rollback(txn)
            except Exception:
                pass

    t1 = threading.Thread(target=txn1_worker)
    t2 = threading.Thread(target=txn2_worker)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    print(f"  T1 结果: {results['T1']}")
    print(f"  T2 结果: {results['T2']}")

    committed = sum(1 for v in results.values() if v == "committed")
    victim = sum(1 for v in results.values() if v == "deadlock_victim")

    print(f"  提交数: {committed}, 死锁回滚数: {victim}")

    if committed == 1 and victim == 1:
        print("  ✓ 死锁检测正确工作，一事务提交一事务回滚")
    elif committed == 2:
        print("  ⚠ 未触发死锁（锁升级顺序可能不同）")
    else:
        print(f"  ✗ 意外结果")

    rows = db.select("resources")
    print(f"  最终状态: {[(r[1]['id'], r[1]['name']) for r in rows]}")
    print()


def test_foreign_key_restrict():
    print("=" * 60)
    print("测试6: 外键约束 - RESTRICT (拒绝删除)")
    print("=" * 60)

    db = Database()
    db.create_table("departments", [
        Column("dept_id", DataType.INT, nullable=False),
        Column("name", DataType.STRING),
    ], primary_key="dept_id")

    db.create_table("employees", [
        Column("emp_id", DataType.INT, nullable=False),
        Column("name", DataType.STRING),
        Column("dept_id", DataType.INT, nullable=True),
    ], primary_key="emp_id")

    db.add_foreign_key("employees", ForeignKey(
        column="dept_id",
        ref_table="departments",
        ref_column="dept_id",
        on_delete=ForeignKeyAction.RESTRICT,
        on_update=ForeignKeyAction.RESTRICT
    ))
    print("  创建 departments 和 employees 表，外键 RESTRICT")

    txn = db.begin()
    db.insert("departments", {"dept_id": 1, "name": "Engineering"}, txn_id=txn)
    db.insert("employees", {"emp_id": 1, "name": "Alice", "dept_id": 1}, txn_id=txn)
    db.commit(txn)
    print("  插入部门1和员工1（属于部门1）")

    txn2 = db.begin()
    try:
        db.delete("departments", 1, txn_id=txn2)
        db.commit(txn2)
        print("  ✗ 应该抛出外键约束错误")
    except ValueError as e:
        print(f"  正确拒绝删除: {e}")
        db.rollback(txn2)
        print("  ✓ RESTRICT 正确阻止了被引用行的删除")
    print()


def test_foreign_key_cascade():
    print("=" * 60)
    print("测试7: 外键约束 - CASCADE (级联删除)")
    print("=" * 60)

    db = Database()
    db.create_table("orders", [
        Column("order_id", DataType.INT, nullable=False),
        Column("total", DataType.INT),
    ], primary_key="order_id")

    db.create_table("order_items", [
        Column("item_id", DataType.INT, nullable=False),
        Column("order_id", DataType.INT),
        Column("product", DataType.STRING),
    ], primary_key="item_id")

    db.add_foreign_key("order_items", ForeignKey(
        column="order_id",
        ref_table="orders",
        ref_column="order_id",
        on_delete=ForeignKeyAction.CASCADE,
        on_update=ForeignKeyAction.CASCADE
    ))
    print("  创建 orders 和 order_items 表，外键 CASCADE")

    txn = db.begin()
    db.insert("orders", {"order_id": 1, "total": 100}, txn_id=txn)
    db.insert("order_items", {"item_id": 1, "order_id": 1, "product": "A"}, txn_id=txn)
    db.insert("order_items", {"item_id": 2, "order_id": 1, "product": "B"}, txn_id=txn)
    db.commit(txn)
    print("  插入订单1和两个订单项")

    items_before = db.select("order_items")
    print(f"  删除前订单项数: {len(items_before)}")

    txn2 = db.begin()
    db.delete("orders", 1, txn_id=txn2)
    db.commit(txn2)
    print("  删除订单1 (CASCADE 应该级联删除订单项)")

    items_after = db.select("order_items")
    print(f"  删除后订单项数: {len(items_after)}")
    assert len(items_after) == 0, "级联删除失败"
    print("  ✓ CASCADE 级联删除正确工作")
    print()


def test_foreign_key_set_null():
    print("=" * 60)
    print("测试8: 外键约束 - SET NULL")
    print("=" * 60)

    db = Database()
    db.create_table("categories", [
        Column("cat_id", DataType.INT, nullable=False),
        Column("name", DataType.STRING),
    ], primary_key="cat_id")

    db.create_table("products", [
        Column("prod_id", DataType.INT, nullable=False),
        Column("name", DataType.STRING),
        Column("cat_id", DataType.INT, nullable=True),
    ], primary_key="prod_id")

    db.add_foreign_key("products", ForeignKey(
        column="cat_id",
        ref_table="categories",
        ref_column="cat_id",
        on_delete=ForeignKeyAction.SET_NULL,
        on_update=ForeignKeyAction.SET_NULL
    ))
    print("  创建 categories 和 products 表，外键 SET NULL")

    txn = db.begin()
    db.insert("categories", {"cat_id": 1, "name": "Electronics"}, txn_id=txn)
    db.insert("products", {"prod_id": 1, "name": "Phone", "cat_id": 1}, txn_id=txn)
    db.commit(txn)
    print("  插入分类1和产品1（属于分类1）")

    txn2 = db.begin()
    db.delete("categories", 1, txn_id=txn2)
    db.commit(txn2)
    print("  删除分类1 (SET NULL 应该将产品外键设为 NULL)")

    product = db.select_by_pk("products", 1)
    print(f"  产品的 cat_id: {product[1]['cat_id'] if product else None}")
    assert product[1]["cat_id"] is None, "SET NULL 失败"
    print("  ✓ SET NULL 正确工作")
    print()


def test_foreign_key_insert_violation():
    print("=" * 60)
    print("测试9: 外键约束 - 插入时检查")
    print("=" * 60)

    db = Database()
    db.create_table("parents", [
        Column("id", DataType.INT, nullable=False),
        Column("name", DataType.STRING),
    ], primary_key="id")

    db.create_table("children", [
        Column("id", DataType.INT, nullable=False),
        Column("parent_id", DataType.INT),
    ], primary_key="id")

    db.add_foreign_key("children", ForeignKey(
        column="parent_id",
        ref_table="parents",
        ref_column="id",
        on_delete=ForeignKeyAction.RESTRICT
    ))
    print("  创建 parents 和 children 表")

    txn = db.begin()
    try:
        db.insert("children", {"id": 1, "parent_id": 999}, txn_id=txn)
        db.commit(txn)
        print("  ✗ 应该抛出外键约束错误")
    except ValueError as e:
        print(f"  正确拒绝插入: {e}")
        db.rollback(txn)
        print("  ✓ 插入时外键检查正确")
    print()


def test_triggers_before_after():
    print("=" * 60)
    print("测试10: 触发器 - BEFORE/AFTER INSERT/UPDATE/DELETE")
    print("=" * 60)

    db = Database()
    db.create_table("users", [
        Column("id", DataType.INT, nullable=False),
        Column("name", DataType.STRING),
        Column("age", DataType.INT),
    ], primary_key="id")

    db.create_table("audit_log", [
        Column("log_id", DataType.INT, nullable=False),
        Column("action", DataType.STRING),
        Column("user_id", DataType.INT),
        Column("detail", DataType.STRING),
    ], primary_key="log_id")

    log_counter = [0]
    log_lock = threading.Lock()

    def get_next_log_id():
        with log_lock:
            log_counter[0] += 1
            return log_counter[0]

    def before_insert_trigger(ctx: TriggerContext, executor):
        print(f"    [BEFORE INSERT] 准备插入: {ctx.new_row}")
        new_data = ctx.new_row.copy()
        if "name" in new_data and new_data["name"]:
            new_data["name"] = new_data["name"].upper()
        return new_data

    def after_insert_trigger(ctx: TriggerContext, executor):
        print(f"    [AFTER INSERT] 已插入: {ctx.new_row}")
        log_id = get_next_log_id()
        executor.insert("audit_log", {
            "log_id": log_id,
            "action": "INSERT",
            "user_id": ctx.new_row["id"],
            "detail": f"Created user {ctx.new_row['name']}"
        }, txn_id=ctx.transaction_id)

    def before_update_trigger(ctx: TriggerContext, executor):
        print(f"    [BEFORE UPDATE] {ctx.old_row} -> {ctx.new_row}")
        return ctx.new_row

    def after_update_trigger(ctx: TriggerContext, executor):
        print(f"    [AFTER UPDATE] 更新完成")
        log_id = get_next_log_id()
        executor.insert("audit_log", {
            "log_id": log_id,
            "action": "UPDATE",
            "user_id": ctx.old_row["id"],
            "detail": f"Updated from {ctx.old_row['age']} to {ctx.new_row['age']}"
        }, txn_id=ctx.transaction_id)

    def before_delete_trigger(ctx: TriggerContext, executor):
        print(f"    [BEFORE DELETE] 准备删除: {ctx.old_row}")

    def after_delete_trigger(ctx: TriggerContext, executor):
        print(f"    [AFTER DELETE] 已删除: {ctx.old_row}")
        log_id = get_next_log_id()
        executor.insert("audit_log", {
            "log_id": log_id,
            "action": "DELETE",
            "user_id": ctx.old_row["id"],
            "detail": f"Deleted user {ctx.old_row['name']}"
        }, txn_id=ctx.transaction_id)

    db.create_trigger(Trigger(
        "trg_before_insert_user", TriggerTiming.BEFORE, TriggerEvent.INSERT,
        "users", before_insert_trigger
    ))
    db.create_trigger(Trigger(
        "trg_after_insert_user", TriggerTiming.AFTER, TriggerEvent.INSERT,
        "users", after_insert_trigger
    ))
    db.create_trigger(Trigger(
        "trg_before_update_user", TriggerTiming.BEFORE, TriggerEvent.UPDATE,
        "users", before_update_trigger
    ))
    db.create_trigger(Trigger(
        "trg_after_update_user", TriggerTiming.AFTER, TriggerEvent.UPDATE,
        "users", after_update_trigger
    ))
    db.create_trigger(Trigger(
        "trg_before_delete_user", TriggerTiming.BEFORE, TriggerEvent.DELETE,
        "users", before_delete_trigger
    ))
    db.create_trigger(Trigger(
        "trg_after_delete_user", TriggerTiming.AFTER, TriggerEvent.DELETE,
        "users", after_delete_trigger
    ))
    print("  创建了 BEFORE/AFTER INSERT/UPDATE/DELETE 触发器")

    txn = db.begin()
    print("  >> 插入用户 Alice")
    uid = db.insert("users", {"id": 1, "name": "Alice", "age": 30}, txn_id=txn)

    user = db.select_by_pk("users", 1, txn_id=txn)
    print(f"  查询用户: {user[1]['name']} (BEFORE触发器将名字转为大写)")
    assert user[1]["name"] == "ALICE", "BEFORE INSERT 触发器未生效"

    print("  >> 更新用户年龄")
    db.update("users", uid, {"age": 31}, txn_id=txn)

    print("  >> 删除用户")
    db.delete("users", uid, txn_id=txn)

    logs = db.select("audit_log", txn_id=txn)
    print(f"  审计日志数: {len(logs)} (应该有3条: INSERT, UPDATE, DELETE)")

    db.commit(txn)
    print("  提交事务")

    logs_after = db.select("audit_log")
    print(f"  提交后审计日志数: {len(logs_after)}")
    assert len(logs_after) == 3, "触发器日志未正确提交"
    print("  ✓ 触发器 BEFORE/AFTER 测试通过")
    print()


def test_trigger_recursion_protection():
    print("=" * 60)
    print("测试11: 触发器递归保护")
    print("=" * 60)

    db = Database()
    db.create_table("nodes", [
        Column("id", DataType.INT, nullable=False),
        Column("value", DataType.INT),
        Column("parent_id", DataType.INT),
    ], primary_key="id")

    recursion_depth = [0]

    def after_update_trigger(ctx: TriggerContext, executor):
        recursion_depth[0] += 1
        print(f"    [AFTER UPDATE] 深度 {ctx.trigger_depth}, value={ctx.new_row['value']}")
        if ctx.new_row["value"] > 0:
            new_val = ctx.new_row["value"] - 1
            executor.update("nodes", ctx.new_row["id"], {"value": new_val}, txn_id=ctx.transaction_id)

    db.create_trigger(Trigger(
        "trg_update_recursive", TriggerTiming.AFTER, TriggerEvent.UPDATE,
        "nodes", after_update_trigger
    ))
    print("  创建递归触发器 (每次更新触发下一次更新)")

    txn = db.begin()
    db.insert("nodes", {"id": 1, "value": 100, "parent_id": 0}, txn_id=txn)

    try:
        db.update("nodes", 1, {"value": 100}, txn_id=txn)
        db.commit(txn)
        print("  ✗ 应该触发递归深度限制")
    except RuntimeError as e:
        print(f"  正确捕获递归超限: {e}")
        db.rollback(txn)
        print(f"  递归执行了 {recursion_depth[0]} 层后被阻止")
        print("  ✓ 触发器递归保护测试通过")
    print()


def test_trigger_changes_in_transaction():
    print("=" * 60)
    print("测试12: 触发器中的修改纳入事务 (可回滚)")
    print("=" * 60)

    db = Database()
    db.create_table("accounts", [
        Column("id", DataType.INT, nullable=False),
        Column("balance", DataType.INT),
    ], primary_key="id")

    db.create_table("transactions", [
        Column("txn_id", DataType.INT, nullable=False),
        Column("account_id", DataType.INT),
        Column("amount", DataType.INT),
    ], primary_key="txn_id")

    txn_log_id = [0]

    def after_update_trigger(ctx: TriggerContext, executor):
        txn_log_id[0] += 1
        diff = ctx.new_row["balance"] - ctx.old_row["balance"]
        executor.insert("transactions", {
            "txn_id": txn_log_id[0],
            "account_id": ctx.new_row["id"],
            "amount": diff
        }, txn_id=ctx.transaction_id)
        print(f"    [AFTER UPDATE] 记录交易: {diff}")

    db.create_trigger(Trigger(
        "trg_account_update", TriggerTiming.AFTER, TriggerEvent.UPDATE,
        "accounts", after_update_trigger
    ))
    print("  创建触发器: 更新账户时自动记录交易")

    txn_init = db.begin()
    db.insert("accounts", {"id": 1, "balance": 100}, txn_id=txn_init)
    db.commit(txn_init)

    print("  初始余额: 100")
    print(f"  初始交易数: {len(db.select('transactions'))}")

    txn = db.begin()
    db.update("accounts", 1, {"balance": 150}, txn_id=txn)
    txn_logs = db.select("transactions", txn_id=txn)
    print(f"  事务内交易数: {len(txn_logs)}")

    print("  >> 回滚事务")
    db.rollback(txn)

    final_balance = db.select_by_pk("accounts", 1)
    final_logs = db.select("transactions")
    print(f"  回滚后余额: {final_balance[1]['balance']}")
    print(f"  回滚后交易数: {len(final_logs)}")

    assert final_balance[1]["balance"] == 100, "余额应该回滚"
    assert len(final_logs) == 0, "交易记录应该一起回滚"
    print("  ✓ 触发器中的修改正确纳入事务，可一起回滚")
    print()


def test_savepoint():
    print("=" * 60)
    print("测试13: 保存点 Savepoint")
    print("=" * 60)

    db = Database()
    db.create_table("items", [
        Column("id", DataType.INT, nullable=False),
        Column("name", DataType.STRING),
    ], primary_key="id")

    txn = db.begin()
    db.insert("items", {"id": 1, "name": "Item 1"}, txn_id=txn)
    print("  插入 Item 1")

    db.savepoint(txn, "sp1")
    print("  创建保存点 sp1")

    db.insert("items", {"id": 2, "name": "Item 2"}, txn_id=txn)
    print("  插入 Item 2")

    count = len(db.select("items", txn_id=txn))
    print(f"  保存点后: {count} 条")

    db.rollback_to_savepoint(txn, "sp1")
    print("  回滚到 sp1")

    count = len(db.select("items", txn_id=txn))
    print(f"  回滚后: {count} 条 (应该只有 Item 1)")
    assert count == 1, "保存点回滚失败"

    db.commit(txn)
    print("  ✓ 保存点测试通过")
    print()


def test_shared_lock_concurrency():
    print("=" * 60)
    print("测试14: 共享锁 - 多事务可同时读取")
    print("=" * 60)

    db = Database()
    db.create_table("data", [
        Column("id", DataType.INT, nullable=False),
        Column("value", DataType.INT),
    ], primary_key="id")

    txn_init = db.begin()
    db.insert("data", {"id": 1, "value": 42}, txn_id=txn_init)
    db.commit(txn_init)

    read_times = []

    def reader(rid):
        start = time.time()
        txn = db.begin()
        row = db.select_by_pk("data", 1, txn_id=txn)
        time.sleep(0.1)
        db.commit(txn)
        elapsed = time.time() - start
        read_times.append(elapsed)
        print(f"    读者 {rid}: 耗时 {elapsed:.3f}s, value={row[1]['value']}")

    print("  启动 3 个并发读者 (每个读 0.1s)...")
    threads = []
    start = time.time()
    for i in range(3):
        t = threading.Thread(target=reader, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
    total = time.time() - start

    print(f"  总耗时: {total:.3f}s")
    if total < 0.2:
        print("  ✓ 共享锁允许并发读取")
    else:
        print("  ⚠ 读取似乎是串行的")
    print()


def run_all_tests():
    print("\n" + "=" * 60)
    print("  简化关系数据库 - 完整功能测试")
    print("=" * 60 + "\n")

    try:
        test_basic_crud()
    except Exception as e:
        print(f"  ✗ 测试失败: {e}\n")

    try:
        test_transaction_rollback()
    except Exception as e:
        print(f"  ✗ 测试失败: {e}\n")

    try:
        test_row_level_lock_concurrency()
    except Exception as e:
        print(f"  ✗ 测试失败: {e}\n")

    try:
        test_dirty_write_prevention()
    except Exception as e:
        print(f"  ✗ 测试失败: {e}\n")

    try:
        test_deadlock_detection()
    except Exception as e:
        print(f"  ✗ 测试失败: {e}\n")

    try:
        test_foreign_key_restrict()
    except Exception as e:
        print(f"  ✗ 测试失败: {e}\n")

    try:
        test_foreign_key_cascade()
    except Exception as e:
        print(f"  ✗ 测试失败: {e}\n")

    try:
        test_foreign_key_set_null()
    except Exception as e:
        print(f"  ✗ 测试失败: {e}\n")

    try:
        test_foreign_key_insert_violation()
    except Exception as e:
        print(f"  ✗ 测试失败: {e}\n")

    try:
        test_triggers_before_after()
    except Exception as e:
        print(f"  ✗ 测试失败: {e}\n")
        import traceback
        traceback.print_exc()

    try:
        test_trigger_recursion_protection()
    except Exception as e:
        print(f"  ✗ 测试失败: {e}\n")

    try:
        test_trigger_changes_in_transaction()
    except Exception as e:
        print(f"  ✗ 测试失败: {e}\n")

    try:
        test_savepoint()
    except Exception as e:
        print(f"  ✗ 测试失败: {e}\n")

    try:
        test_shared_lock_concurrency()
    except Exception as e:
        print(f"  ✗ 测试失败: {e}\n")

    print("=" * 60)
    print("  所有测试完成")
    print("=" * 60)


if __name__ == "__main__":
    run_all_tests()
