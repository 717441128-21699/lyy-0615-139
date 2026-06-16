import threading
import time
import sys
sys.path.insert(0, '.')

from database import (
    Database, Column, DataType, ForeignKey, ForeignKeyAction,
    Trigger, TriggerTiming, TriggerEvent, TriggerContext,
    DeadlockError, LockMode, TransactionStatus
)


def test_primary_key_unique_concurrent():
    print("=" * 60)
    print("测试1: 并发插入相同主键 - 只有一个能提交成功")
    print("=" * 60)

    db = Database()
    db.create_table("users", [
        Column("id", DataType.INT, nullable=False),
        Column("name", DataType.STRING),
    ], primary_key="id")

    results = {"T1": None, "T2": None}

    def insert_worker(tid):
        try:
            txn = db.begin()
            db.insert("users", {"id": 1, "name": f"User-{tid}"}, txn_id=txn)
            time.sleep(0.1)
            db.commit(txn)
            results[tid] = "committed"
        except ValueError as e:
            results[tid] = f"pk_error: {e}"
        except Exception as e:
            results[tid] = f"error: {e}"

    t1 = threading.Thread(target=insert_worker, args=("T1",))
    t2 = threading.Thread(target=insert_worker, args=("T2",))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    print(f"  T1 结果: {results['T1']}")
    print(f"  T2 结果: {results['T2']}")

    committed = sum(1 for v in results.values() if v == "committed")
    pk_errors = sum(1 for v in results.values() if isinstance(v, str) and "Duplicate primary key" in v)

    print(f"  成功提交: {committed}, 主键冲突: {pk_errors}")

    rows = db.select("users")
    print(f"  最终行数: {len(rows)} (应该为1)")
    print(f"  行数据: {[(r[1]['id'], r[1]['name']) for r in rows]}")

    assert committed == 1, f"应该只有1个事务提交成功，实际{committed}个"
    assert len(rows) == 1, f"最终应该只有1行，实际{len(rows)}行"
    assert pk_errors >= 1, "应该至少有1个主键冲突错误"
    print("  ✓ 并发插入相同主键测试通过\n")


def test_primary_key_same_txn():
    print("=" * 60)
    print("测试2: 同一事务内重复插入相同主键 - 直接报错")
    print("=" * 60)

    db = Database()
    db.create_table("items", [
        Column("id", DataType.INT, nullable=False),
        Column("name", DataType.STRING),
    ], primary_key="id")

    txn = db.begin()
    try:
        db.insert("items", {"id": 1, "name": "Item 1"}, txn_id=txn)
        print("  第一次插入 id=1 成功")

        db.insert("items", {"id": 1, "name": "Item 1 duplicate"}, txn_id=txn)
        print("  ✗ 第二次插入应该报错但没有")
        db.rollback(txn)
        assert False, "应该抛出主键重复错误"
    except ValueError as e:
        print(f"  第二次插入正确报错: {e}")
        db.rollback(txn)

    rows = db.select("items")
    print(f"  回滚后行数: {len(rows)} (应该为0)")
    assert len(rows) == 0, "回滚后应该没有数据"
    print("  ✓ 同一事务内重复主键测试通过\n")


def test_update_field_validation():
    print("=" * 60)
    print("测试3: 更新时字段校验 - 不存在的字段和类型错误")
    print("=" * 60)

    db = Database()
    db.create_table("products", [
        Column("id", DataType.INT, nullable=False),
        Column("name", DataType.STRING),
        Column("price", DataType.FLOAT),
        Column("stock", DataType.INT),
    ], primary_key="id")

    txn = db.begin()
    pid = db.insert("products", {"id": 1, "name": "Phone", "price": 999.99, "stock": 100}, txn_id=txn)
    db.commit(txn)
    print("  初始数据插入完成")

    txn2 = db.begin()
    try:
        db.update("products", pid, {"nonexistent_field": "value"}, txn_id=txn2)
        print("  ✗ 更新不存在的字段应该报错")
        db.rollback(txn2)
        assert False
    except ValueError as e:
        print(f"  更新不存在字段正确报错: {e}")

    try:
        txn3 = db.begin()
        db.update("products", pid, {"price": "not a number"}, txn_id=txn3)
        print("  ✗ 类型错误应该报错")
        db.rollback(txn3)
        assert False
    except ValueError as e:
        print(f"  类型错误正确报错: {e}")
        db.rollback(txn3)

    try:
        txn4 = db.begin()
        db.update("products", pid, {"stock": "123"}, txn_id=txn4)
        print("  ✗ 字符串赋给整数列应该报错")
        db.rollback(txn4)
        assert False
    except ValueError as e:
        print(f"  字符串赋给整数列正确报错: {e}")
        db.rollback(txn4)

    txn5 = db.begin()
    db.update("products", pid, {"name": "Smart Phone", "price": 1299.99}, txn_id=txn5)
    db.commit(txn5)
    print("  合法更新成功")

    result = db.select_by_pk("products", 1)
    print(f"  查询验证: name={result[1]['name']}, price={result[1]['price']}, stock={result[1]['stock']}")
    assert result[1]["name"] == "Smart Phone"
    assert result[1]["price"] == 1299.99
    assert result[1]["stock"] == 100
    print("  ✓ 更新字段校验测试通过\n")


def test_set_null_not_null_conflict():
    print("=" * 60)
    print("测试4: SET NULL 与 NOT NULL 冲突 - 删除父行直接失败")
    print("=" * 60)

    db = Database()
    db.create_table("categories", [
        Column("cat_id", DataType.INT, nullable=False),
        Column("name", DataType.STRING),
    ], primary_key="cat_id")

    db.create_table("products", [
        Column("prod_id", DataType.INT, nullable=False),
        Column("name", DataType.STRING),
        Column("cat_id", DataType.INT, nullable=False),
    ], primary_key="prod_id")

    db.add_foreign_key("products", ForeignKey(
        column="cat_id",
        ref_table="categories",
        ref_column="cat_id",
        on_delete=ForeignKeyAction.SET_NULL,
        on_update=ForeignKeyAction.SET_NULL
    ))
    print("  创建表，products.cat_id 是 NOT NULL，外键 SET NULL")

    txn = db.begin()
    db.insert("categories", {"cat_id": 1, "name": "Electronics"}, txn_id=txn)
    db.insert("products", {"prod_id": 1, "name": "Phone", "cat_id": 1}, txn_id=txn)
    db.commit(txn)
    print("  插入分类1和产品1（cat_id 不为空）")

    txn2 = db.begin()
    try:
        db.delete("categories", 1, txn_id=txn2)
        db.commit(txn2)
        print("  ✗ 应该报错但没有")
        assert False
    except ValueError as e:
        print(f"  正确拒绝删除: {e}")
        db.rollback(txn2)

    rows = db.select("products")
    print(f"  产品数: {len(rows)} (应该为1)")
    assert len(rows) == 1, "产品应该还存在"
    assert rows[0][1]["cat_id"] == 1, "cat_id 应该保持为1"
    print("  ✓ SET NULL 与 NOT NULL 冲突测试通过\n")


def test_delete_parent_insert_child_concurrency():
    print("=" * 60)
    print("测试5: 删除父行 vs 插入子行 - 正确互斥")
    print("=" * 60)

    db = Database()
    db.create_table("departments", [
        Column("dept_id", DataType.INT, nullable=False),
        Column("name", DataType.STRING),
    ], primary_key="dept_id")

    db.create_table("employees", [
        Column("emp_id", DataType.INT, nullable=False),
        Column("name", DataType.STRING),
        Column("dept_id", DataType.INT),
    ], primary_key="emp_id")

    db.add_foreign_key("employees", ForeignKey(
        column="dept_id",
        ref_table="departments",
        ref_column="dept_id",
        on_delete=ForeignKeyAction.RESTRICT,
        on_update=ForeignKeyAction.RESTRICT
    ))
    print("  创建 departments 和 employees，外键 RESTRICT")

    txn_init = db.begin()
    db.insert("departments", {"dept_id": 1, "name": "Engineering"}, txn_id=txn_init)
    db.commit(txn_init)
    print("  初始: 部门1存在")

    results = {"T1_delete": None, "T2_insert": None}

    def delete_parent():
        try:
            txn = db.begin()
            db.delete("departments", 1, txn_id=txn)
            time.sleep(0.2)
            db.commit(txn)
            results["T1_delete"] = "deleted"
        except ValueError as e:
            results["T1_delete"] = f"restricted: {e}"
            try:
                db.rollback(txn)
            except Exception:
                pass
        except Exception as e:
            results["T1_delete"] = f"error: {e}"
            try:
                db.rollback(txn)
            except Exception:
                pass

    def insert_child():
        try:
            time.sleep(0.1)
            txn = db.begin()
            db.insert("employees", {"emp_id": 1, "name": "Alice", "dept_id": 1}, txn_id=txn)
            time.sleep(0.2)
            db.commit(txn)
            results["T2_insert"] = "inserted"
        except ValueError as e:
            results["T2_insert"] = f"fk_error: {e}"
            try:
                db.rollback(txn)
            except Exception:
                pass
        except Exception as e:
            results["T2_insert"] = f"error: {e}"
            try:
                db.rollback(txn)
            except Exception:
                pass

    t1 = threading.Thread(target=delete_parent)
    t2 = threading.Thread(target=insert_child)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    print(f"  T1 (删父行) 结果: {results['T1_delete']}")
    print(f"  T2 (插子行) 结果: {results['T2_insert']}")

    dept_exists = db.select_by_pk("departments", 1) is not None
    emp_exists = db.select_by_pk("employees", 1) is not None

    print(f"  部门1是否存在: {dept_exists}")
    print(f"  员工1是否存在: {emp_exists}")

    if results["T1_delete"] == "deleted":
        print("  场景A: T1先执行删除，T2插入时部门已不存在 → T2报错")
        assert not dept_exists, "部门应该被删除"
        assert not emp_exists, "员工不应该存在"
        assert "fk_error" in results["T2_insert"], "T2应该报外键错误"
    elif results["T2_insert"] == "inserted":
        print("  场景B: T2先执行插入，T1删除时被RESTRICT阻止 → T1报错")
        assert dept_exists, "部门应该还存在"
        assert emp_exists, "员工应该存在"
        assert "restricted" in results["T1_delete"], "T1应该被RESTRICT阻止"
    else:
        print(f"  ⚠ 其他情况，需要检查")

    valid = (results["T1_delete"] == "deleted" and "fk_error" in str(results["T2_insert"])) or \
            (results["T2_insert"] == "inserted" and "restricted" in str(results["T1_delete"]))
    assert valid, "必须是其中一种有效场景"

    if dept_exists and emp_exists:
        print("  验证: 部门存在 → 员工引用有效 ✓")
    elif not dept_exists and not emp_exists:
        print("  验证: 部门删除 → 员工未插入 ✓")
    else:
        print(f"  ✗ 不一致状态: 部门存在={dept_exists}, 员工存在={emp_exists}")
        assert False, "数据不一致：父记录没了但子记录还引用它"

    print("  ✓ 删除父行与插入子行并发测试通过\n")


def test_deadlock_victim_auto_abort():
    print("=" * 60)
    print("测试6: 死锁受害者 - 自动ABORTED，禁止提交")
    print("=" * 60)

    db = Database()
    db.create_table("resources", [
        Column("id", DataType.INT, nullable=False),
        Column("name", DataType.STRING),
        Column("value", DataType.INT),
    ], primary_key="id")

    txn_init = db.begin()
    db.insert("resources", {"id": 1, "name": "A", "value": 0}, txn_id=txn_init)
    db.insert("resources", {"id": 2, "name": "B", "value": 0}, txn_id=txn_init)
    db.commit(txn_init)
    print("  初始化两个资源")

    results = {"T1": None, "T2": None}
    txn_ids = {"T1": None, "T2": None}
    victim_txn_id = [None]

    def txn1_worker():
        try:
            txn = db.begin()
            txn_ids["T1"] = txn
            db.select_by_pk("resources", 1, txn_id=txn)
            db.update("resources", 1, {"value": 1}, txn_id=txn)
            time.sleep(0.1)
            db.select_by_pk("resources", 2, txn_id=txn)
            db.update("resources", 2, {"value": 1}, txn_id=txn)
            db.commit(txn)
            results["T1"] = "committed"
        except DeadlockError as e:
            results["T1"] = "deadlock_victim"
            victim_txn_id[0] = txn
            try:
                db.commit(txn)
                results["T1"] = "error: commit_should_have_failed"
            except ValueError as commit_err:
                results["T1"] = f"deadlock_victim_commit_blocked: {commit_err}"
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
            txn_ids["T2"] = txn
            db.select_by_pk("resources", 2, txn_id=txn)
            db.update("resources", 2, {"value": 2}, txn_id=txn)
            time.sleep(0.1)
            db.select_by_pk("resources", 1, txn_id=txn)
            db.update("resources", 1, {"value": 2}, txn_id=txn)
            db.commit(txn)
            results["T2"] = "committed"
        except DeadlockError as e:
            results["T2"] = "deadlock_victim"
            victim_txn_id[0] = txn
            try:
                db.commit(txn)
                results["T2"] = "error: commit_should_have_failed"
            except ValueError as commit_err:
                results["T2"] = f"deadlock_victim_commit_blocked: {commit_err}"
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
    victim_blocked = sum(1 for v in results.values()
                         if isinstance(v, str) and "deadlock_victim_commit_blocked" in v)

    print(f"  成功提交: {committed}, 受害者提交被阻止: {victim_blocked}")

    if victim_txn_id[0]:
        txn_obj = db.txn_manager.get_transaction(victim_txn_id[0])
        if txn_obj:
            print(f"  受害者事务状态: {txn_obj.status}")
            assert txn_obj.status == TransactionStatus.ABORTED, "受害者应该是ABORTED状态"
        else:
            print(f"  受害者事务已被清理")

    assert committed == 1, "应该有1个事务提交成功"
    assert victim_blocked == 1, "受害者的提交应该被阻止"

    r1 = db.select_by_pk("resources", 1)
    r2 = db.select_by_pk("resources", 2)
    print(f"  最终值: R1={r1[1]['value']}, R2={r2[1]['value']}")

    if results["T1"] == "committed":
        assert r1[1]["value"] == 1 and r2[1]["value"] == 1
    else:
        assert r1[1]["value"] == 2 and r2[1]["value"] == 2

    print("  ✓ 死锁受害者自动回滚测试通过\n")


def run_all_new_tests():
    print("\n" + "=" * 60)
    print("  数据一致性增强 - 新功能测试")
    print("=" * 60 + "\n")

    try:
        test_primary_key_unique_concurrent()
    except Exception as e:
        print(f"  ✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        print()

    try:
        test_primary_key_same_txn()
    except Exception as e:
        print(f"  ✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        print()

    try:
        test_update_field_validation()
    except Exception as e:
        print(f"  ✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        print()

    try:
        test_set_null_not_null_conflict()
    except Exception as e:
        print(f"  ✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        print()

    try:
        test_delete_parent_insert_child_concurrency()
    except Exception as e:
        print(f"  ✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        print()

    try:
        test_deadlock_victim_auto_abort()
    except Exception as e:
        print(f"  ✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        print()

    print("=" * 60)
    print("  所有新测试完成")
    print("=" * 60)


if __name__ == "__main__":
    run_all_new_tests()
