import threading
import time
import sys
sys.path.insert(0, '.')

from database import (
    Database, Column, DataType, ForeignKey, ForeignKeyAction,
    Trigger, TriggerTiming, TriggerEvent, TriggerContext,
    DeadlockError, LockMode, TransactionStatus
)


def log(msg):
    print(f"  {msg}")


def test_crud_commit_not_hang():
    print("=" * 60)
    print("测试1: CRUD + 外键级联 提交不卡死")
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
        on_delete=ForeignKeyAction.CASCADE,
        on_update=ForeignKeyAction.CASCADE
    ))
    log("创建表: departments <-- CASCADE --> employees")

    start = time.time()

    # 场景1: 插入 + 提交
    txn1 = db.begin()
    did = db.insert("departments", {"dept_id": 1, "name": "Engineering"}, txn_id=txn1)
    eid1 = db.insert("employees", {"emp_id": 1, "name": "Alice", "dept_id": 1}, txn_id=txn1)
    eid2 = db.insert("employees", {"emp_id": 2, "name": "Bob", "dept_id": 1}, txn_id=txn1)
    db.commit(txn1)
    log("场景1完成: 插入部门+2员工 提交成功")

    # 验证查询
    depts = db.select("departments")
    emps = db.select("employees")
    assert len(depts) == 1, "应该有1个部门"
    assert len(emps) == 2, "应该有2个员工"
    log(f"  查询验证: {len(depts)}部门 {len(emps)}员工")

    # 场景2: 更新 + 提交
    txn2 = db.begin()
    db.update("employees", eid1, {"name": "Alice Smith"}, txn_id=txn2)
    db.update("departments", did, {"name": "Eng"}, txn_id=txn2)
    db.commit(txn2)
    log("场景2完成: 更新员工名+部门名 提交成功")

    # 场景3: 主键查询 + 条件查询
    result_pk = db.select_by_pk("employees", 1)
    assert result_pk is not None, "主键查询应该找到"
    assert result_pk[1]["name"] == "Alice Smith", "名字应该已更新"
    result_where = db.select("employees", where={"dept_id": 1})
    assert len(result_where) == 2, "条件查询应该找到2个"
    log("场景3完成: 主键查询+条件查询 正常")

    # 场景4: 外键级联删除 + 提交
    txn3 = db.begin()
    db.delete("departments", did, txn_id=txn3)
    db.commit(txn3)
    log("场景4完成: CASCADE级联删除部门 提交成功")

    emps_after = db.select("employees")
    depts_after = db.select("departments")
    assert len(depts_after) == 0, "部门应该全被删除"
    assert len(emps_after) == 0, "员工应该被CASCADE级联删除"
    log(f"  级联验证: {len(depts_after)}部门 {len(emps_after)}员工")

    # 场景5: 删除 + 提交 (自动事务)
    did99 = db.insert("departments", {"dept_id": 99, "name": "Temp"})
    db.delete("departments", did99)  # 用内部row_id删除
    remaining = db.select("departments")
    assert len(remaining) == 0, "自动事务删除后应该没有部门"
    log("场景5完成: 自动事务删除 正常")

    elapsed = time.time() - start
    log(f"总耗时: {elapsed:.3f}s")
    assert elapsed < 3.0, f"操作应该快速完成，实际{elapsed:.2f}s(可能卡死)"
    print("  ✓ 所有场景提交不卡死测试通过\n")


def test_trigger_modified_columns_validated():
    print("=" * 60)
    print("测试2: 触发器修改后整行严格校验")
    print("=" * 60)

    db = Database()
    db.create_table("test_table", [
        Column("id", DataType.INT, nullable=False),
        Column("name", DataType.STRING),
        Column("count", DataType.INT),
        Column("required", DataType.STRING, nullable=False),
    ], primary_key="id")

    # 场景1: 触发器把整数列改成字符串 - 应该拒绝
    def bad_type_trigger(ctx: TriggerContext, executor):
        row = ctx.new_row.copy()
        row["count"] = "not_an_int"
        return row

    db.create_trigger(Trigger(
        "bad_type", TriggerTiming.BEFORE, TriggerEvent.UPDATE,
        "test_table", bad_type_trigger
    ))

    txn_init = db.begin()
    rid = db.insert("test_table", {"id": 1, "name": "A", "count": 10, "required": "YES"}, txn_id=txn_init)
    db.commit(txn_init)
    log("初始数据插入完成")

    txn = db.begin()
    try:
        db.update("test_table", rid, {"name": "B"}, txn_id=txn)
        db.commit(txn)
        log("  ✗ 应该报错但没有")
        assert False
    except ValueError as e:
        log(f"触发器改整数列为字符串 正确报错: {type(e).__name__}: {str(e)[:60]}")
        try: db.rollback(txn)
        except: pass

    db.trigger_manager.drop_trigger("test_table", "bad_type")

    # 场景2: 触发器把非空列改成空 - 应该拒绝
    def null_required_trigger(ctx: TriggerContext, executor):
        row = ctx.new_row.copy()
        row["required"] = None
        return row

    db.create_trigger(Trigger(
        "null_req", TriggerTiming.BEFORE, TriggerEvent.UPDATE,
        "test_table", null_required_trigger
    ))

    txn = db.begin()
    try:
        db.update("test_table", rid, {"name": "C"}, txn_id=txn)
        db.commit(txn)
        log("  ✗ 应该报错但没有")
        assert False
    except ValueError as e:
        log(f"触发器改非空列为空 正确报错: {type(e).__name__}: {str(e)[:60]}")
        try: db.rollback(txn)
        except: pass

    db.trigger_manager.drop_trigger("test_table", "null_req")

    # 场景3: 触发器往表里塞不存在的字段 - 应该拒绝
    def extra_col_trigger(ctx: TriggerContext, executor):
        row = ctx.new_row.copy()
        row["not_a_real_column"] = "evil"
        return row

    db.create_trigger(Trigger(
        "extra_col", TriggerTiming.BEFORE, TriggerEvent.UPDATE,
        "test_table", extra_col_trigger
    ))

    txn = db.begin()
    try:
        db.update("test_table", rid, {"name": "D"}, txn_id=txn)
        db.commit(txn)
        log("  ✗ 应该报错但没有")
        assert False
    except ValueError as e:
        log(f"触发器塞不存在字段 正确报错: {type(e).__name__}: {str(e)[:60]}")
        try: db.rollback(txn)
        except: pass

    # 验证最终数据合法
    final = db.select_by_pk("test_table", 1)
    assert final[1]["name"] == "A", "所有坏修改都应被拒绝，name保持A"
    assert isinstance(final[1]["count"], int), "count应该是整数"
    assert final[1]["required"] == "YES", "required应该保持YES"
    assert "not_a_real_column" not in final[1], "不应该有额外字段"
    log(f"最终合法数据: {final[1]}")
    print("  ✓ 触发器整行校验测试通过\n")


def test_pk_update_unique_concurrent():
    print("=" * 60)
    print("测试3: 主键更新时的唯一性 - 两个事务改不同行为同主键")
    print("=" * 60)

    db = Database()
    db.create_table("items", [
        Column("id", DataType.INT, nullable=False),
        Column("value", DataType.STRING),
    ], primary_key="id")

    txn_init = db.begin()
    db.insert("items", {"id": 1, "value": "one"}, txn_id=txn_init)
    db.insert("items", {"id": 2, "value": "two"}, txn_id=txn_init)
    db.commit(txn_init)
    log("初始化两行: id=1 和 id=2")

    results = {"T1": None, "T2": None}
    final_values = {}

    def update_pk_worker(tid, from_pk, to_pk):
        try:
            txn = db.begin()
            row = db.select_by_pk("items", from_pk, txn_id=txn)
            time.sleep(0.1)
            db.update("items", row[0], {"id": to_pk}, txn_id=txn)
            db.commit(txn)
            results[tid] = "committed"
            final_values[tid] = db.select_by_pk("items", to_pk)
        except (ValueError, DeadlockError) as e:
            results[tid] = f"error: {type(e).__name__}: {str(e)[:40]}"
            try: db.rollback(txn)
            except: pass

    t1 = threading.Thread(target=update_pk_worker, args=("T1", 1, 999))
    t2 = threading.Thread(target=update_pk_worker, args=("T2", 2, 999))
    t1.start()
    t2.start()
    t1.join(timeout=15)
    t2.join(timeout=15)

    log(f"T1 (把id=1改成999): {results['T1']}")
    log(f"T2 (把id=2改成999): {results['T2']}")

    committed = sum(1 for v in results.values() if v == "committed")
    errors = sum(1 for v in results.values() if isinstance(v, str) and v.startswith("error"))

    log(f"成功提交: {committed}, 报错: {errors}")

    assert committed >= 1, "至少一个应该成功"
    assert errors >= 1, "至少一个应该报错(主键冲突或死锁)"

    rows = db.select("items")
    pk_values = [r[1]["id"] for r in rows]
    log(f"最终行: {[(r[1]['id'], r[1]['value']) for r in rows]}")
    log(f"主键列表: {pk_values}")

    assert len(pk_values) == len(set(pk_values)), f"不应该有重复主键! 实际: {pk_values}"
    log("无重复主键 ✓")

    id_999_count = sum(1 for pk in pk_values if pk == 999)
    assert id_999_count == 1, f"主键999应该只出现1次，实际{id_999_count}次"
    log("目标主键999只存在1条 ✓")

    print("  ✓ 主键更新唯一性测试通过\n")


def test_deadlock_victim_safe_rollback():
    print("=" * 60)
    print("测试4: 死锁受害者安全回滚 + 成功事务改动保留")
    print("=" * 60)

    db = Database()
    db.create_table("locks", [
        Column("id", DataType.INT, nullable=False),
        Column("name", DataType.STRING),
        Column("touched", DataType.STRING),
    ], primary_key="id")

    txn_init = db.begin()
    db.insert("locks", {"id": 1, "name": "A", "touched": "init"}, txn_id=txn_init)
    db.insert("locks", {"id": 2, "name": "B", "touched": "init"}, txn_id=txn_init)
    db.commit(txn_init)
    log("初始化两行锁资源")

    results = {"T1": None, "T2": None}
    txn_ids = {"T1": None, "T2": None}

    def txn_worker(tid, first_id, second_id, marker):
        try:
            txn = db.begin()
            txn_ids[tid] = txn
            db.select_by_pk("locks", first_id, txn_id=txn)
            db.update("locks", first_id, {"touched": f"{marker}_first"}, txn_id=txn)
            time.sleep(0.15)
            db.select_by_pk("locks", second_id, txn_id=txn)
            db.update("locks", second_id, {"touched": f"{marker}_second"}, txn_id=txn)
            db.commit(txn)
            results[tid] = "committed"
        except DeadlockError as e:
            results[tid] = f"deadlock_victim"
            try:
                db.rollback(txn)
                results[tid] = f"deadlock_victim_rollback_ok"
            except Exception as rb_err:
                results[tid] = f"deadlock_victim_rollback_ERROR: {type(rb_err).__name__}"
        except Exception as e:
            results[tid] = f"error: {type(e).__name__}: {str(e)[:30]}"
            try: db.rollback(txn)
            except: pass

    start = time.time()
    t1 = threading.Thread(target=txn_worker, args=("T1", 1, 2, "T1"))
    t2 = threading.Thread(target=txn_worker, args=("T2", 2, 1, "T2"))
    t1.start()
    t2.start()
    t1.join(timeout=15)
    t2.join(timeout=15)
    elapsed = time.time() - start

    log(f"T1 (锁1→锁2): {results['T1']}")
    log(f"T2 (锁2→锁1): {results['T2']}")
    log(f"耗时: {elapsed:.3f}s")

    committed_tid = None
    for tid in ["T1", "T2"]:
        if results[tid] == "committed":
            committed_tid = tid
        if "rollback_ERROR" in str(results[tid]):
            assert False, f"受害者回滚时抛错了: {tid}={results[tid]}"

    committed_count = sum(1 for v in results.values() if v == "committed")
    victim_ok_count = sum(1 for v in results.values() if v == "deadlock_victim_rollback_ok")

    log(f"成功提交: {committed_count}, 受害者回滚成功: {victim_ok_count}")

    assert committed_count == 1, f"应该有1个事务提交，实际{committed_count}"
    assert victim_ok_count == 1, f"应该有1个受害者安全回滚，实际{victim_ok_count}"

    # 验证成功事务的改动保留
    r1 = db.select_by_pk("locks", 1)
    r2 = db.select_by_pk("locks", 2)
    log(f"最终行1: touched={r1[1]['touched']}")
    log(f"最终行2: touched={r2[1]['touched']}")

    all_touched = [r1[1]["touched"], r2[1]["touched"]]
    init_count = sum(1 for t in all_touched if t == "init")

    if committed_tid == "T1":
        assert r1[1]["touched"] == "T1_first", "T1对行1的改动应该保留"
        assert r2[1]["touched"] == "T1_second", "T1对行2的改动应该保留"
        log("提交方是T1，改动正确保留 ✓")
    elif committed_tid == "T2":
        assert r1[1]["touched"] == "T2_second", "T2对行1的改动应该保留"
        assert r2[1]["touched"] == "T2_first", "T2对行2的改动应该保留"
        log("提交方是T2，改动正确保留 ✓")
    else:
        assert False, "应该有一个事务提交成功"

    # 验证受害者的改动全部消失
    assert init_count == 0, "不应该有任何行保持init状态(两行都被成功事务修改了)"
    log("受害者的改动全部被丢弃 ✓")

    print("  ✓ 死锁受害者安全回滚测试通过\n")


def run_all_tests():
    print("\n" + "=" * 60)
    print("  第二轮增强 - 修复测试")
    print("=" * 60 + "\n")

    tests = [
        ("CRUD+级联提交不卡死", test_crud_commit_not_hang),
        ("触发器修改整行校验", test_trigger_modified_columns_validated),
        ("主键更新唯一性", test_pk_update_unique_concurrent),
        ("死锁安全回滚", test_deadlock_victim_safe_rollback),
    ]

    passed = 0
    failed = 0
    for name, test_func in tests:
        try:
            test_func()
            passed += 1
        except Exception as e:
            print(f"\n  ✗ 测试 [{name}] 失败: {e}")
            import traceback
            traceback.print_exc()
            print()
            failed += 1

    print("=" * 60)
    print(f"  完成: {passed}通过 / {failed}失败 / 共{len(tests)}个")
    print("=" * 60)


if __name__ == "__main__":
    run_all_tests()
