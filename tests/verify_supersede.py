"""复测报告接替功能的正确性与并发验证（直连数据库，不走 HTTP）。

覆盖：
1. 基本接替：原子发布新报告 + 接替关系，旧版与快照永久保留，现行报告切换
2. 沿革查询：按链内任一报告 ID 查完整沿革、每次接替原因、当前有效报告
3. 多次接替成链（A->B->C），链线性、不分叉、不成环
4. 重复/并发接替同一旧版：至多成功一次，败方 409 且返回现行报告标识
5. 新编号全局唯一：与既有报告/旧版自身编号冲突 -> 409，旧报告仍有效、无接替记录
6. 放行规则沿用：禁用优先（422 INSTRUMENT_DISABLED）、无校准、区间外、未通过
   —— 被阻断时旧报告仍有效，不留下接替记录，新报告不落库
7. 非空接替原因校验；接替不存在报告 -> 404
8. 兼容性：单报告提交/查询的请求与响应字段保持兼容（新增字段为附加）
"""
import os

# 并发赛跑线程数超过默认连接池上限，测试进程内调大。
# 必须在导入 app.config 之前设置。
os.environ.setdefault("DB_POOL_MAX_SIZE", "30")
os.environ.setdefault("DB_POOL_MIN_SIZE", "5")

import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from fastapi import HTTPException
from pydantic import ValidationError

import app.db as db
from app.models import (
    CalibrationCreate,
    DisableCreate,
    DisableRevoke,
    ReportSubmit,
    ReportSupersede,
)
from app.repository import (
    create_calibration,
    get_disable,
    publish_report,
    register_disable,
    report_history,
    revoke_disable,
    supersede_report,
)
from app.transactions import run_tx

failures = []

db.open_pool()


def check(name, cond, detail=""):
    print(("PASS" if cond else "FAIL"), "-", name, detail)
    if not cond:
        failures.append(name)


def reset():
    with db.pool.connection() as c:
        c.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    db.init_db()


def http_call(fn, *args):
    try:
        return 200, run_tx(lambda conn: fn(conn, *args))
    except HTTPException as e:
        return e.status_code, e.detail


def locked_publish(payload: ReportSubmit):
    """与生产路由一致：发布按仪器咨询锁串行。"""
    try:
        result = run_tx(lambda conn: publish_report(conn, payload),
                        advisory_lock=db.instrument_lock_key(payload.instrument))
        return 200, result
    except HTTPException as e:
        return e.status_code, e.detail


def locked_supersede(report_id, payload: ReportSupersede):
    """与生产路由一致：接替按复测仪器咨询锁串行。"""
    try:
        result = run_tx(lambda conn: supersede_report(conn, report_id, payload),
                        advisory_lock=db.instrument_lock_key(payload.instrument))
        return 201, result
    except HTTPException as e:
        return e.status_code, e.detail


def T(y, mo=1, d=1, h=0, mi=0, s=0):
    return datetime(y, mo, d, h, mi, s, tzinfo=timezone.utc)


def passed_calibration(instrument, frm=T(2025, 1, 1), to=T(2030, 1, 1)):
    status, row = http_call(create_calibration, CalibrationCreate(
        instrument=instrument, valid_from=frm, valid_to=to, passed=True))
    assert status == 200, row
    return row


def chain_state(chain_id):
    """直查数据库：链上报告与接替记录。"""
    with db.pool.connection() as c:
        reports = c.execute(
            "SELECT * FROM reports WHERE chain_id = %s ORDER BY id",
            (chain_id,)).fetchall()
        sups = c.execute(
            "SELECT * FROM report_supersessions WHERE chain_id = %s ORDER BY id",
            (chain_id,)).fetchall()
    return reports, sups


# ---------- 1. 基本接替：原子发布 + 留痕 + 现行切换 ----------
reset()
passed_calibration("S1")
status, rep1 = locked_publish(ReportSubmit(
    instrument="S1", tested_at=T(2026, 3, 1), reference="S1-R1"))
check("原始报告放行", status == 200 and rep1["released"] is True, str(status))
check("提交响应附带链标识（附加字段）",
      isinstance(rep1.get("chain_id"), int), str(rep1.get("chain_id")))
chain_id = rep1["chain_id"]

status, sup1 = locked_supersede(rep1["report_id"], ReportSupersede(
    instrument="S1", tested_at=T(2026, 3, 10),
    reference="S1-R2", reason="初测数据存疑，复测"))
check("复测接替成功（201）", status == 201 and sup1["released"] is True, str(sup1))
check("新报告沿用同一链", sup1["chain_id"] == chain_id, str(sup1))
check("接替响应含旧版标识与接替记录",
      sup1["superseded_report_id"] == rep1["report_id"]
      and sup1["supersession"]["old_report_id"] == rep1["report_id"]
      and sup1["supersession"]["new_report_id"] == sup1["report_id"]
      and sup1["supersession"]["reason"] == "初测数据存疑，复测", str(sup1))
check("新报告快照独立生成（检测时间为复测时间）",
      sup1["snapshot"]["tested_at"].startswith("2026-03-10")
      and sup1["snapshot"]["released"] is True, str(sup1["snapshot"]))

reports, sups = chain_state(chain_id)
check("链上两份报告（旧版永久保留）", len(reports) == 2, str(len(reports)))
old_row = next(r for r in reports if r["id"] == rep1["report_id"])
new_row = next(r for r in reports if r["id"] == sup1["report_id"])
check("旧版指针指向新版，新版为现行有效",
      old_row["superseded_by_id"] == sup1["report_id"]
      and new_row["superseded_by_id"] is None)
check("旧版快照不被改写（仍为发布时结论）",
      old_row["snapshot"]["tested_at"].startswith("2026-03-01")
      and old_row["snapshot"]["released"] is True
      and old_row["snapshot"]["reason_code"] == "PASSED_CALIBRATION_COVERAGE",
      str(old_row["snapshot"]))
check("接替记录恰一条且原因留痕",
      len(sups) == 1 and sups[0]["reason"] == "初测数据存疑，复测", str(sups))

# ---------- 2. 沿革查询：链内任一 ID 可查完整沿革 ----------
for probe_id, label in ((rep1["report_id"], "旧版"), (sup1["report_id"], "新版")):
    hist = run_tx(lambda conn: report_history(conn, probe_id), readonly=True)
    check(f"按{label} ID 查询沿革：链标识与现行报告正确",
          hist["chain_id"] == chain_id
          and hist["current_report_id"] == sup1["report_id"], str(hist["current_report_id"]))
    check(f"按{label} ID 查询沿革：两份报告 + 一条接替记录（含原因）",
          hist["report_count"] == 2
          and len(hist["reports"]) == 2
          and len(hist["supersessions"]) == 1
          and hist["supersessions"][0]["reason"] == "初测数据存疑，复测")

status, detail = http_call(report_history, 999999)
check("沿革查询不存在报告 -> None（路由映射 404）", status == 200 and detail is None)

# ---------- 3. 多次接替成链（A->B->C），线性、不分叉 ----------
status, sup2 = locked_supersede(sup1["report_id"], ReportSupersede(
    instrument="S1", tested_at=T(2026, 3, 20),
    reference="S1-R3", reason="二次复测"))
check("二次接替成功", status == 201 and sup2["chain_id"] == chain_id, str(sup2))
hist = run_tx(lambda conn: report_history(conn, rep1["report_id"]), readonly=True)
check("链 A->B->C 线性：3 份报告、2 条接替记录、现行报告为 C",
      hist["report_count"] == 3
      and len(hist["supersessions"]) == 2
      and hist["current_report_id"] == sup2["report_id"], str(hist["current_report_id"]))
reasons = [s["reason"] for s in hist["supersessions"]]
check("每次接替原因按序留痕", reasons == ["初测数据存疑，复测", "二次复测"], str(reasons))
ids = [r["id"] for r in hist["reports"]]
ptrs = {r["id"]: r["superseded_by_id"] for r in hist["reports"]}
check("链指针构成单链表（不成环、无分叉）",
      ptrs[ids[0]] == ids[1] and ptrs[ids[1]] == ids[2] and ptrs[ids[2]] is None)

# ---------- 4. 重复接替同一旧版 -> 409 + 现行报告标识 ----------
status, detail = locked_supersede(rep1["report_id"], ReportSupersede(
    instrument="S1", tested_at=T(2026, 3, 25),
    reference="S1-RX", reason="重复接替"))
check("重复接替同一旧版 -> 409 REPORT_ALREADY_SUPERSEDED",
      status == 409 and detail["code"] == "REPORT_ALREADY_SUPERSEDED", str(detail))
check("败方响应返回现行报告标识",
      detail.get("current_report_id") == sup2["report_id"]
      and detail.get("superseded_by_id") == sup1["report_id"], str(detail))
reports, sups = chain_state(chain_id)
check("重复接替未留痕（仍 3 报告 / 2 接替）", len(reports) == 3 and len(sups) == 2)

# ---------- 5. 新编号全局唯一：冲突回滚，旧报告仍有效 ----------
status, other = locked_publish(ReportSubmit(
    instrument="S1", tested_at=T(2026, 4, 1), reference="S1-OTHER"))
check("另发一份报告占用编号", status == 200, str(status))

status, detail = locked_supersede(sup2["report_id"], ReportSupersede(
    instrument="S1", tested_at=T(2026, 4, 2),
    reference="S1-OTHER", reason="编号冲突场景"))
check("新编号与既有报告冲突 -> 409 REPORT_REFERENCE_EXISTS",
      status == 409 and detail["code"] == "REPORT_REFERENCE_EXISTS", str(detail))

status, detail = locked_supersede(sup2["report_id"], ReportSupersede(
    instrument="S1", tested_at=T(2026, 4, 2),
    reference="S1-R3", reason="复用旧版自身编号"))
check("旧编号不得转移/复用（用旧版自身编号）-> 409",
      status == 409 and detail["code"] == "REPORT_REFERENCE_EXISTS", str(detail))

reports, sups = chain_state(chain_id)
cur_row = next(r for r in reports if r["id"] == sup2["report_id"])
check("编号冲突后旧报告仍有效（未被接替）、无新报告、无接替记录",
      cur_row["superseded_by_id"] is None and len(reports) == 3 and len(sups) == 2)

# ---------- 6. 放行规则沿用：阻断时旧报告仍有效、不留接替记录 ----------
reset()
passed_calibration("S6")
status, base = locked_publish(ReportSubmit(
    instrument="S6", tested_at=T(2026, 2, 1), reference="S6-R1"))
chain6 = base["chain_id"]

# 6a. 禁用优先：复测时点命中未撤销禁用区间
http_call(register_disable, DisableCreate(
    event_no="S6-DIS", instrument="S6",
    disabled_from=T(2026, 3, 1), disabled_to=T(2026, 4, 1), reason="泵故障"))
status, detail = locked_supersede(base["report_id"], ReportSupersede(
    instrument="S6", tested_at=T(2026, 3, 15),
    reference="S6-R2", reason="复测遇禁用"))
check("复测命中禁用 -> 422 INSTRUMENT_DISABLED（禁用优先，含禁用依据）",
      status == 422 and detail["code"] == "INSTRUMENT_DISABLED"
      and detail["disable"]["event_no"] == "S6-DIS", str(detail))
reports, sups = chain_state(chain6)
check("被禁用阻断：旧报告仍有效、无新报告、无接替记录",
      len(reports) == 1 and reports[0]["superseded_by_id"] is None and sups == [])

# 撤销禁用后同一复测可成功（验证阻断只是暂时状态）
http_call(revoke_disable, "S6-DIS", DisableRevoke(reason="修复"))
status, ok = locked_supersede(base["report_id"], ReportSupersede(
    instrument="S6", tested_at=T(2026, 3, 15),
    reference="S6-R2", reason="复测遇禁用"))
check("撤销禁用后同一复测接替成功", status == 201 and ok["reference"] == "S6-R2", str(ok))

# 6b. 无校准仪器 -> NO_CALIBRATION
status, nb = locked_publish(ReportSubmit(
    instrument="S6", tested_at=T(2026, 5, 1), reference="S6-R3"))
status, detail = locked_supersede(nb["report_id"], ReportSupersede(
    instrument="S6-NONE", tested_at=T(2026, 5, 2), reason="无校准复测"))
check("复测仪器无校准 -> 422 NO_CALIBRATION，旧报告仍有效",
      status == 422 and detail["code"] == "NO_CALIBRATION", str(detail))

# 6c. 校准未通过 -> CALIBRATION_NOT_PASSED
http_call(create_calibration, CalibrationCreate(
    instrument="S6-FAIL", valid_from=T(2025, 1, 1), valid_to=T(2030, 1, 1), passed=False))
status, detail = locked_supersede(nb["report_id"], ReportSupersede(
    instrument="S6-FAIL", tested_at=T(2026, 6, 1), reason="未通过校准复测"))
check("复测校准未通过 -> 422 CALIBRATION_NOT_PASSED",
      status == 422 and detail["code"] == "CALIBRATION_NOT_PASSED", str(detail))

# 6d. 区间外 -> OUTSIDE_VALID_PERIOD
http_call(create_calibration, CalibrationCreate(
    instrument="S6-GAP", valid_from=T(2026, 1, 1), valid_to=T(2026, 2, 1), passed=True))
status, detail = locked_supersede(nb["report_id"], ReportSupersede(
    instrument="S6-GAP", tested_at=T(2026, 2, 1), reason="区间外复测"))
check("复测时点在校准区间外（恰为 valid_to）-> 422 OUTSIDE_VALID_PERIOD",
      status == 422 and detail["code"] == "OUTSIDE_VALID_PERIOD", str(detail))

reports, sups = chain_state(nb["chain_id"])
check("全部阻断场景：旧报告仍有效、链上无新报告、无接替记录",
      len(reports) == 1 and reports[0]["superseded_by_id"] is None and sups == [])

# ---------- 7. 非空接替原因 + 不存在报告 ----------
try:
    ReportSupersede(instrument="S6", tested_at=T(2026, 1, 1), reason="")
    check("空原因 -> 校验拒绝", False)
except ValidationError:
    check("空原因 -> 校验拒绝", True)
try:
    ReportSupersede(instrument="S6", tested_at=T(2026, 1, 1), reason="   ")
    check("纯空白原因 -> 校验拒绝", False)
except ValidationError:
    check("纯空白原因 -> 校验拒绝", True)

status, detail = locked_supersede(999999, ReportSupersede(
    instrument="S6", tested_at=T(2026, 1, 1), reason="不存在的报告"))
check("接替不存在报告 -> 404 NOT_FOUND", status == 404 and detail["code"] == "NOT_FOUND", str(detail))

# ---------- 8. 并发接替同一旧版：至多成功一次 ----------
reset()
passed_calibration("S8")
# 为“不同仪器并发”路径准备多台仪器的通过校准
for inst in ("S8-A", "S8-B", "S8-C", "S8-D"):
    passed_calibration(inst)
status, victim = locked_publish(ReportSubmit(
    instrument="S8", tested_at=T(2026, 6, 1), reference="S8-R1"))
chain8 = victim["chain_id"]

N = 8
barrier = threading.Barrier(N)


def race_supersede(i):
    barrier.wait()
    # 一半线程用同仪器（咨询锁串行路径），一半用各自仪器（行锁串行路径）
    inst = "S8" if i % 2 == 0 else f"S8-{'ABCD'[i % 4]}"
    code, payload = locked_supersede(victim["report_id"], ReportSupersede(
        instrument=inst, tested_at=T(2026, 6, 2),
        reference=f"S8-race-{i}", reason=f"并发复测 {i}"))
    if code == 201:
        return ("ok", payload["report_id"], payload["chain_id"])
    return (payload["code"], payload.get("current_report_id"), None)


with ThreadPoolExecutor(N) as ex:
    results = list(ex.map(race_supersede, range(N)))

ok = [r for r in results if r[0] == "ok"]
losers = [r for r in results if r[0] == "REPORT_ALREADY_SUPERSEDED"]
others = [r for r in results if r[0] not in ("ok", "REPORT_ALREADY_SUPERSEDED")]
check("并发 8 个接替：恰好 1 个成功", len(ok) == 1, f"ok={len(ok)} losers={len(losers)} other={others}")
check("并发败方全部 REPORT_ALREADY_SUPERSEDED", len(losers) == 7 and not others, str(others))
winner_new_id = ok[0][1]
check("并发败方均拿到现行报告标识（胜出新报告）",
      all(l[1] == winner_new_id for l in losers), str(results))
check("胜出新报告仍在原链上", ok[0][2] == chain8)
reports, sups = chain_state(chain8)
check("并发后链上恰 2 份报告、1 条接替记录（不分叉）",
      len(reports) == 2 and len(sups) == 1, f"reports={len(reports)} sups={len(sups)}")
currents = [r for r in reports if r["superseded_by_id"] is None]
check("链上恰一份现行有效报告（即胜出新版）",
      len(currents) == 1 and currents[0]["id"] == winner_new_id)

# 并发后再接替旧版：稳定返回现行报告标识
status, detail = locked_supersede(victim["report_id"], ReportSupersede(
    instrument="S8", tested_at=T(2026, 6, 3), reason="赛后再次接替"))
check("并发后再接替旧版 -> 409 且现行报告标识一致",
      status == 409 and detail.get("current_report_id") == winner_new_id, str(detail))

# ---------- 9. 兼容性：单报告提交/查询响应字段 ----------
reset()
passed_calibration("S9")
status, rep = locked_publish(ReportSubmit(
    instrument="S9", tested_at=T(2026, 3, 1), reference="S9-R1"))
expected_keys = {"released", "reason_code", "basis_calibration",
                 "report_id", "reference", "snapshot"}
check("提交响应保留原有字段（新增 chain_id 为附加）",
      expected_keys <= set(rep.keys()), str(sorted(rep.keys())))
check("快照格式不变",
      set(rep["snapshot"].keys()) == {"released", "reason_code", "instrument",
                                      "tested_at", "basis_calibration"},
      str(sorted(rep["snapshot"].keys())))

# 阻断响应格式不变
status, detail = locked_publish(ReportSubmit(
    instrument="S9-NONE", tested_at=T(2026, 3, 1)))
check("阻断响应仍为 422 + 原因码", status == 422 and detail["code"] == "NO_CALIBRATION")

# 接替后旧版仍可按原接口查得且快照不变（永久保留）
status, sup = locked_supersede(rep["report_id"], ReportSupersede(
    instrument="S9", tested_at=T(2026, 3, 5), reference="S9-R2", reason="复测"))
with db.pool.connection() as c:
    old = c.execute("SELECT * FROM reports WHERE id = %s",
                    (rep["report_id"],)).fetchone()
check("旧版报告仍可按 ID 直查，快照与发布时一致",
      old is not None and old["snapshot"]["tested_at"].startswith("2026-03-01")
      and old["superseded_by_id"] == sup["report_id"])

# ---------- 10. 链结构约束在数据库层兜底 ----------
from psycopg.errors import UniqueViolation

with db.pool.connection() as c:
    idx = {r["indexname"] for r in c.execute(
        "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'").fetchall()}
    check("唯一索引齐备（防分叉/防合并成环/编号唯一/指针唯一）",
          {"uq_supersession_old", "uq_supersession_new",
           "uq_report_reference", "uq_report_superseded_by"} <= idx, str(sorted(idx)))
    # 直接 SQL 试图给同一旧版再插一条接替记录 -> 唯一索引拒绝
    try:
        c.execute(
            "INSERT INTO report_supersessions (chain_id, old_report_id, new_report_id, reason) "
            "VALUES (%s, %s, %s, %s)",
            (sup["chain_id"], rep["report_id"], sup["report_id"], "手工分叉"))
        check("数据库层拒绝同一旧版第二条接替记录", False)
    except UniqueViolation:
        check("数据库层拒绝同一旧版第二条接替记录（uq_supersession_old）", True)

print()
if failures:
    print(f"{len(failures)} 项失败:", failures)
    sys.exit(1)
print("全部复测接替验证通过")
