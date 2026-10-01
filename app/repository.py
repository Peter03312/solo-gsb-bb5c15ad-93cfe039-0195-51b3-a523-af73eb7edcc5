"""领域逻辑：校准判定、版本更正、发布、快照漂移重算。"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from psycopg.errors import ExclusionViolation, UniqueViolation

from .errors import blocked, conflict, not_found

# ---- 判定原因码 ----------------------------------------------------------------
# 放行成功
REASON_PASSED = "PASSED_CALIBRATION_COVERAGE"
# 检测时间命中该仪器“未撤销”的禁用区间（即使校准通过也阻断）
REASON_DISABLED = "INSTRUMENT_DISABLED"
# 该仪器当前没有任何校准版本
REASON_NO_CALIBRATION = "NO_CALIBRATION"
# 存在当前校准版本，但检测时间不落在任何 [valid_from, valid_to) 内
REASON_OUTSIDE_PERIOD = "OUTSIDE_VALID_PERIOD"
# 覆盖检测时间的当前校准版本结论为未通过
REASON_NOT_PASSED = "CALIBRATION_NOT_PASSED"

# 会话级咨询锁命名空间（int4）：同仪器的“禁用变更 / 发布”共用一把锁。
# 键为 (ADVISORY_LOCK_NS, hashtext(instrument))，hashtext 由数据库计算
# （见 app.db.instrument_lock_key，在事务外取键）。
ADVISORY_LOCK_NS = 0xD15A  # “DISA”

_COVERAGE_SQL = """
    SELECT *
    FROM calibration_versions
    WHERE instrument = %s
      AND is_current = TRUE
      AND valid_from <= %s
      AND (valid_to IS NULL OR %s < valid_to)
"""

_NEAREST_SQL = """
    SELECT *
    FROM calibration_versions
    WHERE instrument = %s AND is_current = TRUE
    ORDER BY (valid_from <= %s) DESC,
             CASE WHEN valid_from <= %s THEN valid_to END DESC NULLS LAST,
             valid_from ASC
"""


def _cal_brief(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "chain_id": row["chain_id"],
        "version": row["version"],
        "valid_from": row["valid_from"].isoformat(),
        "valid_to": row["valid_to"].isoformat() if row["valid_to"] else None,
        "passed": row["passed"],
    }


# ---- 禁用区间判定 --------------------------------------------------------------

_DISABLE_COVERAGE_SQL = """
    SELECT *
    FROM instrument_disable_events
    WHERE instrument = %s
      AND revoked = FALSE
      AND disabled_from <= %s
      AND (disabled_to IS NULL OR %s < disabled_to)
"""


def disable_brief(row: dict[str, Any]) -> dict[str, Any]:
    """禁用依据（出现在阻断响应与受影响报告查询中）。"""
    return {
        "event_no": row["event_no"],
        "instrument": row["instrument"],
        "disabled_from": row["disabled_from"].isoformat(),
        "disabled_to": row["disabled_to"].isoformat() if row["disabled_to"] else None,
        "reason": row["reason"],
    }


def find_active_disable(conn, instrument: str, at: datetime, *, lock: bool):
    """返回命中该时点的未撤销禁用区间（至多一条，排他约束保证），无则 None。

    ``lock=True`` 时命中行加 FOR UPDATE；未命中时锁定该仪器全部未撤销行，
    与“登记禁用 / 撤销禁用”事务互斥，保证判定可串行化。
    """
    sql = _DISABLE_COVERAGE_SQL + (" FOR UPDATE" if lock else "")
    with conn.cursor() as cur:
        cur.execute(sql, (instrument, at, at))
        rows = cur.fetchall()
        if rows:
            return rows[0]
        if lock:
            cur.execute(
                "SELECT id FROM instrument_disable_events "
                "WHERE instrument = %s AND revoked = FALSE FOR UPDATE",
                (instrument,),
            )
            cur.fetchall()
    return None


def determine(conn, instrument: str, tested_at: datetime, *, lock: bool) -> dict[str, Any]:
    """返回该时点的校准判定。

    ``lock=True`` 时对覆盖行加 FOR UPDATE 行锁，并在无覆盖时锁定该仪器全部
    当前版本 —— 与校准更正事务互斥，保证“发布”与“更正”可串行化。
    """
    sql = _COVERAGE_SQL + (" FOR UPDATE" if lock else "")
    with conn.cursor() as cur:
        cur.execute(sql, (instrument, tested_at, tested_at))
        covering = cur.fetchall()

    if covering:
        row = covering[0]  # 排除约束保证同仪器当前版本区间不重叠，至多一条
        if row["passed"]:
            return {"releasable": True, "reason_code": REASON_PASSED,
                    "calibration": row, "reason_message": None}
        return {
            "releasable": False, "reason_code": REASON_NOT_PASSED,
            "calibration": row,
            "reason_message": f"检测时间 {tested_at.isoformat()} 被校准版本 "
                              f"v{row['version']} 覆盖，但其结论为未通过",
        }

    # 无覆盖：给出可操作的阻断原因
    with conn.cursor() as cur:
        if lock:
            cur.execute(
                "SELECT id FROM calibration_versions "
                "WHERE instrument = %s AND is_current = TRUE FOR UPDATE",
                (instrument,),
            )
            cur.fetchall()
        cur.execute(
            "SELECT count(*) AS n FROM calibration_versions "
            "WHERE instrument = %s AND is_current = TRUE",
            (instrument,),
        )
        count = cur.fetchone()["n"]
        nearest = None
        if count:
            cur.execute(_NEAREST_SQL, (instrument, tested_at, tested_at))
            nearest = cur.fetchone()

    if count == 0:
        return {"releasable": False, "reason_code": REASON_NO_CALIBRATION,
                "calibration": None,
                "reason_message": f"仪器 {instrument} 当前不存在任何校准记录"}

    if nearest["valid_from"] > tested_at:
        msg = (f"检测时间 {tested_at.isoformat()} 早于最近校准生效时间 "
               f"{nearest['valid_from'].isoformat()}（左闭右开区间）")
    else:
        end = nearest["valid_to"]
        msg = (f"检测时间 {tested_at.isoformat()} 晚于最近校准失效时间 "
               f"{end.isoformat() if end else '开放区间'}（左闭右开区间）")
    return {"releasable": False, "reason_code": REASON_OUTSIDE_PERIOD,
            "calibration": None, "nearest_calibration": nearest,
            "reason_message": msg}


# ---- 校准写入 ------------------------------------------------------------------

def create_calibration(conn, payload) -> dict[str, Any]:
    """创建原始校准记录（新版本链的 v1）。"""
    if payload.valid_to is not None and payload.valid_to <= payload.valid_from:
        raise blocked("INVALID_INTERVAL",
                      "有效区间必须满足 valid_from < valid_to（左闭右开）")
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO calibration_chains (instrument) VALUES (%s) RETURNING id",
            (payload.instrument,),
        )
        chain_id = cur.fetchone()["id"]
        try:
            cur.execute(
                """
                INSERT INTO calibration_versions
                    (chain_id, version, instrument, valid_from, valid_to,
                     passed, corrects_id, is_current)
                VALUES (%s, 1, %s, %s, %s, %s, NULL, TRUE)
                RETURNING *
                """,
                (chain_id, payload.instrument, payload.valid_from,
                 payload.valid_to, payload.passed),
            )
        except ExclusionViolation:
            raise conflict("INTERVAL_OVERLAP",
                           "与该仪器另一条当前校准版本的有效区间重叠",
                           instrument=payload.instrument)
        return cur.fetchone()


def correct_calibration(conn, payload) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """提交更正版本。返回 (新版本, 受影响的已发布报告漂移列表)。"""
    if payload.valid_to is not None and payload.valid_to <= payload.valid_from:
        raise blocked("INVALID_INTERVAL",
                      "有效区间必须满足 valid_from < valid_to（左闭右开）")

    with conn.cursor() as cur:
        # 锁定被引用记录
        cur.execute("SELECT * FROM calibration_versions WHERE id = %s FOR UPDATE",
                    (payload.corrects_id,))
        original = cur.fetchone()
        if original is None:
            raise not_found("校准记录", payload.corrects_id)

        # 锁版本链：同一链上的并发更正在此串行
        cur.execute("SELECT id FROM calibration_chains WHERE id = %s FOR UPDATE",
                    (original["chain_id"],))

        cur.execute(
            "SELECT * FROM calibration_versions WHERE chain_id = %s AND is_current = TRUE FOR UPDATE",
            (original["chain_id"],),
        )
        current = cur.fetchone()
        # 唯一部分索引保证至多一条；防御性处理
        assert current is not None, "版本链缺少最新版本，数据异常"

        expected_version = current["version"] + 1
        if payload.version != expected_version:
            raise conflict(
                "VERSION_CONFLICT",
                f"版本号冲突：当前最新版本为 v{current['version']}，"
                f"更正必须提交连续版本号 v{expected_version}，收到 v{payload.version}",
                current_version=current["version"],
                expected_version=expected_version,
            )

        instrument = current["instrument"]

        # 先下线旧版，再插入新版（避免与自身触发区间约束）
        cur.execute(
            "UPDATE calibration_versions SET is_current = FALSE "
            "WHERE id = %s RETURNING id",
            (current["id"],),
        )
        try:
            cur.execute(
                """
                INSERT INTO calibration_versions
                    (chain_id, version, instrument, valid_from, valid_to,
                     passed, corrects_id, is_current)
                VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)
                RETURNING *
                """,
                (current["chain_id"], payload.version, instrument,
                 payload.valid_from, payload.valid_to, payload.passed,
                 payload.corrects_id),
            )
        except UniqueViolation:
            raise conflict("VERSION_CONFLICT",
                           "版本冲突：该版本号已存在或链上已存在最新版")
        except ExclusionViolation:
            raise conflict("INTERVAL_OVERLAP",
                           "新版本与该仪器另一条当前校准版本的有效区间重叠",
                           instrument=instrument)
        new_row = cur.fetchone()

    drift = _drift_for_instrument(conn, instrument)
    return new_row, drift


# ---- 报告发布 ------------------------------------------------------------------

def _decide_and_insert(conn, *, chain_id: int, instrument: str,
                       tested_at: datetime, reference: str | None):
    """按当前状态判定放行并落库一份报告，返回 (报告行, 所据校准, 快照)。

    判定规则（单报告提交与复测接替共用）：禁用区间优先——命中未撤销禁用
    即 422；再按“当前通过校准覆盖”判定。阻断或编号冲突抛 4xx，整个事务
    回滚，不落库、不留任何痕迹。
    """
    # 禁用优先：命中未撤销禁用区间即拒绝放行，即使当前校准通过。
    active_disable = find_active_disable(conn, instrument, tested_at, lock=True)
    if active_disable is not None:
        raise blocked(
            REASON_DISABLED,
            f"检测时间 {tested_at.isoformat()} 命中仪器 "
            f"{instrument} 的未撤销禁用区间（事件 {active_disable['event_no']}）："
            f"{active_disable['reason']}",
            instrument=instrument,
            tested_at=tested_at.isoformat(),
            disable=disable_brief(active_disable),
        )

    decision = determine(conn, instrument, tested_at, lock=True)
    basis = decision["calibration"]
    snapshot: dict[str, Any] = {
        "released": decision["releasable"],
        "reason_code": decision["reason_code"],
        "instrument": instrument,
        "tested_at": tested_at.isoformat(),
    }
    if basis is not None:
        snapshot["basis_calibration"] = _cal_brief(basis)
    if not decision["releasable"]:
        snapshot["reason_message"] = decision["reason_message"]
        raise blocked(
            decision["reason_code"],
            decision["reason_message"],
            instrument=instrument,
            tested_at=tested_at.isoformat(),
            nearest_calibration=(
                _cal_brief(decision["nearest_calibration"])
                if decision.get("nearest_calibration") else None
            ),
        )

    with conn.cursor() as cur:
        try:
            cur.execute(
                """
                INSERT INTO reports
                    (chain_id, reference, instrument, tested_at, released,
                     basis_calibration_id, snapshot)
                VALUES (%s, %s, %s, %s, TRUE, %s, %s)
                RETURNING *
                """,
                (chain_id, reference, instrument, tested_at,
                 basis["id"], json.dumps(snapshot)),
            )
        except UniqueViolation:
            raise conflict("REPORT_REFERENCE_EXISTS",
                           f"报告编号已存在: {reference}")
        report = cur.fetchone()
    return report, basis, snapshot


def publish_report(conn, payload) -> dict[str, Any]:
    # 每份原始报告自开一条链；若放行被阻断，链行同事务回滚，不留空链
    with conn.cursor() as cur:
        cur.execute("INSERT INTO report_chains DEFAULT VALUES RETURNING id")
        chain_id = cur.fetchone()["id"]
    report, basis, snapshot = _decide_and_insert(
        conn, chain_id=chain_id, instrument=payload.instrument,
        tested_at=payload.tested_at, reference=payload.reference)
    return {
        "released": True,
        "reason_code": REASON_PASSED,
        "basis_calibration": _cal_brief(basis),
        "report_id": report["id"],
        "reference": report["reference"],
        "chain_id": report["chain_id"],
        "snapshot": snapshot,
    }


# ---- 报告接替（复测） -----------------------------------------------------------

def _chain_current(conn, chain_id: int) -> dict[str, Any] | None:
    """链上现行有效报告（superseded_by_id 为 NULL 的那一份）。"""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM reports WHERE chain_id = %s AND superseded_by_id IS NULL",
            (chain_id,),
        )
        return cur.fetchone()


def supersede_report(conn, report_id: int, payload) -> dict[str, Any]:
    """以复测报告接替一份当前有效报告；新报告与接替关系同一事务原子落库。

    - 旧报告不存在 -> 404
    - 旧报告已被接替 -> 409 REPORT_ALREADY_SUPERSEDED（附现行报告标识）
    - 复测放行被阻断 / 新编号冲突 -> 抛错回滚：旧报告仍有效，不留接替记录
    """
    with conn.cursor() as cur:
        # 锁旧报告行：同一旧版的并发接替在此串行，后到者重试后看到已被接替
        cur.execute("SELECT * FROM reports WHERE id = %s FOR UPDATE", (report_id,))
        old = cur.fetchone()
    if old is None:
        raise not_found("报告", report_id)
    if not old["released"]:
        # 防御：被阻断的报告本不落库，正常不会走到
        raise conflict("REPORT_NOT_RELEASED",
                       f"报告 {report_id} 不是已发布报告，不能被接替",
                       report_id=report_id)
    if old["superseded_by_id"] is not None:
        current = _chain_current(conn, old["chain_id"])
        current_id = current["id"] if current else old["superseded_by_id"]
        raise conflict(
            "REPORT_ALREADY_SUPERSEDED",
            f"报告 {report_id} 已被报告 {old['superseded_by_id']} 接替，"
            f"该链现行有效报告为 {current_id}；同一旧版至多接替一次",
            report_id=report_id,
            superseded_by_id=old["superseded_by_id"],
            current_report_id=current_id,
            chain_id=old["chain_id"],
        )

    # 复测放行判定与单报告提交同一套规则（禁用优先 + 当前通过校准覆盖）；
    # 新编号沿用全局唯一约束，旧编号不得转移或复用（唯一索引拦截）。
    new_report, basis, snapshot = _decide_and_insert(
        conn, chain_id=old["chain_id"], instrument=payload.instrument,
        tested_at=payload.tested_at, reference=payload.reference)

    with conn.cursor() as cur:
        # 旧版下线（指针指向新版）+ 接替关系留痕，与新报告同一事务提交
        cur.execute(
            "UPDATE reports SET superseded_by_id = %s WHERE id = %s",
            (new_report["id"], old["id"]),
        )
        try:
            cur.execute(
                """
                INSERT INTO report_supersessions
                    (chain_id, old_report_id, new_report_id, reason)
                VALUES (%s, %s, %s, %s)
                RETURNING *
                """,
                (old["chain_id"], old["id"], new_report["id"], payload.reason),
            )
        except UniqueViolation:
            # 行锁已保证不会走到；唯一索引兜底（并发接替同一旧版）
            raise conflict(
                "REPORT_ALREADY_SUPERSEDED",
                f"报告 {report_id} 已被其他复测接替，同一旧版至多接替一次",
                report_id=report_id,
                chain_id=old["chain_id"],
            )
        supersession = cur.fetchone()

    return {
        "released": True,
        "reason_code": REASON_PASSED,
        "basis_calibration": _cal_brief(basis),
        "report_id": new_report["id"],
        "reference": new_report["reference"],
        "chain_id": old["chain_id"],
        "snapshot": snapshot,
        "superseded_report_id": old["id"],
        "supersession": {
            "id": supersession["id"],
            "chain_id": supersession["chain_id"],
            "old_report_id": supersession["old_report_id"],
            "new_report_id": supersession["new_report_id"],
            "reason": supersession["reason"],
            "created_at": supersession["created_at"].isoformat(),
        },
    }


def report_history(conn, report_id: int) -> dict[str, Any] | None:
    """按链内任一报告 ID 返回完整沿革；报告不存在返回 None。

    链上全部版本（含各自不可变快照）+ 每次接替原因 + 当前有效报告。
    """
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM reports WHERE id = %s", (report_id,))
        probe = cur.fetchone()
        if probe is None:
            return None
        chain_id = probe["chain_id"]
        cur.execute(
            "SELECT * FROM reports WHERE chain_id = %s ORDER BY id", (chain_id,))
        reports = cur.fetchall()
        cur.execute(
            "SELECT * FROM report_supersessions WHERE chain_id = %s ORDER BY id",
            (chain_id,))
        supersessions = cur.fetchall()
    current = next((r for r in reports if r["superseded_by_id"] is None), None)
    return {
        "chain_id": chain_id,
        "current_report_id": current["id"] if current else None,
        "report_count": len(reports),
        "reports": reports,
        "supersessions": supersessions,
    }


# ---- 漂移重算 ------------------------------------------------------------------

def _drift_for_instrument(conn, instrument: str) -> list[dict[str, Any]]:
    return _drift(conn, instrument=instrument)


def compute_drift(conn, instrument: str | None = None) -> list[dict[str, Any]]:
    return _drift(conn, instrument=instrument)


def _drift(conn, instrument: str | None) -> list[dict[str, Any]]:
    """对已发布报告按当前校准重算；仅返回结论与快照不同的报告。

    快照本身绝不改写。判定不加锁（只读重算）。
    """
    with conn.cursor() as cur:
        if instrument:
            cur.execute(
                "SELECT * FROM reports WHERE released = TRUE AND instrument = %s "
                "ORDER BY id",
                (instrument,),
            )
        else:
            cur.execute("SELECT * FROM reports WHERE released = TRUE ORDER BY id")
        reports = cur.fetchall()

    drifted: list[dict[str, Any]] = []
    for report in reports:
        now_decision = determine(conn, report["instrument"], report["tested_at"],
                                 lock=False)
        snap = report["snapshot"]
        snap_basis = (snap or {}).get("basis_calibration") or {}

        if now_decision["releasable"]:
            now_basis = now_decision["calibration"]
            if now_basis["id"] == snap_basis.get("id"):
                continue  # 结论与依据版本均未变化
            change = "NOW_COVERED_BY_OTHER_VERSION" if snap.get("released") else "NOW_RELEASEABLE"
            drifted.append({
                "report_id": report["id"],
                "reference": report["reference"],
                "instrument": report["instrument"],
                "tested_at": report["tested_at"].isoformat(),
                "change": change,
                "snapshot": snap,
                "current_decision": {
                    "released": True,
                    "reason_code": REASON_PASSED,
                    "basis_calibration": _cal_brief(now_basis),
                },
            })
        else:
            drifted.append({
                "report_id": report["id"],
                "reference": report["reference"],
                "instrument": report["instrument"],
                "tested_at": report["tested_at"].isoformat(),
                "change": "NOW_BLOCKED",
                "snapshot": snap,
                "current_decision": {
                    "released": False,
                    "reason_code": now_decision["reason_code"],
                    "reason_message": now_decision["reason_message"],
                    "basis_calibration": (
                        _cal_brief(now_decision["calibration"])
                        if now_decision["calibration"] else None
                    ),
                },
            })
    return drifted


# ---- 禁用区间：登记 / 撤销 / 查询 ----------------------------------------------

def register_disable(conn, payload) -> dict[str, Any]:
    """登记禁用区间。无效区间 -> 422；重复事件号/区间重叠 -> 409。"""
    if payload.disabled_to is not None and payload.disabled_to <= payload.disabled_from:
        raise blocked("INVALID_INTERVAL",
                      "禁用区间必须满足 disabled_from < disabled_to（左闭右开）")
    with conn.cursor() as cur:
        try:
            cur.execute(
                """
                INSERT INTO instrument_disable_events
                    (event_no, instrument, disabled_from, disabled_to, reason)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING *
                """,
                (payload.event_no, payload.instrument, payload.disabled_from,
                 payload.disabled_to, payload.reason),
            )
        except UniqueViolation:
            raise conflict("DISABLE_EVENT_EXISTS",
                           f"禁用事件号已存在（不可重复登记，含已撤销事件）: {payload.event_no}",
                           event_no=payload.event_no)
        except ExclusionViolation:
            raise conflict("DISABLE_INTERVAL_OVERLAP",
                           "与该仪器另一条未撤销禁用区间重叠（边界相接允许）",
                           instrument=payload.instrument,
                           event_no=payload.event_no)
        return cur.fetchone()


def revoke_disable(conn, event_no: str, payload) -> dict[str, Any]:
    """按事件号撤销禁用；未知事件 -> 404，已撤销 -> 409。记录保留。"""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM instrument_disable_events WHERE event_no = %s FOR UPDATE",
            (event_no,),
        )
        row = cur.fetchone()
        if row is None:
            raise not_found("禁用事件", event_no)
        if row["revoked"]:
            raise conflict("DISABLE_ALREADY_REVOKED",
                           f"禁用事件已撤销，不能重复撤销: {event_no}",
                           event_no=event_no,
                           revoked_at=row["revoked_at"].isoformat())
        cur.execute(
            """
            UPDATE instrument_disable_events
            SET revoked = TRUE, revoked_at = now(), revoke_reason = %s
            WHERE id = %s
            RETURNING *
            """,
            (payload.reason, row["id"]),
        )
        return cur.fetchone()


def get_disable(conn, event_no: str) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM instrument_disable_events WHERE event_no = %s",
            (event_no,),
        )
        return cur.fetchone()


def list_disables(conn, instrument: str | None, include_revoked: bool) -> list[dict[str, Any]]:
    sql = "SELECT * FROM instrument_disable_events"
    where, params = [], []
    if instrument:
        where.append("instrument = %s")
        params.append(instrument)
    if not include_revoked:
        where.append("revoked = FALSE")
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY instrument, disabled_from, id"
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def affected_reports(conn, instrument: str | None) -> list[dict[str, Any]]:
    """按当前禁用状态，列出检测时间命中未撤销禁用区间的已发布报告及禁用依据。

    追溯登记 / 撤销禁用后结果随之变化；已发布报告与其快照不改写。
    只读重算，不加锁（运行在 SERIALIZABLE 只读事务中）。
    """
    sql = """
        SELECT r.id AS report_id, r.reference, r.instrument, r.tested_at,
               r.basis_calibration_id, r.snapshot, r.created_at AS report_created_at,
               d.event_no, d.reason, d.disabled_from, d.disabled_to
        FROM reports r
        JOIN instrument_disable_events d
          ON d.instrument = r.instrument
         AND d.revoked = FALSE
         AND d.disabled_from <= r.tested_at
         AND (d.disabled_to IS NULL OR r.tested_at < d.disabled_to)
    """
    params: list[Any] = []
    if instrument:
        sql += " WHERE r.instrument = %s"
        params.append(instrument)
    sql += " ORDER BY r.instrument, r.tested_at, r.id"
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    items: list[dict[str, Any]] = []
    for row in rows:
        items.append({
            "report_id": row["report_id"],
            "reference": row["reference"],
            "instrument": row["instrument"],
            "tested_at": row["tested_at"].isoformat(),
            "basis_calibration_id": row["basis_calibration_id"],
            "snapshot": row["snapshot"],
            "disable": {
                "event_no": row["event_no"],
                "instrument": row["instrument"],
                "disabled_from": row["disabled_from"].isoformat(),
                "disabled_to": row["disabled_to"].isoformat() if row["disabled_to"] else None,
                "reason": row["reason"],
            },
        })
    return items
