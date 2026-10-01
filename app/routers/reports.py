"""检测报告接口。"""
from __future__ import annotations

from fastapi import APIRouter

from .. import db
from ..errors import not_found
from ..models import ReportSubmit, ReportSupersede, serialize_supersession
from ..repository import compute_drift
from ..transactions import run_tx
from ..repository import publish_report, report_history, supersede_report

router = APIRouter(prefix="/reports", tags=["reports"])


def _serialize_report(row: dict) -> dict:
    return {
        "id": row["id"],
        "chain_id": row["chain_id"],
        "reference": row["reference"],
        "instrument": row["instrument"],
        "tested_at": row["tested_at"].isoformat(),
        "released": row["released"],
        "basis_calibration_id": row["basis_calibration_id"],
        "superseded_by_id": row["superseded_by_id"],
        "is_current": row["superseded_by_id"] is None,
        "snapshot": row["snapshot"],
        "created_at": row["created_at"].isoformat(),
    }


@router.post("", status_code=201)
def submit_report(payload: ReportSubmit):
    """提交检测报告并尝试放行。

    判定顺序：先检查仪器禁用区间（命中未撤销区间即 422 INSTRUMENT_DISABLED
    并返回禁用依据，即使校准通过）；再检查校准——检测时间被该仪器一条“结论
    为通过”的当前校准版本覆盖才放行（左闭右开，仅最新版参与判定）。

    - 放行：201，返回所据校准版本及不可变判定快照
    - 命中禁用 / 缺失校准 / 区间外 / 校准未通过：422，detail 内含阻断原因码与说明
    """
    return run_tx(
        lambda conn: publish_report(conn, payload),
        advisory_lock=db.instrument_lock_key(payload.instrument),
    )


# 注意：静态路由需在 /{report_id} 之前声明
@router.get("/drift/recompute")
def report_drift(instrument: str | None = None):
    items = run_tx(lambda conn: compute_drift(conn, instrument), readonly=True)
    return {"items": items, "count": len(items)}


@router.get("")
def list_reports(instrument: str | None = None):
    assert db.pool is not None
    with db.pool.connection() as conn:
        if instrument:
            rows = conn.execute(
                "SELECT * FROM reports WHERE instrument = %s ORDER BY id",
                (instrument,),
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM reports ORDER BY id").fetchall()
    return {"items": [_serialize_report(r) for r in rows]}


@router.get("/{report_id}")
def get_report(report_id: int):
    assert db.pool is not None
    with db.pool.connection() as conn:
        row = conn.execute(
            "SELECT * FROM reports WHERE id = %s", (report_id,)
        ).fetchone()
    if row is None:
        raise not_found("报告", report_id)
    return {"report": _serialize_report(row)}


@router.post("/{report_id}/supersede", status_code=201)
def supersede(report_id: int, payload: ReportSupersede):
    """以复测报告接替一份当前有效报告（路径 ID 即被接替的当前有效报告）。

    放行规则与单报告提交完全一致（禁用优先 + 当前通过校准覆盖）；
    成功时新报告与接替关系同一事务原子发布，旧版报告及其快照永久保留，
    链上现行有效报告切换为新版。

    - 旧报告不存在：404 NOT_FOUND
    - 旧报告已被接替（含并发败方）：409 REPORT_ALREADY_SUPERSEDED，
      detail 附 `current_report_id`（该链现行有效报告标识）
    - 复测被阻断（禁用/无校准/区间外/未通过）：422，与单报告提交同约定
    - 新编号与既有报告（含旧版自身）重复：409 REPORT_REFERENCE_EXISTS
    - 阻断或编号冲突时整体回滚：旧报告仍有效，不留下接替记录
    """
    return run_tx(
        lambda conn: supersede_report(conn, report_id, payload),
        advisory_lock=db.instrument_lock_key(payload.instrument),
    )


@router.get("/{report_id}/history")
def get_report_history(report_id: int):
    """按链内任一报告 ID 查询完整接替沿革。

    返回该链全部报告版本（含各自不可变快照）、每次接替的原因与时间、
    以及链上当前有效报告标识；链为线性（不分叉、不成环）。
    """
    result = run_tx(lambda conn: report_history(conn, report_id), readonly=True)
    if result is None:
        raise not_found("报告", report_id)
    return {
        "chain_id": result["chain_id"],
        "current_report_id": result["current_report_id"],
        "report_count": result["report_count"],
        "reports": [_serialize_report(r) for r in result["reports"]],
        "supersessions": [serialize_supersession(s) for s in result["supersessions"]],
    }
