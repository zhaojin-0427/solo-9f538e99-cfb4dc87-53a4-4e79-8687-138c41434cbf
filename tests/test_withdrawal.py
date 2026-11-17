"""患者撤回关键流程：登记失效、停供、恢复、防再纳入、幂等、交错裁决。"""
import time

from app.db import get_db
from app.service import Service
from tests.conftest import make_csv, make_dup, upload


def rows_basic():
    return [
        ("I1", "P1", "A"), ("I2", "P2", "A"), ("I3", "P3", "A"), ("I4", "P4", "A"),
        ("I5", "P5", "B"), ("I6", "P6", "B"), ("I7", "P7", "B"),
        ("I8", "P8", "C"), ("I9", "P9", "C"), ("I10", "P10", "D"),
    ]


def quota_full():
    return {"A": 2, "B": 1, "C": 1, "D": 1}


def _wait_jobs(svc, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        jobs = svc.list_jobs()
        if jobs and all(j["status"] in ("succeeded", "failed") for j in jobs):
            return
        time.sleep(0.1)
    raise AssertionError("jobs did not finish in time")


def test_withdrawal_invalidates_current_and_stops_download(client):
    c, svc = client
    upload(c, rows_basic(), quota_full())
    c.post("/partition", follow_redirects=True)
    m1 = svc.current_manifest()
    assert m1 is not None
    assert c.get(f"/manifests/{m1['id']}/download/val").status_code == 200

    # 登记撤回 P3（含于当前版本）→ 成功回执
    resp = c.post("/withdrawals", data={"patient_no": "P3", "reason": "患者撤回同意"},
                  follow_redirects=True)
    assert resp.status_code == 200
    page = resp.data.decode("utf-8")
    assert "已立即失效" in page and "不可解除" in page

    # 当前版本立即失效：无当前版本、无当前清单
    assert svc.current_version() is None
    assert svc.current_manifest() is None
    v1 = svc.get_version(1)
    assert v1["status"] == "invalidated"

    # 回执后新发起的下载不得含撤回患者 → 停供
    resp = c.get(f"/manifests/{m1['id']}/download/val", follow_redirects=True)
    assert "停供" in resp.data.decode("utf-8")
    resp = c.get(f"/manifests/{m1['id']}/download/train", follow_redirects=True)
    assert "停供" in resp.data.decode("utf-8")

    # 历史明细留作追溯：版本详情与作业明细仍可查看，但不可下载
    vpage = c.get("/versions/1").data.decode("utf-8")
    assert "P3" in vpage and "已失效" in vpage
    job = svc.list_jobs()[0]
    jpage = c.get(f"/jobs/{job['id']}").data.decode("utf-8")
    assert "P3" in jpage  # 明细可见（追溯）

    # 受影响清单在撤回页可见
    wpage = c.get("/withdrawals").data.decode("utf-8")
    assert "P3" in wpage and f"#{m1['id']}" in wpage and "已停供" in wpage


def test_reupload_without_patient_and_repartition_restores_supply(client):
    c, svc = client
    upload(c, rows_basic(), quota_full())
    c.post("/partition", follow_redirects=True)
    c.post("/withdrawals", data={"patient_no": "P3"}, follow_redirects=True)
    assert svc.current_manifest() is None  # 供数已停

    # 重新上传不含 P3 的全量 CSV；配额按已锁定侧别精确推算（沿用侧别永久固定规则）
    rows2 = [r for r in rows_basic() if r[1] != "P3"]
    reg = {r["image_no"]: r["side"] for r in svc.registry_snapshot()}
    quota = {}
    for img, _pat, cls in rows2:
        if reg[img] == "val":
            quota[cls] = quota.get(cls, 0) + 1
    resp = upload(c, rows2, quota)
    assert "输入校验失败" not in resp.data.decode("utf-8")
    v2 = svc.current_version()
    assert v2 is not None and v2["status"] == "active"

    # 完成划分后恢复供数，且清单不含撤回患者
    c.post("/partition", follow_redirects=True)
    m2 = svc.current_manifest()
    assert m2 is not None
    entries = svc.get_manifest_entries(m2["id"])
    assert len(entries) == 9
    assert all(e["patient_no"] != "P3" for e in entries)
    assert c.get(f"/manifests/{m2['id']}/download/val").status_code == 200
    assert c.get(f"/manifests/{m2['id']}/download/train").status_code == 200


def test_withdrawn_patient_never_reincluded_even_with_new_image_nos(client):
    c, svc = client
    upload(c, rows_basic(), quota_full())
    c.post("/partition", follow_redirects=True)
    c.post("/withdrawals", data={"patient_no": "P3"}, follow_redirects=True)

    # 换图像号重新纳入 P3 → 上传被拒绝，不产生新版本，供数状态不变
    rows2 = [r for r in rows_basic() if r[1] != "P3"] + [
        ("NEW01", "P3", "A"), ("NEW02", "P3", "B"),
    ]
    resp = upload(c, rows2, {"A": 2, "B": 2, "C": 1, "D": 1})
    page = resp.data.decode("utf-8")
    assert "已撤回患者" in page and "P3" in page
    assert svc.current_version() is None          # 仍无当前版本（未恢复）
    assert len(svc.list_versions()) == 1          # 没有产生新版本


def test_duplicate_withdrawal_returns_original_record(client):
    c, svc = client
    upload(c, rows_basic(), quota_full())
    c.post("/partition", follow_redirects=True)
    c.post("/withdrawals", data={"patient_no": "P5", "reason": "第一次登记"},
           follow_redirects=True)
    resp = c.post("/withdrawals", data={"patient_no": "P5", "reason": "重复登记"},
                  follow_redirects=True)
    page = resp.data.decode("utf-8")
    assert "已在撤回登记中" in page and "原记录 #1" in page
    ws = svc.list_withdrawals()
    assert len(ws) == 1                       # 不重复生效
    assert ws[0]["reason"] == "第一次登记"     # 原记录不变
    assert ws[0]["invalidated_version_id"] == 1


def test_failed_registration_keeps_supply_state(client):
    c, svc = client
    upload(c, rows_basic(), quota_full())
    c.post("/partition", follow_redirects=True)
    m1 = svc.current_manifest()

    # 空患者号 → 登记失败，不改变原供数状态
    resp = c.post("/withdrawals", data={"patient_no": "   "}, follow_redirects=True)
    assert "患者号不能为空" in resp.data.decode("utf-8")
    assert svc.list_withdrawals() == []
    assert svc.current_manifest() is not None
    assert svc.current_version()["status"] == "active"
    assert c.get(f"/manifests/{m1['id']}/download/val").status_code == 200


def test_withdrawal_not_in_current_version_keeps_supply(client):
    c, svc = client
    upload(c, rows_basic(), quota_full())
    c.post("/partition", follow_redirects=True)
    m1 = svc.current_manifest()

    # 撤回当前版本之外的患者（预防性登记）→ 供数不受影响
    resp = c.post("/withdrawals", data={"patient_no": "PX"}, follow_redirects=True)
    assert "供数不受影响" in resp.data.decode("utf-8")
    assert svc.current_version()["status"] == "active"
    assert svc.current_manifest() is not None
    assert c.get(f"/manifests/{m1['id']}/download/val").status_code == 200

    # 但此后含 PX 的上传一律被拒绝
    rows2 = rows_basic() + [("I11", "PX", "A")]
    resp = upload(c, rows2, {"A": 3, "B": 1, "C": 1, "D": 1})
    assert "已撤回患者" in resp.data.decode("utf-8")
    assert len(svc.list_versions()) == 1


def test_withdrawal_during_solve_blocks_publish(tmp_path):
    """撤回与求解发布交错：按已提交状态裁决，撤回先提交则发布判 stale。"""
    db = str(tmp_path / "w.db")
    svc = Service(db, synchronous=False, solve_delay=0.6)
    svc.create_version(make_csv(rows_basic()), make_dup([]), quota_full())
    job_id = svc.start_partition()
    time.sleep(0.2)
    # 求解期间登记撤回（含该版本的 P3）
    result = svc.register_withdrawal("P3")
    assert result["created"] is True
    assert result["invalidated_version_id"] == 1
    _wait_jobs(svc)
    job = svc.get_job(job_id)
    assert job["status"] == "failed"
    assert job["fail_stage"] == "stale"
    assert "撤回" in job["reason"]
    # 未发布任何清单、未写入任何侧别登记
    conn = get_db(db)
    try:
        assert conn.execute("SELECT COUNT(*) c FROM manifests").fetchone()["c"] == 0
        assert conn.execute("SELECT COUNT(*) c FROM side_registry").fetchone()["c"] == 0
    finally:
        conn.close()
    svc.close()


def test_withdrawal_after_publish_then_reupload_flow(tmp_path):
    """服务层顺序流：发布 → 撤回 → 重传（不含患者）→ 重新划分恢复。"""
    db = str(tmp_path / "f.db")
    svc = Service(db, synchronous=True)
    svc.create_version(make_csv(rows_basic()), make_dup([]), quota_full())
    svc.start_partition()
    assert svc.current_manifest() is not None

    result = svc.register_withdrawal("P7")
    assert result["created"] is True
    assert len(result["affected"]) == 1       # 受影响清单即首份清单
    assert svc.current_manifest() is None

    # 含 P7 的重传被拒绝；不含 P7 的重传可划分恢复
    reg = {r["image_no"]: r["side"] for r in svc.registry_snapshot()}
    rows2 = [r for r in rows_basic() if r[1] != "P7"]
    quota = {}
    for img, _pat, cls in rows2:
        if reg[img] == "val":
            quota[cls] = quota.get(cls, 0) + 1
    try:
        svc.create_version(make_csv(rows_basic()), make_dup([]), quota_full())
        raise AssertionError("含撤回患者的上传应被拒绝")
    except Exception as exc:
        assert "已撤回患者" in str(exc)
    svc.create_version(make_csv(rows2), make_dup([]), quota)
    svc.start_partition()
    m2 = svc.current_manifest()
    assert m2 is not None
    entries = svc.get_manifest_entries(m2["id"])
    assert all(e["patient_no"] != "P7" for e in entries)
    svc.close()
