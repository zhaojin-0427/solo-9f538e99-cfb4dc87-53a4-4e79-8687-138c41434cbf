"""Flask 路由：浏览器更正、划分、查看原因、下载两侧清单。"""
from __future__ import annotations

import csv
import io
import json
import os

from flask import (
    Flask,
    Response,
    abort,
    flash,
    redirect,
    render_template,
    request,
    url_for,
)

from .db import VALID, get_db
from .parser import InputError
from .service import Service


def create_app(db_path: str | None = None) -> Flask:
    app = Flask(__name__)
    app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-change-me")
    db_path = db_path or os.environ.get("DB_PATH", "/data/pathology.db")
    synchronous = os.environ.get("PYX_SYNCHRONOUS", "0") == "1"
    solve_delay = float(os.environ.get("PYX_SOLVE_DELAY", "0"))
    svc = Service(db_path, synchronous=synchronous, solve_delay=solve_delay)
    app.extensions["service"] = svc

    # ------------------------------------------------------------ 首页/仪表盘
    @app.route("/")
    def index():
        version = svc.current_version()
        manifest = svc.current_manifest()
        entries = svc.get_manifest_entries(manifest["id"]) if manifest else []
        jobs = svc.list_jobs(limit=8)
        classes = version["class_names"].split(",") if version else []
        quotas = json.loads(version["quotas"]) if version else {}
        val_counts: dict[str, int] = {}
        train_counts: dict[str, int] = {}
        for e in entries:
            bucket = val_counts if e["side"] == VALID else train_counts
            bucket[e["class_name"]] = bucket.get(e["class_name"], 0) + 1
        return render_template(
            "index.html",
            version=version,
            manifest=manifest,
            entries=entries,
            jobs=jobs,
            classes=classes,
            quotas=quotas,
            val_counts=val_counts,
            train_counts=train_counts,
            withdrawal_count=svc.withdrawal_count(),
        )

    # ------------------------------------------------------------ 上传/更正
    @app.route("/upload", methods=["POST"])
    def upload():
        image_file = request.files.get("image_csv")
        dup_file = request.files.get("dup_csv")
        if not image_file or not image_file.filename:
            flash("请选择全量图像 CSV 文件", "error")
            return redirect(url_for("index"))
        # 编码兜底：utf-8-sig → gbk
        image_csv = _decode(image_file.read())
        dup_csv = ""
        if dup_file and dup_file.filename:
            dup_csv = _decode(dup_file.read())
        # 配额字段名由前端按类别动态生成，统一使用 quota::<类别> 前缀
        quota_raw = {
            key[len("quota::"):]: val
            for key, val in request.form.items()
            if key.startswith("quota::")
        }
        try:
            vid, classes = svc.create_version(image_csv, dup_csv, quota_raw)
        except InputError as exc:
            flash(f"输入校验失败，未保存任何内容：{exc}", "error")
            return redirect(url_for("index"))
        flash(
            f"新版本 v{vid} 已生效（类别：{'、'.join(classes)}）。"
            "旧版本及其清单已转为历史，当前下载已停供，重新划分成功后恢复。",
            "success",
        )
        return redirect(url_for("index"))

    # ------------------------------------------------------------ 发起划分
    @app.route("/partition", methods=["POST"])
    def partition():
        try:
            job_id = svc.start_partition()
        except InputError as exc:
            flash(str(exc), "error")
            return redirect(url_for("index"))
        return redirect(url_for("job_detail", job_id=job_id))

    @app.route("/jobs/<int:job_id>")
    def job_detail(job_id: int):
        job = svc.get_job(job_id)
        if job is None:
            abort(404)
        version = svc.get_version(job["version_id"])
        manifest = None
        entries = []
        if job["manifest_id"]:
            conn = get_db(svc.db_path)
            try:
                manifest = conn.execute(
                    "SELECT * FROM manifests WHERE id=?", (job["manifest_id"],)
                ).fetchone()
            finally:
                conn.close()
            entries = svc.get_manifest_entries(job["manifest_id"])
        return render_template(
            "job.html", job=job, version=version, manifest=manifest, entries=entries
        )

    @app.route("/jobs/<int:job_id>/status")
    def job_status(job_id: int):
        job = svc.get_job(job_id)
        if job is None:
            abort(404)
        return {
            "id": job["id"],
            "status": job["status"],
            "fail_stage": job["fail_stage"],
            "reason": job["reason"],
            "manifest_id": job["manifest_id"],
            "done": job["status"] in ("succeeded", "failed"),
        }

    # ------------------------------------------------------------ 历史
    @app.route("/history")
    def history():
        versions = svc.list_versions()
        jobs = svc.list_jobs(limit=100)
        manifests = svc.list_manifests()
        return render_template(
            "history.html", versions=versions, jobs=jobs, manifests=manifests
        )

    @app.route("/versions/<int:version_id>")
    def version_detail(version_id: int):
        version = svc.get_version(version_id)
        if version is None:
            abort(404)
        conn = get_db(svc.db_path)
        try:
            images = conn.execute(
                "SELECT * FROM images WHERE version_id=? ORDER BY image_no",
                (version_id,),
            ).fetchall()
            edges = conn.execute(
                "SELECT * FROM dup_edges WHERE version_id=? ORDER BY id", (version_id,)
            ).fetchall()
            manifest = conn.execute(
                "SELECT * FROM manifests WHERE version_id=? AND status='published'",
                (version_id,),
            ).fetchone()
        finally:
            conn.close()
        return render_template(
            "version.html",
            version=version,
            images=images,
            edges=edges,
            manifest=manifest,
            quotas=json.loads(version["quotas"]),
        )

    # ------------------------------------------------------------ 侧别登记
    @app.route("/registry")
    def registry():
        rows = svc.registry_snapshot()
        return render_template("registry.html", rows=rows)

    # ------------------------------------------------------------ 患者撤回
    @app.route("/withdrawals")
    def withdrawals():
        items = svc.list_withdrawals()
        return render_template("withdrawals.html", items=items)

    @app.route("/withdrawals", methods=["POST"])
    def register_withdrawal():
        patient_no = request.form.get("patient_no", "")
        reason = request.form.get("reason", "")
        try:
            result = svc.register_withdrawal(patient_no, reason)
        except InputError as exc:
            # 登记失败不改变原供数状态
            flash(str(exc), "error")
            return redirect(url_for("withdrawals"))
        rec = result["record"]
        n = len(result["affected"])
        if not result["created"]:
            # 重复撤回：返回原记录，状态不变
            flash(
                f"患者 {rec['patient_no']} 已在撤回登记中（原记录 #{rec['id']}，"
                f"登记于 {rec['created_at']}），撤回不可解除，本次未做更改；"
                f"受影响清单 {n} 份（见下表）。",
                "info",
            )
        elif result["invalidated_version_id"]:
            flash(
                f"患者 {rec['patient_no']} 撤回已登记（记录 #{rec['id']}，不可解除）。"
                f"当前版本 v{result['invalidated_version_id']} 含该患者，已立即失效，"
                f"清单下载已停止；受影响清单 {n} 份。"
                "重新上传不含该患者的全量 CSV 并重新划分后才能恢复供数。",
                "success",
            )
        else:
            flash(
                f"患者 {rec['patient_no']} 撤回已登记（记录 #{rec['id']}，不可解除）。"
                f"当前版本不含该患者，供数不受影响；受影响历史清单 {n} 份（保持停供）。"
                "今后上传含该患者号的 CSV 将被拒绝（即使更换图像号）。",
                "success",
            )
        return redirect(url_for("withdrawals"))

    # ------------------------------------------------------------ 下载
    @app.route("/manifests/<int:manifest_id>/download/<side>")
    def download(manifest_id: int, side):
        if side not in ("train", "val"):
            abort(404)
        conn = get_db(svc.db_path)
        try:
            manifest = conn.execute(
                "SELECT m.*, v.status AS version_status FROM manifests m "
                "JOIN input_versions v ON v.id=m.version_id WHERE m.id=?",
                (manifest_id,),
            ).fetchone()
        finally:
            conn.close()
        if manifest is None or manifest["status"] != "published":
            abort(404)
        # 更正后旧清单仅作历史：非当前版本一律停供下载
        if manifest["version_status"] != "active":
            flash("该清单所属输入版本已被更正，仅作历史保留，当前下载停供。", "error")
            return redirect(url_for("history"))
        # 撤回防线：清单含已撤回患者时一律停供（正常流程下版本失效已先行拦截，
        # 这里在下载边界再核验一次已提交状态，确保回执后新发起的下载绝不含撤回患者）
        conn = get_db(svc.db_path)
        try:
            blocked = conn.execute(
                "SELECT COUNT(*) c FROM manifest_entries me "
                "JOIN patient_withdrawals w ON w.patient_no = me.patient_no "
                "WHERE me.manifest_id=?",
                (manifest_id,),
            ).fetchone()["c"]
        finally:
            conn.close()
        if blocked:
            flash("该清单含已撤回患者，已停止下载；历史明细仅留追溯。", "error")
            return redirect(url_for("history"))
        entries = [e for e in svc.get_manifest_entries(manifest_id) if e["side"] == side]
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["图像号", "患者号", "类别", "侧别", "连通分量"])
        for e in entries:
            writer.writerow(
                [e["image_no"], e["patient_no"], e["class_name"], e["side"], e["component_id"]]
            )
        filename = f"manifest_v{manifest['version_id']}_{side}.csv"
        return Response(
            buf.getvalue(),
            mimetype="text/csv; charset=utf-8",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    return app


def _decode(raw: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "gbk"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")
