"""业务服务层：输入版本、作业调度、原子发布、历史与下载。"""
from __future__ import annotations

import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from . import db as dbmod
from .db import get_db
from .parser import (
    InputError,
    parse_dups_csv,
    parse_images_csv,
    parse_quota,
    validate_and_build,
)
from .solver import SolveError, solve


class Service:
    def __init__(self, db_path: str, synchronous: bool = False, solve_delay: float = 0.0):
        self.db_path = db_path
        # synchronous=True：作业在请求线程内立刻执行，便于自测确定性
        self.synchronous = synchronous
        self.solve_delay = solve_delay
        self._pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="partition")
        self._lock = threading.Lock()
        dbmod.init_db(db_path)

    def close(self) -> None:
        """关闭后台线程池，避免多实例下线程跨库争用作业。"""
        self._pool.shutdown(wait=False, cancel_futures=True)

    # ------------------------------------------------------------------ 版本
    def create_version(
        self, image_csv: str, dup_csv: str, quota_raw: dict
    ) -> tuple[int, list[str]]:
        """解析+校验全部输入，通过后整体写入新版本；旧版本立即失效。

        返回 (version_id, 警告信息列表)。任何一步失败都不写库。
        """
        images = parse_images_csv(image_csv)
        dup_edges = parse_dups_csv(dup_csv or "")
        components, classes = validate_and_build(images, dup_edges)
        quota = parse_quota(quota_raw, classes)
        total_imgs = len(images)
        total_quota = sum(quota.values())
        if total_quota > total_imgs:
            raise InputError(
                f"验证配额合计 {total_quota} 张，超过全量图像 {total_imgs} 张"
            )

        conn = get_db(self.db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            # 已撤回患者不得重新纳入（即使更换图像号）。校验与版本写入在同一事务：
            # 与撤回登记交错时按已提交状态裁决——撤回先提交则此处必能读到并拒绝；
            # 本版本先提交则后续撤回会把它判失效。
            withdrawn = {
                r["patient_no"]
                for r in conn.execute(
                    "SELECT patient_no FROM patient_withdrawals"
                ).fetchall()
            }
            blocked = sorted({r.patient_no for r in images} & withdrawn)
            if blocked:
                raise InputError(
                    "全量 CSV 含已撤回患者（"
                    + "、".join(blocked)
                    + "）：撤回不可解除，即使更换图像号也不得重新纳入，本次上传未保存任何内容。"
                    "请移除上述患者的全部图像后重新上传。"
                )
            cur = conn.execute(
                "INSERT INTO input_versions(image_csv, dup_csv, image_count, "
                "class_names, quotas, note) VALUES (?,?,?,?,?,?)",
                (
                    image_csv,
                    dup_csv or "",
                    total_imgs,
                    ",".join(classes),
                    json.dumps(quota, ensure_ascii=False),
                    f"用户上传于 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                ),
            )
            vid = cur.lastrowid
            conn.executemany(
                "INSERT INTO images(version_id, image_no, patient_no, class_name) "
                "VALUES (?,?,?,?)",
                [(vid, r.image_no, r.patient_no, r.class_name) for r in images],
            )
            conn.executemany(
                "INSERT INTO dup_edges(version_id, image_a, image_b) VALUES (?,?,?)",
                [(vid, a, b) for a, b in dup_edges],
            )
            # 新版本生效的同一事务里，旧的当前版本转为历史；
            # 已被撤回判定失效的版本保持 invalidated，留作追溯。
            conn.execute(
                "UPDATE input_versions SET status='superseded' "
                "WHERE id<>? AND status='active'",
                (vid,),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()
        return vid, classes

    def current_version(self) -> sqlite3.Row | None:
        conn = get_db(self.db_path)
        try:
            return conn.execute(
                "SELECT * FROM input_versions WHERE status='active' "
                "ORDER BY id DESC LIMIT 1"
            ).fetchone()
        finally:
            conn.close()

    def get_version(self, vid: int) -> sqlite3.Row | None:
        conn = get_db(self.db_path)
        try:
            return conn.execute(
                "SELECT * FROM input_versions WHERE id=?", (vid,)
            ).fetchone()
        finally:
            conn.close()

    def list_versions(self) -> list[sqlite3.Row]:
        conn = get_db(self.db_path)
        try:
            return list(
                conn.execute("SELECT * FROM input_versions ORDER BY id DESC").fetchall()
            )
        finally:
            conn.close()

    # ------------------------------------------------------------------ 作业
    def start_partition(self) -> int:
        version = self.current_version()
        if version is None:
            raise InputError("尚未上传任何输入数据，请先上传 CSV 并填写配额")
        # 部分唯一索引 uq_job_active 保证同版本至多一个进行中/成功的作业。
        # - 并发发起：INSERT OR IGNORE 撞约束，复用同一作业；
        # - 失败作业不占约束位，允许重新发起（新作业行，历史失败仍保留）。
        with self._lock:
            conn = get_db(self.db_path)
            try:
                cur = conn.execute(
                    "INSERT OR IGNORE INTO partition_jobs(version_id, status) "
                    "VALUES (?,'queued')",
                    (version["id"],),
                )
                if cur.rowcount == 1:
                    job_id = cur.lastrowid
                    is_new = True
                else:
                    row = conn.execute(
                        "SELECT id FROM partition_jobs WHERE version_id=? "
                        "AND status IN ('queued','running','succeeded') ORDER BY id LIMIT 1",
                        (version["id"],),
                    ).fetchone()
                    job_id = row["id"]
                    is_new = False
            finally:
                conn.close()

        # 只有新插入的作业需要投递；已有作业（含运行中/已成功）直接返回其 id
        if is_new:
            if self.synchronous:
                self.run_job(job_id)
            else:
                self._pool.submit(self.run_job, job_id)
        return job_id

    def get_job(self, job_id: int) -> sqlite3.Row | None:
        conn = get_db(self.db_path)
        try:
            return conn.execute(
                "SELECT * FROM partition_jobs WHERE id=?", (job_id,)
            ).fetchone()
        finally:
            conn.close()

    def list_jobs(self, limit: int = 20) -> list[sqlite3.Row]:
        conn = get_db(self.db_path)
        try:
            return list(
                conn.execute(
                    "SELECT j.*, v.status AS version_status FROM partition_jobs j "
                    "JOIN input_versions v ON v.id=j.version_id "
                    "ORDER BY j.id DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            )
        finally:
            conn.close()

    def run_job(self, job_id: int) -> None:
        """作业主体：求解 + 原子发布。整个发布在单个写事务内，失败不留半成品。"""
        conn = get_db(self.db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            job = conn.execute(
                "SELECT * FROM partition_jobs WHERE id=?", (job_id,)
            ).fetchone()
            if job is None or job["status"] not in ("queued", "running"):
                conn.execute("ROLLBACK")
                return
            version = conn.execute(
                "SELECT * FROM input_versions WHERE id=?", (job["version_id"],)
            ).fetchone()
            # 过期依据不得发布：求解期间输入被更正（superseded）或患者撤回（invalidated）
            if version["status"] != "active":
                conn.execute(
                    "UPDATE partition_jobs SET status='failed', fail_stage='stale', "
                    "finished_at=datetime('now'), reason=? WHERE id=?",
                    (
                        "求解期间输入或配额已被更正，或患者撤回使该版本失效，"
                        "该版本已成为历史，过期依据不得发布。请基于当前版本重新划分。",
                        job_id,
                    ),
                )
                conn.execute("COMMIT")
                return
            # 并发保护：同版本已有成功清单则直接复用，不产生第二份
            existing = conn.execute(
                "SELECT id FROM manifests WHERE version_id=? AND status='published'",
                (version["id"],),
            ).fetchone()
            if existing:
                conn.execute(
                    "UPDATE partition_jobs SET status='succeeded', "
                    "finished_at=datetime('now'), manifest_id=? WHERE id=?",
                    (existing["id"], job_id),
                )
                conn.execute("COMMIT")
                return

            conn.execute(
                "UPDATE partition_jobs SET status='running', "
                "started_at=datetime('now') WHERE id=?",
                (job_id,),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

        # ---- 事务外求解（CPU 工作，不持有写锁；发布前再次校验版本新鲜度）----
        try:
            result_data = self._compute(version)
        except SolveError as exc:
            self._fail_job(job_id, exc.stage, exc.reason, exc.detail)
            return
        except InputError as exc:
            self._fail_job(job_id, "validation", str(exc), {})
            return
        except Exception as exc:  # 兜底：任何意外都收敛为失败作业，绝不悬挂
            self._fail_job(job_id, "error", f"求解发生未预期错误：{exc}", {})
            return

        self._publish(job_id, version["id"], result_data)

    def _compute(self, version: sqlite3.Row) -> dict:
        images = parse_images_csv(version["image_csv"])
        dup_edges = parse_dups_csv(version["dup_csv"])
        components, classes = validate_and_build(images, dup_edges)
        quota = json.loads(version["quotas"])
        conn = get_db(self.db_path)
        try:
            registry = {
                r["image_no"]: r["side"]
                for r in conn.execute("SELECT image_no, side FROM side_registry").fetchall()
            }
        finally:
            conn.close()
        if self.solve_delay:
            import time

            time.sleep(self.solve_delay)
        result = solve(components, classes, quota, registry)
        img_index = {r.image_no: r for r in images}
        entries = []
        for comp in components:
            side = result.assignment[comp.cid]
            for img in comp.image_nos:
                row = img_index[img]
                entries.append(
                    {
                        "image_no": img,
                        "patient_no": row.patient_no,
                        "class_name": row.class_name,
                        "component_id": comp.cid,
                        "side": side,
                    }
                )
        return {
            "entries": entries,
            "classes": classes,
            "quota": quota,
            "locked": [
                {
                    "component_id": lc.component.cid,
                    "side": lc.side,
                    "locked_images": lc.locked_images,
                }
                for lc in result.locked
            ],
        }

    def _publish(self, job_id: int, version_id: int, data: dict) -> None:
        """原子发布：版本新鲜度复查 → 清单 → 侧别登记 → 作业成功，同一事务。"""
        conn = get_db(self.db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            version = conn.execute(
                "SELECT status FROM input_versions WHERE id=?", (version_id,)
            ).fetchone()
            if version["status"] != "active":
                conn.execute(
                    "UPDATE partition_jobs SET status='failed', fail_stage='stale', "
                    "finished_at=datetime('now'), reason=? WHERE id=?",
                    (
                        "求解期间输入或配额已被更正，或患者撤回使该版本失效，"
                        "过期依据不得发布。",
                        job_id,
                    ),
                )
                conn.execute("COMMIT")
                return
            dup = conn.execute(
                "SELECT id FROM manifests WHERE version_id=? AND status='published'",
                (version_id,),
            ).fetchone()
            if dup:
                conn.execute(
                    "UPDATE partition_jobs SET status='succeeded', "
                    "finished_at=datetime('now'), manifest_id=? WHERE id=?",
                    (dup["id"], job_id),
                )
                conn.execute("COMMIT")
                return

            # 登记侧别前再次核对：永久登记与本次分配必须一致，冲突则整体失败
            for e in data["entries"]:
                row = conn.execute(
                    "SELECT side FROM side_registry WHERE image_no=?",
                    (e["image_no"],),
                ).fetchone()
                if row is not None and row["side"] != e["side"]:
                    conn.execute(
                        "UPDATE partition_jobs SET status='failed', fail_stage='conflict', "
                        "finished_at=datetime('now'), reason=? WHERE id=?",
                        (
                            f"关联冲突：图像 {e['image_no']} 曾发布且侧别永久固定为 "
                            f"{row['side']}，本次求解结果为 {e['side']}，已中止发布，未写入任何分配。",
                            job_id,
                        ),
                    )
                    conn.execute("COMMIT")
                    return

            cur = conn.execute(
                "INSERT INTO manifests(version_id, status, reason) "
                "VALUES (?,'published', NULL)",
                (version_id,),
            )
            manifest_id = cur.lastrowid
            conn.executemany(
                "INSERT INTO manifest_entries(manifest_id, image_no, patient_no, "
                "class_name, component_id, side) VALUES (?,?,?,?,?,?)",
                [
                    (
                        manifest_id,
                        e["image_no"],
                        e["patient_no"],
                        e["class_name"],
                        e["component_id"],
                        e["side"],
                    )
                    for e in data["entries"]
                ],
            )
            # 永久侧别登记：已存在的不动（INSERT OR IGNORE 配合上面的冲突复查）
            conn.executemany(
                "INSERT OR IGNORE INTO side_registry(image_no, side, first_manifest_id) "
                "VALUES (?,?,?)",
                [
                    (e["image_no"], e["side"], manifest_id) for e in data["entries"]
                ],
            )
            conn.execute(
                "UPDATE partition_jobs SET status='succeeded', finished_at=datetime('now'), "
                "manifest_id=? WHERE id=?",
                (manifest_id, job_id),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            # 事务回滚后把作业标记失败（独立事务），避免悬挂
            try:
                self._fail_job(job_id, "error", "发布时发生数据库错误，事务已整体回滚，未留下半次分配。", {})
            except Exception:
                pass
            raise
        finally:
            conn.close()

    def _fail_job(self, job_id: int, stage: str, reason: str, detail: dict) -> None:
        # 失败只登记在 partition_jobs（历史/原因页展示），绝不产生半成品 manifest。
        # reason 存人类可读说明，detail 单独成列存结构化诊断。
        conn = get_db(self.db_path)
        try:
            conn.execute(
                "UPDATE partition_jobs SET status='failed', fail_stage=?, reason=?, "
                "detail=?, finished_at=datetime('now') WHERE id=?",
                (stage, reason, json.dumps(detail, ensure_ascii=False) if detail else None, job_id),
            )
        finally:
            conn.close()

    # ------------------------------------------------------------------ 清单
    def current_manifest(self) -> sqlite3.Row | None:
        version = self.current_version()
        if version is None:
            return None
        conn = get_db(self.db_path)
        try:
            return conn.execute(
                "SELECT * FROM manifests WHERE version_id=? AND status='published'",
                (version["id"],),
            ).fetchone()
        finally:
            conn.close()

    def get_manifest_entries(self, manifest_id: int) -> list[sqlite3.Row]:
        conn = get_db(self.db_path)
        try:
            return list(
                conn.execute(
                    "SELECT * FROM manifest_entries WHERE manifest_id=? "
                    "ORDER BY side DESC, class_name, image_no",
                    (manifest_id,),
                ).fetchall()
            )
        finally:
            conn.close()

    def list_manifests(self) -> list[sqlite3.Row]:
        conn = get_db(self.db_path)
        try:
            return list(
                conn.execute(
                    "SELECT m.*, v.status AS version_status, v.image_count "
                    "FROM manifests m JOIN input_versions v ON v.id=m.version_id "
                    "ORDER BY m.id DESC"
                ).fetchall()
            )
        finally:
            conn.close()

    def registry_snapshot(self) -> list[sqlite3.Row]:
        conn = get_db(self.db_path)
        try:
            return list(
                conn.execute(
                    "SELECT * FROM side_registry ORDER BY image_no"
                ).fetchall()
            )
        finally:
            conn.close()

    # ------------------------------------------------------------------ 撤回
    def register_withdrawal(self, patient_no: str, reason: str = "") -> dict:
        """登记患者撤回（不可解除）。幂等：重复登记返回原记录，不改任何状态。

        登记与失效判定在同一写事务内完成：
        - 含该患者的当前 active 版本立即转为 invalidated，其清单随即停供；
        - 进行中/排队作业在发布前复查版本状态时会判 stale，不会发布含该患者的清单；
        - 与上传/发布交错时按已提交状态裁决（谁先提交谁生效）。
        返回 {"record", "created", "affected", "invalidated_version_id"}。
        """
        patient_no = (patient_no or "").strip()
        if not patient_no:
            raise InputError("患者号不能为空，撤回登记未生效，供数状态未改变")
        reason = (reason or "").strip()

        conn = get_db(self.db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                "SELECT * FROM patient_withdrawals WHERE patient_no=?",
                (patient_no,),
            ).fetchone()
            if existing is not None:
                # 重复撤回：返回原记录，不做任何更改
                record = dict(existing)
                created = False
                invalidated = existing["invalidated_version_id"]
            else:
                # 含该患者的当前版本立即失效（同事务，清单下载随即停止）
                version = conn.execute(
                    "SELECT id FROM input_versions WHERE status='active' "
                    "ORDER BY id DESC LIMIT 1"
                ).fetchone()
                invalidated = None
                if version is not None:
                    hit = conn.execute(
                        "SELECT COUNT(*) c FROM images "
                        "WHERE version_id=? AND patient_no=?",
                        (version["id"], patient_no),
                    ).fetchone()["c"]
                    if hit:
                        conn.execute(
                            "UPDATE input_versions SET status='invalidated' "
                            "WHERE id=? AND status='active'",
                            (version["id"],),
                        )
                        invalidated = version["id"]
                cur = conn.execute(
                    "INSERT INTO patient_withdrawals"
                    "(patient_no, reason, invalidated_version_id) VALUES (?,?,?)",
                    (patient_no, reason, invalidated),
                )
                record = dict(
                    conn.execute(
                        "SELECT * FROM patient_withdrawals WHERE id=?",
                        (cur.lastrowid,),
                    ).fetchone()
                )
                created = True
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()
        return {
            "record": record,
            "created": created,
            "affected": self.affected_manifests(patient_no),
            "invalidated_version_id": invalidated,
        }

    def affected_manifests(self, patient_no: str) -> list[dict]:
        """含该患者的全部已发布清单（含已失效/历史版本），供追溯查看；均不可下载。"""
        conn = get_db(self.db_path)
        try:
            rows = conn.execute(
                "SELECT m.id AS manifest_id, m.version_id, m.created_at, "
                "v.status AS version_status, "
                "COUNT(me.id) AS hit_count, "
                "GROUP_CONCAT(DISTINCT me.image_no) AS image_nos, "
                "GROUP_CONCAT(DISTINCT me.side) AS sides "
                "FROM manifest_entries me "
                "JOIN manifests m ON m.id=me.manifest_id "
                "JOIN input_versions v ON v.id=m.version_id "
                "WHERE me.patient_no=? AND m.status='published' "
                "GROUP BY m.id ORDER BY m.id DESC",
                (patient_no,),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def list_withdrawals(self) -> list[dict]:
        """全部撤回登记（含各自受影响清单），按登记时间倒序。"""
        conn = get_db(self.db_path)
        try:
            rows = conn.execute(
                "SELECT * FROM patient_withdrawals ORDER BY id DESC"
            ).fetchall()
        finally:
            conn.close()
        out = []
        for r in rows:
            d = dict(r)
            d["affected"] = self.affected_manifests(r["patient_no"])
            out.append(d)
        return out

    def withdrawal_count(self) -> int:
        conn = get_db(self.db_path)
        try:
            return conn.execute(
                "SELECT COUNT(*) c FROM patient_withdrawals"
            ).fetchone()["c"]
        finally:
            conn.close()
