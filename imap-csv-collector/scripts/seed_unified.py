"""为本地 IMAP 样例服务追加一封“项目名/单位不统一”的仪器邮件。

在 docker compose 的 imap 容器启动时（MAIL_SEED_UNIFIED=1）执行，
让演示一开始就同时具备：可被规则统一的项目（铅Pb/Pb、镉Cd）、
缺映射的项目（砷As）与非数值结果（<0.01）。

脚本只依赖标准库与同仓库 imap_server 包；重复执行时按 Message-ID 跳过。
"""
from __future__ import annotations

import email
import os
import sys

# 脚本位于 scripts/ 子目录，显式把项目根加入模块路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from imap_server import mailbox, seed as seed_mod

MESSAGE_ID = "inst-unified-0001@lab-instrument"


def _csv(rows: list[tuple[str, str, str, str]]) -> bytes:
    header = "样品编号,检测项目,结果,单位"
    lines = [header] + [",".join(row) for row in rows]
    return ("\r\n".join(lines) + "\r\n").encode("utf-8")


def main() -> int:
    root = os.environ.get("MAIL_ROOT", "/srv/mail")
    folder = os.environ.get("MAIL_FOLDER", "INBOX")
    # 先保证基础种子邮件（含坏附件停采场景）存在，再追加统一场景邮件
    seed_mod.seed(root, folder)
    mailbox.ensure_folder(root, folder)

    for uid in mailbox.list_uids(root, folder):
        raw = mailbox.read_message(root, folder, uid) or b""
        if MESSAGE_ID in email.message_from_bytes(raw).get("Message-ID", ""):
            print(f"统一演示邮件已存在（UID={uid}），跳过")
            return 0

    raw = mailbox.build_message(
        message_id=MESSAGE_ID,
        subject="仪器结果 批次 U1（项目名与单位待统一）",
        sender="instrument-c@lab.local",
        recipient="lab@lab.local",
        attachments=[
            (
                "batch_u1.csv",
                _csv(
                    [
                        ("S-1001", "Pb", "0.0045", "mg/kg"),
                        ("S-1001", "Cd", "0.0012", "mg/kg"),
                        ("S-1002", "Pb", "0.030", "mg/kg"),
                        ("S-1002", "Hg", "0.005", "mg/kg"),
                        ("S-1003", "Hg", "5.0", "μg/kg"),
                        ("S-1004", "As", "<0.01", "mg/kg"),
                        ("S-1006", "Cr", "44", "μg/kg"),
                    ]
                ),
            )
        ],
    )
    uid = mailbox.append_message(root, folder, raw)
    print(f"已写入统一演示邮件 UID={uid} Message-ID={MESSAGE_ID}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
