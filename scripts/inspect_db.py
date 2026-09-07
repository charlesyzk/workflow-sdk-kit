from __future__ import annotations

import sqlite3
import sys
from pathlib import Path


database = Path(sys.argv[1] if len(sys.argv) > 1 else "workflow_demo.db").resolve()
tables = [
    "obei_workshop_task",
    "obei_workshop_task_run",
    "obei_workshop_task_checkpoint",
    "obei_workshop_task_node_execution",
    "obei_workshop_task_artifact",
    "obei_workshop_task_event",
    "obei_workshop_task_decision",
]

if not database.exists():
    raise SystemExit(f"database not found: {database}")

with sqlite3.connect(database) as connection:
    for table in tables:
        count = connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        print(f"{table:<45} {count:>6}")

