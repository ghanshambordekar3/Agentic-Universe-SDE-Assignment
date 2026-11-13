"""
Exporter for final_state.csv according to the assignment output contract.
"""
from __future__ import annotations

import csv
import os
from typing import List, Dict, Any

from .db import Database


def export_final_state(db: Database, out_path: str = "final_state.csv") -> str:
    """
    Exports final_state.csv sorted by campaign_id, lead_id.
    Matches provider expected schema and values:
    campaign_id,lead_id,final_state,attempts,last_status,last_disposition
    """
    with db.transaction() as conn:
        cur = conn.execute(
            """
            SELECT campaign_id, lead_id, status, attempts, last_status, last_disposition
            FROM leads
            ORDER BY campaign_id ASC, lead_id ASC
            """
        )
        rows = cur.fetchall()

    out_dir = os.path.dirname(os.path.abspath(out_path))
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["campaign_id", "lead_id", "final_state", "attempts", "last_status", "last_disposition"])
        for r in rows:
            final_state = r["status"].lower()  # completed, exhausted, dnd_blocked
            attempts = r["attempts"]
            last_status = r["last_status"] or ""
            last_disp = r["last_disposition"] or ""
            writer.writerow([
                r["campaign_id"],
                r["lead_id"],
                final_state,
                attempts,
                last_status,
                last_disp,
            ])

    return os.path.abspath(out_path)
