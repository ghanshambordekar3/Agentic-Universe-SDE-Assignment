"""
FastAPI webhook receiver with HMAC signature verification and deduplication.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
from typing import Optional

from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import JSONResponse

from .config import SystemConfig
from .clock import ProviderClock
from .db import Database
from .state_machine import apply_call_result

logger = logging.getLogger("webhook_receiver")


def create_webhook_app(cfg: SystemConfig, db: Database, clock: ProviderClock) -> FastAPI:
    app = FastAPI(title="AU Campaign Dispatcher - Webhook Receiver")

    @app.get("/health")
    async def health():
        return {"status": "healthy", "sim_now": clock.now().isoformat()}

    @app.post("/webhooks/telephony")
    async def handle_telephony_webhook(request: Request):
        raw_body = await request.body()
        sig_header = request.headers.get("X-AU-Signature")
        delivery_id = request.headers.get("X-AU-Delivery-Id")

        # 1. HMAC Signature Verification
        if not sig_header:
            logger.warning("Rejecting webhook without X-AU-Signature")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing X-AU-Signature header",
            )

        expected_sig = hmac.new(
            cfg.webhook_secret.encode("utf-8"),
            raw_body,
            hashlib.sha256,
        ).hexdigest()

        if not hmac.compare_digest(sig_header, expected_sig):
            logger.warning("Rejecting webhook with invalid HMAC signature")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Invalid HMAC signature",
            )

        # 2. Check for duplicate delivery_id
        now = clock.now()
        now_iso = now.isoformat()

        if delivery_id:
            with db.transaction() as conn:
                cur = conn.execute(
                    "SELECT delivery_id FROM processed_webhooks WHERE delivery_id = ?",
                    (delivery_id,),
                )
                if cur.fetchone() is not None:
                    # Idempotent replay of duplicate webhook delivery
                    return JSONResponse(
                        content={"status": "duplicate_delivery_ignored"},
                        status_code=status.HTTP_200_OK,
                    )
                conn.execute(
                    "INSERT INTO processed_webhooks (delivery_id, received_at) VALUES (?, ?)",
                    (delivery_id, now_iso),
                )

        # 3. Parse JSON and apply call result
        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except Exception as e:
            logger.error("Malformed JSON in webhook: %s", e)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Malformed JSON body",
            )

        try:
            apply_call_result(db, payload, cfg.retry_policy, now)
        except Exception as e:
            logger.exception("Error applying call result from webhook: %s", e)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal state error",
            )

        return JSONResponse(content={"status": "accepted"}, status_code=status.HTTP_200_OK)

    return app
