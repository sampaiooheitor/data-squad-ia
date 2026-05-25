"""Send a Slack notification via incoming webhook."""

import logging

import requests

from config import settings
from src.models import RunContext

logger = logging.getLogger(__name__)


def notify(ctx: RunContext) -> RunContext:
    if not settings.slack_webhook_url:
        logger.warning("SLACK_WEBHOOK_URL not configured — skipping notification")
        return ctx

    payload = {"text": ctx.reporter_message or _fallback_message(ctx)}

    resp = requests.post(
        settings.slack_webhook_url,
        json=payload,
        timeout=10,
    )

    if resp.status_code == 200:
        ctx.slack_sent = True
        logger.info("Slack notification sent")
    else:
        logger.warning(
            "Slack notification failed: %s %s", resp.status_code, resp.text
        )

    return ctx


def _fallback_message(ctx: RunContext) -> str:
    return (
        f"*Data Squad* — run `{ctx.run_id}` completed\n"
        f"• Tema: {ctx.tema}\n"
        f"• Bronze: `{ctx.table_bronze}`\n"
        f"• Silver: `{ctx.table_silver}`\n"
        f"• PR: {ctx.pr_url or 'N/A'}"
    )
