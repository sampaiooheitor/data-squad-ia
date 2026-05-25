"""Read the latest form response from Google Sheets."""

import logging
import time

import google.auth
from googleapiclient.discovery import build

from config import settings
from src.models import RunContext

logger = logging.getLogger(__name__)

_SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
_POLL_INTERVAL = 3
_MAX_WAIT = 30


def read_response(ctx: RunContext) -> RunContext:
    """Poll the spreadsheet until a new response row appears, then confirm CSV arrived."""
    creds, _ = google.auth.default(scopes=_SCOPES)
    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()

    logger.info("Polling Google Sheets for form response (spreadsheet: %s)", settings.google_spreadsheet_id)

    elapsed = 0
    while elapsed < _MAX_WAIT:
        result = (
            sheet.values()
            .get(spreadsheetId=settings.google_spreadsheet_id, range="A:Z")
            .execute()
        )
        rows = result.get("values", [])
        # rows[0] is header; rows[1:] are responses; take the last one
        if len(rows) >= 2:
            last_row = rows[-1]
            # The first text column after timestamp contains the CSV
            csv_column = _find_csv_column(last_row)
            if csv_column:
                logger.info("Form response received — %d rows in spreadsheet", len(rows) - 1)
                # Confirm we have the CSV (sheets_reader trusts the submit step)
                return ctx

        time.sleep(_POLL_INTERVAL)
        elapsed += _POLL_INTERVAL

    raise TimeoutError(
        f"Google Sheets response not found after {_MAX_WAIT}s. "
        "Check form submission and spreadsheet ID."
    )


def _find_csv_column(row: list[str]) -> str | None:
    """Return the first cell that looks like a CSV (contains commas and newlines or is long)."""
    for cell in row:
        if "\n" in cell or (len(cell) > 100 and "," in cell):
            return cell
    return None
