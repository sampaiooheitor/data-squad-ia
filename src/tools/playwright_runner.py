"""Submit the generated CSV to a Google Form via browser automation."""

import logging

from playwright.sync_api import sync_playwright

from config import settings
from src.models import RunContext

logger = logging.getLogger(__name__)


def submit_form(ctx: RunContext) -> RunContext:
    """Fill Google Form fields with CSV data and data dictionary, then submit."""
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()

        logger.info("Navigating to Google Form: %s", settings.google_form_url)
        page.goto(settings.google_form_url, wait_until="networkidle")

        # Google Forms renders text areas as contenteditable divs or textarea elements.
        # We target paragraph-type (long answer) fields in order of appearance.
        text_inputs = page.locator("textarea, div[contenteditable='true']").all()

        if len(text_inputs) < 1:
            raise RuntimeError(
                "Google Form has no text input fields. "
                "Ensure the form has at least one 'Paragraph' question."
            )

        logger.info("Filling field 1 with CSV data (%d chars)", len(ctx.csv_content))
        text_inputs[0].click()
        text_inputs[0].fill(ctx.csv_content)

        if len(text_inputs) >= 2 and ctx.data_dict:
            dict_json = ctx.data_dict.model_dump_json(indent=2)
            logger.info("Filling field 2 with data dictionary")
            text_inputs[1].click()
            text_inputs[1].fill(dict_json)

        # Click the submit button
        submit_btn = page.locator(
            "div[role='button']:has-text('Submit'), "
            "div[role='button']:has-text('Enviar'), "
            "span:has-text('Submit'), "
            "span:has-text('Enviar')"
        ).first
        submit_btn.click()

        # Wait for confirmation page
        page.wait_for_url("**/formResponse**", timeout=15_000)
        logger.info("Form submitted successfully")

        browser.close()

    return ctx
