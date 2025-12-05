import smtplib
from email.message import EmailMessage
from typing import Any, Dict, List

import logging

LOGGER = logging.getLogger("UG_Survey")


def _normalize_recipients(value: Any) -> List[str]:
    """
    Normalize email recipient config into a list of addresses.

    Accepts:
      - "a@b.com"
      - "a@b.com, c@d.com"
      - ["a@b.com", "c@d.com"]
      - ["a@b.com, c@d.com"]
    """
    if not value:
        return []

    if isinstance(value, str):
        return [v.strip() for v in value.split(",") if v.strip()]

    if isinstance(value, (list, tuple)):
        out: List[str] = []
        for item in value:
            if isinstance(item, str):
                out.extend([v.strip() for v in item.split(",") if v.strip()])
        return out

    return []


def send_email(em: Dict[str, Any], subject: str, body: str) -> None:
    """
    Send a short status email using the same email config structure
    as other modules (settings.yaml -> email section).

    em should look like:
      {
        "enabled": True,
        "smtp_host": "...",
        "smtp_port": 25,
        "use_starttls": False,
        "from_addr": "L-DWEMAIL@LISTS.PSU.EDU",
        "to_addrs": "L-sarakaj@psu.edu",
        "subject_prefix": "[UGSurvey]"
      }
    """

    if not em.get("enabled", True):
        LOGGER.info("Email disabled by config; not sending load_raw notification.")
        return

    smtp_host = em.get("smtp_host", "smtp.psu.edu")
    smtp_port = int(em.get("smtp_port", 25))
    use_starttls = bool(em.get("use_starttls", False))

    from_addr = em.get("from_addr", "L-DWEMAIL@LISTS.PSU.EDU")
    to_list = _normalize_recipients(em.get("to_addrs")) or [from_addr]

    subject_prefix = em.get("subject_prefix", "[UGSurvey]")
    full_subject = f"{subject_prefix} {subject}"

    msg = EmailMessage()
    msg["From"] = from_addr
    msg["To"] = ", ".join(to_list)          # ✅ now valid
    msg["Subject"] = full_subject
    msg.set_content(body)

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as smtp:
            if use_starttls:
                smtp.starttls()
            smtp.send_message(msg)
        LOGGER.info("load_raw: Email sent to %s", to_list)
    except Exception as ex:
        LOGGER.error("load_raw: Failed to send email: %s", ex)
