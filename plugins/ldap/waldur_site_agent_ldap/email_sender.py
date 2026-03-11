"""Welcome email sender for LDAP user provisioning."""

from __future__ import annotations

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from jinja2 import ChainableUndefined, Environment, FileSystemLoader

from waldur_site_agent.backend import logger


class WelcomeEmailSender:
    """Sends templated welcome emails to newly provisioned LDAP users via SMTP."""

    def __init__(self, settings: dict) -> None:
        """Initialize from a welcome_email settings dict."""
        self.smtp_host: str = settings["smtp_host"]
        self.smtp_port: int = settings.get("smtp_port", 587)
        self.smtp_username: str | None = settings.get("smtp_username")
        self.smtp_password: str | None = settings.get("smtp_password")
        self.use_tls: bool = settings.get("use_tls", True)
        self.use_ssl: bool = settings.get("use_ssl", False)
        self.timeout: int = settings.get("timeout", 30)

        self.from_address: str = settings["from_address"]
        self.from_name: str | None = settings.get("from_name")
        self.subject_template_str: str = settings.get(
            "subject", "Your new account has been created"
        )

        template_path = Path(settings["template_path"])
        if not template_path.is_absolute():
            template_path = Path.cwd() / template_path
        template_path = template_path.resolve()

        if not template_path.is_file():
            logger.warning(
                "Welcome email template not found at %s — emails will not be sent",
                template_path,
            )

        self._env = Environment(
            loader=FileSystemLoader(str(template_path.parent)),
            undefined=ChainableUndefined,
            trim_blocks=True,
            lstrip_blocks=True,
            autoescape=True,
        )
        self._template_name = template_path.name
        logger.info("Welcome email template loaded from %s", template_path)

    def send_welcome_email(self, recipient_email: str, **template_vars: str) -> None:
        """Render the template and send the email. Never raises."""
        try:
            self._send(recipient_email, template_vars)
        except Exception:
            logger.exception(
                "Failed to send welcome email to %s — user was still created successfully",
                recipient_email,
            )

    def _send(self, recipient_email: str, template_vars: dict[str, str]) -> None:
        body_template = self._env.get_template(self._template_name)
        body = body_template.render(**template_vars)

        subject_template = self._env.from_string(self.subject_template_str)
        subject = subject_template.render(**template_vars)

        sender = (
            f"{self.from_name} <{self.from_address}>"
            if self.from_name
            else self.from_address
        )

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = sender
        msg["To"] = recipient_email

        # Detect HTML vs plain text from template content
        if "<html" in body.lower() or "<body" in body.lower():
            msg.attach(MIMEText(body, "html"))
        else:
            msg.attach(MIMEText(body, "plain"))

        smtp: smtplib.SMTP
        if self.use_ssl:
            smtp = smtplib.SMTP_SSL(
                self.smtp_host, self.smtp_port, timeout=self.timeout
            )
        else:
            smtp = smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=self.timeout)

        try:
            if self.use_tls and not self.use_ssl:
                smtp.starttls()
            if self.smtp_username and self.smtp_password:
                smtp.login(self.smtp_username, self.smtp_password)
            smtp.sendmail(self.from_address, [recipient_email], msg.as_string())
            logger.info("Welcome email sent to %s", recipient_email)
        finally:
            smtp.quit()
