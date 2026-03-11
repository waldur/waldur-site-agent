"""Tests for the welcome email sender."""

from __future__ import annotations

import smtplib
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from waldur_site_agent_ldap.email_sender import WelcomeEmailSender

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture()
def txt_template(tmp_path: Path) -> Path:
    tpl = tmp_path / "welcome.txt.j2"
    tpl.write_text(
        "Hello {{ first_name }},\n\n"
        "Username: {{ username }}\n"
        "{% if vpn_password %}VPN password: {{ vpn_password }}\n{% endif %}"
        "Home: {{ home_directory }}\n"
    )
    return tpl


@pytest.fixture()
def html_template(tmp_path: Path) -> Path:
    tpl = tmp_path / "welcome.html.j2"
    tpl.write_text(
        "<html><body>\n"
        "<p>Hello {{ first_name }},</p>\n"
        "<p>Username: {{ username }}</p>\n"
        "{% if vpn_password %}<p>VPN: {{ vpn_password }}</p>\n{% endif %}"
        "</body></html>\n"
    )
    return tpl


def _settings(template_path: str, **overrides: object) -> dict:
    base = {
        "smtp_host": "smtp.test.local",
        "smtp_port": 587,
        "from_address": "noreply@test.local",
        "use_tls": True,
        "use_ssl": False,
        "template_path": template_path,
    }
    base.update(overrides)
    return base


class TestWelcomeEmailSender:
    def test_plain_text_email_sent(self, txt_template: Path) -> None:
        sender = WelcomeEmailSender(_settings(str(txt_template)))

        with patch("waldur_site_agent_ldap.email_sender.smtplib") as mock_smtplib:
            mock_smtp = MagicMock()
            mock_smtplib.SMTP.return_value = mock_smtp

            sender.send_welcome_email(
                recipient_email="user@example.com",
                username="jsmith",
                first_name="John",
                last_name="Smith",
                vpn_password="s3cret",
                home_directory="/home/jsmith",
            )

            mock_smtp.starttls.assert_called_once()
            mock_smtp.sendmail.assert_called_once()
            args = mock_smtp.sendmail.call_args
            assert args[0][0] == "noreply@test.local"
            assert args[0][1] == ["user@example.com"]
            msg_str = args[0][2]
            assert "jsmith" in msg_str
            assert "s3cret" in msg_str
            assert "Hello John" in msg_str
            mock_smtp.quit.assert_called_once()

    def test_html_email_sent(self, html_template: Path) -> None:
        sender = WelcomeEmailSender(_settings(str(html_template)))

        with patch("waldur_site_agent_ldap.email_sender.smtplib") as mock_smtplib:
            mock_smtp = MagicMock()
            mock_smtplib.SMTP.return_value = mock_smtp

            sender.send_welcome_email(
                recipient_email="user@example.com",
                username="jsmith",
                first_name="John",
                last_name="Smith",
                home_directory="/home/jsmith",
            )

            args = mock_smtp.sendmail.call_args
            msg_str = args[0][2]
            assert "text/html" in msg_str

    def test_ssl_mode(self, txt_template: Path) -> None:
        sender = WelcomeEmailSender(
            _settings(str(txt_template), use_ssl=True, use_tls=False)
        )

        with patch("waldur_site_agent_ldap.email_sender.smtplib") as mock_smtplib:
            mock_smtp_ssl = MagicMock()
            mock_smtplib.SMTP_SSL.return_value = mock_smtp_ssl

            sender.send_welcome_email(
                recipient_email="user@example.com",
                username="jsmith",
                first_name="John",
                last_name="Smith",
                home_directory="/home/jsmith",
            )

            mock_smtplib.SMTP_SSL.assert_called_once()
            mock_smtp_ssl.starttls.assert_not_called()
            mock_smtp_ssl.sendmail.assert_called_once()

    def test_authentication(self, txt_template: Path) -> None:
        sender = WelcomeEmailSender(
            _settings(
                str(txt_template),
                smtp_username="user",
                smtp_password="pass",
            )
        )

        with patch("waldur_site_agent_ldap.email_sender.smtplib") as mock_smtplib:
            mock_smtp = MagicMock()
            mock_smtplib.SMTP.return_value = mock_smtp

            sender.send_welcome_email(
                recipient_email="user@example.com",
                username="jsmith",
                first_name="John",
                last_name="Smith",
                home_directory="/home/jsmith",
            )

            mock_smtp.login.assert_called_once_with("user", "pass")

    def test_no_auth_when_credentials_missing(self, txt_template: Path) -> None:
        sender = WelcomeEmailSender(_settings(str(txt_template)))

        with patch("waldur_site_agent_ldap.email_sender.smtplib") as mock_smtplib:
            mock_smtp = MagicMock()
            mock_smtplib.SMTP.return_value = mock_smtp

            sender.send_welcome_email(
                recipient_email="user@example.com",
                username="jsmith",
                first_name="John",
                last_name="Smith",
                home_directory="/home/jsmith",
            )

            mock_smtp.login.assert_not_called()

    def test_smtp_failure_does_not_raise(self, txt_template: Path) -> None:
        sender = WelcomeEmailSender(_settings(str(txt_template)))

        with patch("waldur_site_agent_ldap.email_sender.smtplib") as mock_smtplib:
            mock_smtplib.SMTP.side_effect = smtplib.SMTPConnectError(421, b"down")

            # Should not raise
            sender.send_welcome_email(
                recipient_email="user@example.com",
                username="jsmith",
                first_name="John",
                last_name="Smith",
                home_directory="/home/jsmith",
            )

    def test_missing_optional_vars_render_empty(self, txt_template: Path) -> None:
        sender = WelcomeEmailSender(_settings(str(txt_template)))

        with patch("waldur_site_agent_ldap.email_sender.smtplib") as mock_smtplib:
            mock_smtp = MagicMock()
            mock_smtplib.SMTP.return_value = mock_smtp

            # vpn_password not passed — template should render without error
            sender.send_welcome_email(
                recipient_email="user@example.com",
                username="jsmith",
                first_name="John",
                last_name="Smith",
                home_directory="/home/jsmith",
            )

            args = mock_smtp.sendmail.call_args
            msg_str = args[0][2]
            assert "VPN password" not in msg_str  # conditional block skipped

    def test_subject_template_rendering(self, txt_template: Path) -> None:
        sender = WelcomeEmailSender(
            _settings(
                str(txt_template),
                subject="Welcome {{ username }} to the cluster",
            )
        )

        with patch("waldur_site_agent_ldap.email_sender.smtplib") as mock_smtplib:
            mock_smtp = MagicMock()
            mock_smtplib.SMTP.return_value = mock_smtp

            sender.send_welcome_email(
                recipient_email="user@example.com",
                username="jsmith",
                first_name="John",
                last_name="Smith",
                home_directory="/home/jsmith",
            )

            args = mock_smtp.sendmail.call_args
            msg_str = args[0][2]
            assert "Welcome jsmith to the cluster" in msg_str

    def test_from_name_in_sender(self, txt_template: Path) -> None:
        sender = WelcomeEmailSender(
            _settings(str(txt_template), from_name="HPC Admin")
        )

        with patch("waldur_site_agent_ldap.email_sender.smtplib") as mock_smtplib:
            mock_smtp = MagicMock()
            mock_smtplib.SMTP.return_value = mock_smtp

            sender.send_welcome_email(
                recipient_email="user@example.com",
                username="jsmith",
                first_name="John",
                last_name="Smith",
                home_directory="/home/jsmith",
            )

            args = mock_smtp.sendmail.call_args
            msg_str = args[0][2]
            assert "HPC Admin <noreply@test.local>" in msg_str
