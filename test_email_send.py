"""
Comprehensive mocked email send tests.
Tests the scheduler's _deliver_email and related functions with mock SMTP.
Verifies:
  1. SMTP connection (host, port, TLS, auth)
  2. Message construction (From, To, Subject, HTML)
  3. Error handling (missing config, SMTP failures)
"""

import os
import sys
import unittest
from unittest.mock import Mock, patch, MagicMock, call
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Mock environment before importing scheduler
os.environ.setdefault("SMTP_HOST", "smtp-mail.outlook.com")
os.environ.setdefault("SMTP_PORT", "587")
os.environ.setdefault("SMTP_USERNAME", "ai-datavalidator@outlook.com")
os.environ.setdefault("SMTP_PASSWORD", "test-password")
os.environ.setdefault("EMAIL_FROM", "\"AI-Datavalidator\" <ai-datavalidator@outlook.com>")

from workspace.scheduler import _deliver_email, _html_report, _send_rich_email_report


class TestEmailSend(unittest.TestCase):
    """Test suite for scheduler email delivery."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_to_email = "ravindra.upadhyay1001@gmail.com"
        self.test_from_email = '"AI-Datavalidator" <ai-datavalidator@outlook.com>'
        self.test_subject = "[Data Validation] test job — compare"
        self.test_html = "<h2>Test Report</h2><p>All systems operational.</p>"

    @patch.dict(os.environ, {
        "SMTP_HOST": "smtp-mail.outlook.com",
        "SMTP_PORT": "587",
        "SMTP_USERNAME": "ai-datavalidator@outlook.com",
        "SMTP_PASSWORD": "test-password",
    })
    @patch("smtplib.SMTP")
    def test_smtp_587_with_starttls_and_auth(self, mock_smtp_class):
        """
        Test SMTP connection on port 587 with STARTTLS and authentication.
        This is the standard setup for Outlook and most modern mail providers.
        """
        mock_smtp = MagicMock()
        mock_smtp_class.return_value.__enter__.return_value = mock_smtp

        # Call _deliver_email
        _deliver_email(self.test_to_email, self.test_from_email, 
                      self.test_subject, self.test_html)

        # Verify SMTP connection
        mock_smtp_class.assert_called_once()
        call_args = mock_smtp_class.call_args
        assert call_args[0][0] == "smtp-mail.outlook.com", "SMTP host mismatch"
        assert call_args[0][1] == 587, "SMTP port should be 587"
        assert call_args[1].get("timeout") == 20, "SMTP timeout should be 20s"

        # Verify STARTTLS was called
        mock_smtp.starttls.assert_called_once()

        # Verify login was called with correct credentials
        mock_smtp.login.assert_called_once_with(
            "ai-datavalidator@outlook.com",
            "test-password"
        )

        # Verify send_message was called
        mock_smtp.send_message.assert_called_once()

    @patch.dict(os.environ, {
        "SMTP_HOST": "smtp.gmail.com",
        "SMTP_PORT": "465",
        "SMTP_USERNAME": "user@gmail.com",
        "SMTP_PASSWORD": "app-password",
    })
    @patch("smtplib.SMTP_SSL")
    def test_smtp_ssl_port_465(self, mock_smtp_ssl_class):
        """
        Test SMTP_SSL connection on port 465 (implicit TLS).
        Some providers (older Outlook configs, etc.) use this.
        """
        mock_smtp = MagicMock()
        mock_smtp_ssl_class.return_value.__enter__.return_value = mock_smtp

        _deliver_email("user@example.com", "noreply@example.com",
                      "Test", "<p>Test</p>")

        # Verify SMTP_SSL was called instead of SMTP
        mock_smtp_ssl_class.assert_called_once()
        call_args = mock_smtp_ssl_class.call_args
        assert call_args[0][1] == 465, "Port 465 requires SMTP_SSL"

        # Verify login and send
        mock_smtp.login.assert_called_once_with("user@gmail.com", "app-password")
        mock_smtp.send_message.assert_called_once()

        # STARTTLS should NOT be called with SMTP_SSL
        mock_smtp.starttls.assert_not_called()

    @patch.dict(os.environ, {
        "SMTP_HOST": "",  # Empty host
        "SMTP_PORT": "587",
        "SMTP_USERNAME": "user@example.com",
        "SMTP_PASSWORD": "password",
    })
    def test_missing_smtp_host_raises_error(self):
        """
        Test that missing SMTP_HOST raises a clear, actionable error.
        Previously this would fail silently with a connection error.
        """
        with self.assertRaises(RuntimeError) as ctx:
            _deliver_email(self.test_to_email, self.test_from_email,
                          self.test_subject, self.test_html)
        
        error_msg = str(ctx.exception)
        assert "SMTP_HOST is not configured" in error_msg, "Error message should mention SMTP_HOST"
        assert "SMTP_PORT" in error_msg, "Error message should mention SMTP_PORT"
        assert "SMTP_USERNAME" in error_msg or "SMTP_USER" in error_msg, "Error message should mention credentials"
        assert "SMTP_PASSWORD" in error_msg, "Error message should mention password"

    @patch.dict(os.environ, {
        "SMTP_HOST": "smtp-mail.outlook.com",
        "SMTP_PORT": "587",
        "SMTP_USERNAME": "ai-datavalidator@outlook.com",
        "SMTP_PASSWORD": "test-password",
    })
    @patch("smtplib.SMTP")
    def test_message_construction(self, mock_smtp_class):
        """
        Test that the email message is correctly constructed with all headers.
        """
        mock_smtp = MagicMock()
        mock_smtp_class.return_value.__enter__.return_value = mock_smtp

        _deliver_email(self.test_to_email, self.test_from_email,
                      self.test_subject, self.test_html)

        # Extract the message that was sent
        send_message_call = mock_smtp.send_message.call_args
        msg = send_message_call[0][0]

        # Verify message headers
        assert msg["Subject"] == self.test_subject, "Subject header mismatch"
        assert msg["From"] == self.test_from_email, "From header mismatch"
        assert msg["To"] == self.test_to_email, "To header mismatch"

        # Verify message is HTML
        assert msg.is_multipart(), "Message should be multipart"

    @patch.dict(os.environ, {
        "SMTP_HOST": "smtp-mail.outlook.com",
        "SMTP_PORT": "587",
        "SMTP_USERNAME": "ai-datavalidator@outlook.com",
        "SMTP_PASSWORD": "test-password",
        "EMAIL_FROM": '"AI-Datavalidator" <ai-datavalidator@outlook.com>',
    })
    @patch("smtplib.SMTP")
    def test_fallback_from_address_uses_env(self, mock_smtp_class):
        """
        Test that when no from_email is provided, EMAIL_FROM env var is used.
        """
        mock_smtp = MagicMock()
        mock_smtp_class.return_value.__enter__.return_value = mock_smtp

        # Call with from_email=None to trigger fallback
        _deliver_email(self.test_to_email, None, self.test_subject, self.test_html)

        send_message_call = mock_smtp.send_message.call_args
        msg = send_message_call[0][0]

        # Verify EMAIL_FROM was used (the patch.dict ensures it's set)
        expected_from = '"AI-Datavalidator" <ai-datavalidator@outlook.com>'
        assert msg["From"] == expected_from, \
            f"Should fall back to EMAIL_FROM env var. Got: {msg['From']!r}"

    @patch.dict(os.environ, {
        "SMTP_HOST": "smtp-mail.outlook.com",
        "SMTP_PORT": "587",
        "SMTP_USERNAME": "",  # Empty username
        "SMTP_PASSWORD": "password",
    })
    @patch("smtplib.SMTP")
    def test_empty_credentials_skips_login(self, mock_smtp_class):
        """
        Test that if username is empty, login() is not called.
        Some SMTP servers allow unauthenticated sends (rare, but valid).
        """
        mock_smtp = MagicMock()
        mock_smtp_class.return_value.__enter__.return_value = mock_smtp

        _deliver_email(self.test_to_email, self.test_from_email,
                      self.test_subject, self.test_html)

        # Verify login was NOT called
        mock_smtp.login.assert_not_called()

        # But send should still happen
        mock_smtp.send_message.assert_called_once()

    @patch.dict(os.environ, {
        "SMTP_HOST": "smtp-mail.outlook.com",
        "SMTP_PORT": "587",
        "SMTP_USERNAME": "ai-datavalidator@outlook.com",
        "SMTP_PASSWORD": "test-password",
    })
    @patch("smtplib.SMTP")
    def test_ipv4_only_socket_patching(self, mock_smtp_class):
        """
        Test that socket.getaddrinfo is patched to IPv4-only during SMTP connection.
        This is a Railway/container-specific fix for IPv6 unreachability.
        """
        import socket
        
        mock_smtp = MagicMock()
        mock_smtp_class.return_value.__enter__.return_value = mock_smtp

        original_getaddrinfo = socket.getaddrinfo
        patched_during_call = []

        def tracking_socket_patch(*args, **kwargs):
            # Track that IPv4 patching happened
            patched_during_call.append(socket.getaddrinfo)
            return original_getaddrinfo(*args, **kwargs)

        with patch("socket.getaddrinfo", side_effect=tracking_socket_patch):
            _deliver_email(self.test_to_email, self.test_from_email,
                          self.test_subject, self.test_html)

        # After the call, socket.getaddrinfo should be restored
        assert socket.getaddrinfo == original_getaddrinfo, \
            "socket.getaddrinfo should be restored after email send"

    def test_html_report_generation(self):
        """
        Test that _html_report generates valid HTML with correct metrics.
        """
        job = {
            "name": "Daily BFSI Reconciliation",
            "action": "compare",
        }
        result = {
            "counts": {
                "matched": 45000,
                "file1_only": 15,
                "file2_only": 8,
                "modified": 42,
            }
        }
        sla_result = {
            "breached": False,
            "reasons": [],
        }

        html = _html_report(job, result, sla_result)

        # Verify HTML contains expected content
        assert "Daily BFSI Reconciliation" in html, "Job name not in report"
        assert "compare" in html, "Action not in report"
        assert "45000" in html, "Matched count not in report"
        assert "15" in html, "File1-only count not in report"
        assert "8" in html, "File2-only count not in report"
        assert "42" in html, "Modified count not in report"
        assert "<table" in html, "No HTML table in report"

    def test_html_report_with_sla_breach(self):
        """
        Test that _html_report highlights SLA breaches prominently.
        """
        job = {
            "name": "Hourly Position Reconciliation",
            "action": "compare",
        }
        result = {
            "counts": {
                "matched": 2000,
                "file1_only": 150,
                "file2_only": 200,
                "modified": 0,
            }
        }
        sla_result = {
            "breached": True,
            "reasons": [
                "350 breaks exceeds max_breaks (100)",
                "Schema drift detected: 2 changes",
            ],
        }

        html = _html_report(job, result, sla_result)

        # Verify SLA breach banner is present
        assert "SLA BREACH" in html, "SLA breach not highlighted"
        assert "350 breaks exceeds max_breaks" in html, "Breach reason not in report"
        assert "Schema drift detected" in html, "Schema drift not in report"

    @patch.dict(os.environ, {
        "SMTP_HOST": "smtp-mail.outlook.com",
        "SMTP_PORT": "587",
        "SMTP_USERNAME": "ai-datavalidator@outlook.com",
        "SMTP_PASSWORD": "test-password",
        "EMAIL_FROM": '"AI-Datavalidator" <ai-datavalidator@outlook.com>',
    })
    @patch("smtplib.SMTP")
    def test_full_scheduled_job_email_flow(self, mock_smtp_class):
        """
        Integration test: simulate a complete scheduled job email flow.
        """
        mock_smtp = MagicMock()
        mock_smtp_class.return_value.__enter__.return_value = mock_smtp

        job = {
            "name": "Daily Trade Blotter Reconciliation",
            "action": "compare",
            "notify_email": "ravindra.upadhyay1001@gmail.com",
            "from_email": '"AI-Datavalidator" <ai-datavalidator@outlook.com>',
        }
        result = {
            "counts": {
                "matched": 98500,
                "file1_only": 12,
                "file2_only": 8,
                "modified": 5,
            }
        }
        sla_result = {
            "breached": False,
            "reasons": [],
        }

        # Trigger the email send
        _send_rich_email_report(job, result, sla_result)

        # Verify the call chain
        mock_smtp_class.assert_called_once()
        mock_smtp.starttls.assert_called_once()
        mock_smtp.login.assert_called_once()
        mock_smtp.send_message.assert_called_once()

        # Verify message was sent to the right recipient
        msg = mock_smtp.send_message.call_args[0][0]
        assert msg["To"] == "ravindra.upadhyay1001@gmail.com"


if __name__ == "__main__":
    unittest.main(verbosity=2)

