import unittest
from unittest.mock import patch, MagicMock
import os
import tempfile
from email_manager import EmailManager

class TestEmailManager(unittest.TestCase):
    def setUp(self):
        # Create a temporary environment file for testing
        self.temp_env_file = tempfile.NamedTemporaryFile(mode='w', delete=False)
        self.temp_env_file.write("""EMAIL_ALERTS_ENABLED=true
SENDER_EMAIL=test@example.com
SENDER_PASSWORD=test_password
TO_EMAILS=recipient1@example.com,recipient2@example.com
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587""")
        self.temp_env_file.close()
        
        # Create EmailManager instance with test credentials
        self.email_manager = EmailManager()
        self.email_manager.sender = "test@example.com"
        self.email_manager.password = "test_password"
        self.email_manager.recipients = ["recipient1@example.com", "recipient2@example.com"]
        self.email_manager.smtp_server = "smtp.gmail.com"
        self.email_manager.smtp_port = 587
        self.email_manager.enabled = True
        
        # Set up test data
        self.test_position_data = {
            'symbol': 'AAPL',
            'period': '1m',
            'position_type': 'LONG',
            'action': 'OPEN',
            'signal_details': {
                'price': 150.0,
                'conditions_met': 3,
                'condition_summary': 'Test conditions met',
                'timestamp': '2024-03-10 10:00:00'
            },
            'pnl_info': None,
            'positions': {
                '1m': 'L',
                '5m': 'S',
                '15m': 'L'
            }
        }
        
        self.test_signal_data = {
            'action': 'BUY',
            'symbol': 'AAPL',
            'timeframe': 15,
            'signal_details': {
                'price': 150.0,
                'underlying_price': 150.0,
                'is_option': True,
                'option_symbol': 'AAPL240315C150',
                'option_type': 'CALL',
                'strike_price': 150.0,
                'expiry_date': '2024-03-15',
                'conditions_met': 3,
                'condition_summary': 'Test conditions met',
                'buy_time': '2024-03-10 10:00:00'
            }
        }

    def tearDown(self):
        # Clean up temporary file
        os.unlink(self.temp_env_file.name)

    @patch('smtplib.SMTP')
    def test_send_position_notification(self, mock_smtp):
        # Mock SMTP server
        mock_server = MagicMock()
        mock_smtp.return_value = mock_server
        
        # Test sending position notification
        result = self.email_manager.send_position_notification(**self.test_position_data)
        
        # Verify SMTP was called correctly
        mock_smtp.assert_called_once_with('smtp.gmail.com', 587)
        mock_server.starttls.assert_called_once()
        mock_server.login.assert_called_once_with('test@example.com', 'test_password')
        mock_server.sendmail.assert_called_once()
        mock_server.quit.assert_called_once()
        
        # Verify result
        self.assertTrue(result)

    @patch('smtplib.SMTP')
    def test_send_signal_alert(self, mock_smtp):
        # Mock SMTP server
        mock_server = MagicMock()
        mock_smtp.return_value = mock_server
        
        # Test sending signal alert
        result = self.email_manager.send_signal_alert(**self.test_signal_data)
        
        # Verify SMTP was called correctly
        mock_smtp.assert_called_once_with('smtp.gmail.com', 587)
        mock_server.starttls.assert_called_once()
        mock_server.login.assert_called_once_with('test@example.com', 'test_password')
        mock_server.sendmail.assert_called_once()
        mock_server.quit.assert_called_once()
        
        # Verify result
        self.assertTrue(result)

    @patch('smtplib.SMTP')
    def test_send_bootstrap_summary_email(self, mock_smtp):
        # Mock SMTP server
        mock_server = MagicMock()
        mock_smtp.return_value = mock_server
        
        # Test data
        symbols = ['AAPL', 'MSFT']
        total_processed = 10
        open_positions_count = 2
        transaction_count = 5
        
        # Test sending bootstrap summary
        result = self.email_manager.send_bootstrap_summary_email(
            symbols, total_processed, open_positions_count, transaction_count
        )
        
        # Verify SMTP was called correctly
        mock_smtp.assert_called_once_with('smtp.gmail.com', 587)
        mock_server.starttls.assert_called_once()
        mock_server.login.assert_called_once_with('test@example.com', 'test_password')
        mock_server.sendmail.assert_called_once()
        mock_server.quit.assert_called_once()
        
        # Verify result
        self.assertTrue(result)

    @patch('smtplib.SMTP')
    def test_send_open_positions_email(self, mock_smtp):
        # Mock SMTP server
        mock_server = MagicMock()
        mock_smtp.return_value = mock_server
        
        # Test data
        open_positions = [
            {
                'symbol': 'AAPL',
                'timeframe': '15m',
                'option_type': 'CALL',
                'entry_price': 150.0,
                'current_price': 155.0,
                'entry_time': '2024-03-10 10:00:00',
                'option_symbol': 'AAPL240315C150',
                'strike_price': 150.0,
                'expiry_date': '2024-03-15'
            }
        ]
        
        # Test sending open positions email
        result = self.email_manager.send_open_positions_email(open_positions)
        
        # Verify SMTP was called correctly
        mock_smtp.assert_called_once_with('smtp.gmail.com', 587)
        mock_server.starttls.assert_called_once()
        mock_server.login.assert_called_once_with('test@example.com', 'test_password')
        mock_server.sendmail.assert_called_once()
        mock_server.quit.assert_called_once()
        
        # Verify result
        self.assertTrue(result)

    def test_email_disabled(self):
        # Test when email is disabled
        self.email_manager.enabled = False
        
        # Test all email sending methods
        position_result = self.email_manager.send_position_notification(**self.test_position_data)
        signal_result = self.email_manager.send_signal_alert(**self.test_signal_data)
        bootstrap_result = self.email_manager.send_bootstrap_summary_email(['AAPL'], 1, 1, 1)
        open_positions_result = self.email_manager.send_open_positions_email([])
        
        # Verify all methods return True when disabled
        self.assertTrue(position_result)
        self.assertTrue(signal_result)
        self.assertTrue(bootstrap_result)
        self.assertTrue(open_positions_result)

    @patch('smtplib.SMTP')
    def test_smtp_error_handling(self, mock_smtp):
        # Mock SMTP server to raise an exception
        mock_smtp.side_effect = Exception("SMTP Error")
        
        # Test sending position notification with error
        result = self.email_manager.send_position_notification(**self.test_position_data)
        
        # Verify result is False when error occurs
        self.assertFalse(result)

    def test_missing_credentials(self):
        # Test with missing credentials
        self.email_manager.sender = None
        self.email_manager.password = None
        self.email_manager.recipients = []
        
        # Test sending position notification
        result = self.email_manager.send_position_notification(**self.test_position_data)
        
        # Verify result is False when credentials are missing
        self.assertFalse(result)

if __name__ == '__main__':
    unittest.main() 