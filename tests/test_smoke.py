from signaldesk_email_worker.settings import Settings


def test_package_imports() -> None:
    assert Settings.service_name == "signaldesk-email-worker"
