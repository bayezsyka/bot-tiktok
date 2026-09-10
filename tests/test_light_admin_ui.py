from pathlib import Path


def test_light_admin_ui_css_variables():
    # Read the CSS file
    css_path = Path("app/static/style.css")
    assert css_path.exists(), "CSS file is missing"

    content = css_path.read_text()

    # Assert light theme variables exist (Sangkolo Design System)
    assert "--background: #fafafa;" in content or "--page-bg: #f7f8fa;" in content
    assert "--card: #ffffff;" in content or "--surface: #ffffff;" in content
    assert "--foreground: #09090b;" in content or "--text: #172033;" in content

    # Assert no dark theme variables
    assert "--bg-base: #0f172a;" not in content
    assert "--bg-panel: #1e293b;" not in content

def test_dashboard_uses_details_tag():
    # Read dashboard HTML
    history_detail_path = Path("app/templates/history_detail.html")
    assert history_detail_path.exists()

    content = history_detail_path.read_text()

    # Assert <details> is used for technical data
    assert "<details" in content
    assert "Data Teknis Lanjutan" in content
    assert "Job UUID" in content
