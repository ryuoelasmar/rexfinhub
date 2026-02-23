"""Test CSS rules to prevent sticky header / overflow conflicts."""
import re
from pathlib import Path

STYLE_CSS = Path("webapp/static/css/style.css")
MARKET_CSS = Path("webapp/static/css/market.css")


def test_no_sticky_th_in_global_rule():
    """Global th rule must NOT have position:sticky (breaks inside overflow containers)."""
    css = STYLE_CSS.read_text()
    # Find the global `th {` rule (not prefixed by a class/element selector)
    th_blocks = re.findall(r'(?:^|\n)\s*th\s*\{([^}]+)\}', css)
    for block in th_blocks:
        assert 'position: sticky' not in block and 'position:sticky' not in block, \
            "Global th rule should not have position:sticky"


def test_data_table_th_no_sticky():
    """.data-table th must NOT have position:sticky."""
    css = STYLE_CSS.read_text()
    matches = re.findall(r'\.data-table\s+th\s*\{([^}]+)\}', css)
    for block in matches:
        assert 'position: sticky' not in block and 'position:sticky' not in block, \
            ".data-table th should not have position:sticky"


def test_th_has_background_color():
    """All th rules should specify a background to prevent see-through headers."""
    css = STYLE_CSS.read_text()
    # Check global th
    th_blocks = re.findall(r'(?:^|\n)\s*th\s*\{([^}]+)\}', css)
    for block in th_blocks:
        assert 'background' in block, "th rule must have background color"
    # Check .data-table th
    dt_blocks = re.findall(r'\.data-table\s+th\s*\{([^}]+)\}', css)
    for block in dt_blocks:
        assert 'background' in block, ".data-table th must have background color"
