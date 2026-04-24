"""CBOE symbol-reservation scanner + cross-reference layer.

Polls CBOE's issuer-only symbol_status endpoint to track which 1-4 letter
uppercase tickers are available / reserved / active, then joins against
mkt_master_data to turn raw taken/available flags into competitor intel.

Session-cookie authenticated. Cookie lives in config/.env as
CBOE_SESSION_COOKIE and must be rotated manually when it expires.
"""
