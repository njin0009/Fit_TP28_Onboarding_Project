"""
test_us2_1.py — US2.1 Acceptance Criteria Tests

Maps directly to LeanKit AC sub-tasks.

Run LOCAL tests (no server needed):
    pytest test_us2_1.py -v -k "local"

Run API tests (server must be running):
    export API_URL=http://localhost:5001
    pytest test_us2_1.py -v -k "api"

Run ALL tests:
    pytest test_us2_1.py -v
"""

import os
import sys
import sqlite3
import json

import pytest

# ─────────────────────────────────────────────────
# Helpers — import app directly for local tests
# ─────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(__file__))

try:
    from app import app as flask_app, DB_PATH
    APP_AVAILABLE = True
except ImportError:
    APP_AVAILABLE = False


# ═══════════════════════════════════════════════════
# LOCAL TESTS — no network, no running server needed
# These test the Flask app and SQLite DB directly
# ═══════════════════════════════════════════════════

class TestUS21Local:

    @pytest.fixture(autouse=True)
    def client(self):
        if not APP_AVAILABLE:
            pytest.skip("app.py not found")
        flask_app.config['TESTING'] = True
        with flask_app.test_client() as c:
            self.client = c

    def test_local_ac1_response_has_two_charts(self):
        """AC: Both chart datasets are returned"""
        r    = self.client.get('/api/historical-impacts')
        data = json.loads(r.data)
        assert r.status_code == 200
        assert 'chart1_skin_cancer' in data
        assert 'chart2_uv_trend'    in data

    def test_local_ac2_chart1_has_correct_structure(self):
        """AC: Chart 1 has labels, data, and label fields"""
        r    = self.client.get('/api/historical-impacts')
        data = json.loads(r.data)
        c1   = data['chart1_skin_cancer']
        assert 'labels' in c1
        assert 'data'   in c1
        assert 'label'  in c1

    def test_local_ac3_chart2_has_correct_structure(self):
        """AC: Chart 2 has labels, data, and label fields"""
        r    = self.client.get('/api/historical-impacts')
        data = json.loads(r.data)
        c2   = data['chart2_uv_trend']
        assert 'labels' in c2
        assert 'data'   in c2
        assert 'label'  in c2

    def test_local_ac4_chart1_data_not_empty(self):
        """AC: Chart 1 contains actual data points"""
        r    = self.client.get('/api/historical-impacts')
        data = json.loads(r.data)
        assert len(data['chart1_skin_cancer']['labels']) > 0
        assert len(data['chart1_skin_cancer']['data'])   > 0

    def test_local_ac5_chart2_data_not_empty(self):
        """AC: Chart 2 contains actual data points"""
        r    = self.client.get('/api/historical-impacts')
        data = json.loads(r.data)
        assert len(data['chart2_uv_trend']['labels']) > 0
        assert len(data['chart2_uv_trend']['data'])   > 0

    def test_local_ac6_chart1_years_in_range(self):
        """AC: Chart 1 covers 1982–2017 (AIHW data range)"""
        r      = self.client.get('/api/historical-impacts')
        data   = json.loads(r.data)
        labels = data['chart1_skin_cancer']['labels']
        assert min(labels) >= 1982
        assert max(labels) <= 2017

    def test_local_ac7_chart2_years_in_range(self):
        """AC: Chart 2 covers 2016–2024 (ARPANSA data range)"""
        r      = self.client.get('/api/historical-impacts')
        data   = json.loads(r.data)
        labels = data['chart2_uv_trend']['labels']
        assert min(labels) >= 2016
        assert max(labels) <= 2025

    def test_local_ac8_uv_values_are_realistic(self):
        """AC: UV index values are within realistic range (0–20)"""
        r    = self.client.get('/api/historical-impacts')
        data = json.loads(r.data)
        for v in data['chart2_uv_trend']['data']:
            assert 0 <= v <= 20, f"UV value {v} out of range"

    def test_local_ac9_incidence_values_are_positive(self):
        """AC: Cancer incidence values are positive numbers"""
        r    = self.client.get('/api/historical-impacts')
        data = json.loads(r.data)
        for v in data['chart1_skin_cancer']['data']:
            assert v > 0, f"Incidence value {v} is not positive"

    def test_local_ac10_cors_preflight_handled(self):
        """AC: CORS preflight OPTIONS request returns 200"""
        r = self.client.options('/api/historical-impacts')
        assert r.status_code == 200

    def test_local_ac11_city_filter_works(self):
        """AC: Optional city filter returns city-specific UV data"""
        r    = self.client.get('/api/historical-impacts?city=Melbourne')
        data = json.loads(r.data)
        assert r.status_code == 200
        assert 'Melbourne' in data['chart2_uv_trend']['label']

    def test_local_ac12_health_endpoint(self):
        """AC: Health endpoint returns ok status"""
        r    = self.client.get('/health')
        data = json.loads(r.data)
        assert r.status_code == 200
        assert data['status'] == 'ok'


# ═══════════════════════════════════════════════════
# API TESTS — requires running server
# Set API_URL env var before running
# ═══════════════════════════════════════════════════

@pytest.mark.skipif(
    not os.environ.get('API_URL'),
    reason="Set API_URL env var to run live API tests"
)
class TestUS21API:

    BASE = os.environ.get('API_URL', 'http://localhost:5001')

    def test_api_ac1_returns_200(self):
        import requests
        r = requests.get(f'{self.BASE}/api/historical-impacts', timeout=10)
        assert r.status_code == 200

    def test_api_ac2_both_charts_present(self):
        import requests
        data = requests.get(f'{self.BASE}/api/historical-impacts', timeout=10).json()
        assert 'chart1_skin_cancer' in data
        assert 'chart2_uv_trend'    in data

    def test_api_ac3_charts_have_real_data(self):
        import requests
        data = requests.get(f'{self.BASE}/api/historical-impacts', timeout=10).json()
        assert len(data['chart1_skin_cancer']['data']) > 0
        assert len(data['chart2_uv_trend']['data'])    > 0

    def test_api_ac4_city_filter_works(self):
        import requests
        r = requests.get(f'{self.BASE}/api/historical-impacts?city=Melbourne', timeout=10)
        assert r.status_code == 200
        assert 'chart2_uv_trend' in r.json()

    def test_api_ac5_cors_header_present(self):
        import requests
        r = requests.get(f'{self.BASE}/api/historical-impacts', timeout=10)
        assert 'Access-Control-Allow-Origin' in r.headers

    def test_api_ac6_health_check(self):
        import requests
        r = requests.get(f'{self.BASE}/health', timeout=10)
        assert r.status_code == 200
        assert r.json()['status'] == 'ok'
