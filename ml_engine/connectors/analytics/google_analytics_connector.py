from typing import Any
from ..base import BaseConnector

GA4_REPORTS = [
    {"id": "sessions_by_date", "label": "Sessions by Date", "dimensions": ["date"], "metrics": ["sessions", "totalUsers"]},
    {"id": "traffic_by_source", "label": "Traffic by Source / Medium", "dimensions": ["sessionSource", "sessionMedium"], "metrics": ["sessions", "totalUsers"]},
    {"id": "top_pages", "label": "Top Pages", "dimensions": ["pagePath", "pageTitle"], "metrics": ["screenPageViews"]},
    {"id": "conversions_by_event", "label": "Conversions by Event", "dimensions": ["eventName"], "metrics": ["eventCount", "conversions"]},
]

class GoogleAnalyticsConnector(BaseConnector):
    """
    Connects to Google Analytics 4 via a Service Account.
    config keys: { "property_id": str, "service_account_json": dict }
    """

    def _get_client(self, service_account_json: dict):
        try:
            from google.analytics.data_v1beta import BetaAnalyticsDataClient
            from google.oauth2.service_account import Credentials
        except ImportError:
            raise RuntimeError("google-analytics-data and google-auth are not installed")

        creds = Credentials.from_service_account_info(
            service_account_json,
            scopes=["https://www.googleapis.com/auth/analytics.readonly"],
        )
        return BetaAnalyticsDataClient(credentials=creds)

    def validate(self, config: dict[str, Any]) -> dict:
        try:
            client = self._get_client(config["service_account_json"])
            property_id = config["property_id"]
            
            from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Metric
            req = RunReportRequest(
                property=property_id,
                date_ranges=[DateRange(start_date="yesterday", end_date="yesterday")],
                metrics=[Metric(name="sessions")],
            )
            client.run_report(req)

            return {
                "status": "ok",
                "sources": [
                    {"id": r["id"], "label": r["label"], "meta": {"dimensions": r["dimensions"]}}
                    for r in GA4_REPORTS
                ],
                "message": f"Connected to {property_id}. Preset reports available",
            }
        except Exception as e:
            return {"status": "error", "sources": [], "message": str(e)}

    def fetch(self, config: dict[str, Any], source: str):
        import pandas as pd
        from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric

        report_def = next((r for r in GA4_REPORTS if r["id"] == source), None)
        if not report_def:
            raise ValueError(f"Unknown GA4 report: {source}")

        client = self._get_client(config["service_account_json"])
        property_id = config["property_id"]

        req = RunReportRequest(
            property=property_id,
            date_ranges=[DateRange(start_date="90daysAgo", end_date="today")],
            dimensions=[Dimension(name=d) for d in report_def["dimensions"]],
            metrics=[Metric(name=m) for m in report_def["metrics"]],
            limit=10000,
        )
        response = client.run_report(req)

        rows = []
        for row in response.rows:
            record = {}
            for i, dim in enumerate(report_def["dimensions"]):
                record[dim] = row.dimension_values[i].value
            for i, met in enumerate(report_def["metrics"]):
                record[met] = row.metric_values[i].value
            rows.append(record)

        return pd.DataFrame(rows)

    @classmethod
    def get_config_schema(cls) -> dict:
        return {
            "fields": [
                {
                    "name": "property_id", 
                    "type": "text", 
                    "label": "Property ID", 
                    "placeholder": "properties/123456789", 
                    "required": True,
                    "hint": "Found in GA4 Admin → Property Settings → Property ID"
                },
                {
                    "name": "service_account_file", 
                    "type": "file", 
                    "label": "Service Account JSON", 
                    "required": True,
                    "hint": "Service account must have Viewer access to the GA4 property"
                },
            ]
        }
