from .base import BaseConnector

# Lazy Registry Mapping: ID -> (Module Path, Class Name)
LAZY_REGISTRY = {
    "csv": ("ml_engine.connectors.file.csv_connector", "CSVConnector"),
    "excel": ("ml_engine.connectors.file.excel_connector", "ExcelConnector"),
    "json_api": ("ml_engine.connectors.file.json_connector", "JSONConnector"),
    "postgresql": ("ml_engine.connectors.database.postgresql_connector", "PostgreSQLConnector"),
    "mysql": ("ml_engine.connectors.database.mysql_connector", "MySQLConnector"),
    "google_sheets": ("ml_engine.connectors.saas.google_sheets_connector", "GoogleSheetsConnector"),
    "airtable": ("ml_engine.connectors.saas.airtable_connector", "AirtableConnector"),
    "notion": ("ml_engine.connectors.saas.notion_connector", "NotionConnector"),
    "hubspot": ("ml_engine.connectors.analytics.hubspot_connector", "HubSpotConnector"),
    "google_analytics": ("ml_engine.connectors.analytics.google_analytics_connector", "GoogleAnalyticsConnector"),
}

CONNECTOR_CATALOG = [
    {
        "id": "csv",
        "label": "CSV File",
        "category": "file",
        "tier": "easy",
        "description": "Upload a comma-separated values file",
        "setup_guide": {
            "steps": [
                "Ensure your CSV file has a header row with unique column names.",
                "The file should use commas (,) as delimiters.",
                "Large files (up to 10k rows) are supported for the preview."
            ]
        }
    },
    {
        "id": "excel",
        "label": "Excel",
        "category": "file",
        "tier": "easy",
        "description": "Upload an .xlsx or .xls spreadsheet",
        "setup_guide": {
            "steps": [
                "Your Excel file can contain multiple sheets; you will select one in the next step.",
                "Ensure the first row of your data contains column headers.",
                "Supported formats: .xlsx and .xls."
            ]
        }
    },
    {
        "id": "json_api",
        "label": "JSON / REST API",
        "category": "file",
        "tier": "easy",
        "description": "Fetch data from any public JSON REST endpoint",
        "setup_guide": {
            "steps": [
                "The URL must be a public GET endpoint (no authentication required currently).",
                "If the data is nested (e.g. inside a 'results' key), specify the path in the JSON Path field.",
                "The final target must be an array of objects."
            ]
        }
    },
    {
        "id": "postgresql",
        "label": "PostgreSQL",
        "category": "database",
        "tier": "easy",
        "description": "Connect to a PostgreSQL database",
        "setup_guide": {
            "steps": [
                "Find your database host address (IP or domain).",
                "Ensure your database firewall allows incoming connections from our engine.",
                "Default PostgreSQL port is 5432.",
                "In the next step, you will be able to pick any table from your schemas."
            ]
        }
    },
    {
        "id": "mysql",
        "label": "MySQL",
        "category": "database",
        "tier": "easy",
        "description": "Connect to a MySQL or MariaDB database",
        "setup_guide": {
            "steps": [
                "Find your MySQL server host and credentials.",
                "Check that the user has SELECT permissions for the target database.",
                "Default MySQL port is 3306.",
                "The platform will list all base tables in the database for you to choose from."
            ]
        }
    },
    {
        "id": "google_sheets",
        "label": "Google Sheets",
        "category": "saas",
        "tier": "medium",
        "description": "Import data from a Google Sheets spreadsheet via Service Account",
        "setup_guide": {
            "steps": [
                "Go to Google Cloud Console and create a project.",
                "Enable the Google Sheets API.",
                "Create a Service Account and download the JSON key file.",
                "Share your spreadsheet with the service account email (found in the JSON).",
                "Copy the Spreadsheet ID from the browser URL."
            ],
            "docs": "https://cloud.google.com/iam/docs/service-accounts-create"
        }
    },
    {
        "id": "airtable",
        "label": "Airtable",
        "category": "saas",
        "tier": "medium",
        "description": "Pull records from any table in an Airtable base",
        "setup_guide": {
            "steps": [
                "Visit your Airtable Account settings.",
                "Create a 'Personal Access Token' with data.records:read scope.",
                "Copy the Base ID from the Airtable API documentation page for your base."
            ],
            "docs": "https://support.airtable.com/docs/creating-and-using-personal-access-tokens"
        }
    },
    {
        "id": "notion",
        "label": "Notion",
        "category": "saas",
        "tier": "medium",
        "description": "Import rows from a Notion database via integration token",
        "setup_guide": {
            "steps": [
                "Go to notion.so/my-integrations and create a New Integration.",
                "Copy the 'Internal Integration Token'.",
                "Open your Notion database, click '...' -> 'Add connections' and select your integration.",
                "Copy the Database ID from the URL."
            ],
            "docs": "https://developers.notion.com/docs/getting-started"
        }
    },
    {
        "id": "hubspot",
        "label": "HubSpot",
        "category": "analytics",
        "tier": "hard",
        "description": "Import CRM objects from HubSpot",
        "setup_guide": {
            "steps": [
                "In HubSpot, go to Settings -> Integrations -> Private Apps.",
                "Create a new Private App.",
                "Select the 'crm.objects.contacts.read' scope.",
                "Copy the Access Token."
            ],
            "docs": "https://developers.hubspot.com/docs/api/private-apps"
        }
    },
    {
        "id": "google_analytics",
        "label": "Google Analytics 4",
        "category": "analytics",
        "tier": "hard",
        "description": "Pull reports from a GA4 property via Service Account",
        "setup_guide": {
            "steps": [
                "Create a Google Cloud Service Account.",
                "Go to Google Analytics Admin -> Property Access Management.",
                "Add the Service Account email with 'Viewer' role.",
                "Enable the Google Analytics Data API in Cloud Console.",
                "Copy the Property ID from GA4 Property Settings."
            ],
            "docs": "https://developers.google.com/analytics/devguides/reporting/data/v1"
        }
    },
]

_LOADED_CONNECTORS: dict[str, type[BaseConnector]] = {}

def _get_connector_class(name: str) -> type[BaseConnector]:
    if name in _LOADED_CONNECTORS:
        return _LOADED_CONNECTORS[name]
    
    if name not in LAZY_REGISTRY:
        raise ValueError(f"Unknown connector: '{name}'")
    
    module_path, class_name = LAZY_REGISTRY[name]
    import importlib
    mod = importlib.import_module(module_path)
    cls = getattr(mod, class_name)
    _LOADED_CONNECTORS[name] = cls
    return cls


def get_connector(name: str) -> BaseConnector:
    cls = _get_connector_class(name)
    return cls()


def get_catalog() -> list[dict]:
    catalog = []
    for entry in CONNECTOR_CATALOG:
        c_id = entry["id"]
        item = dict(entry)
        if c_id in LAZY_REGISTRY:
            cls = _get_connector_class(c_id)
            item["config_schema"] = cls.get_config_schema()
        catalog.append(item)
    return catalog
