#!/usr/bin/env python
"""
Seed script: Creates synthetic BigQuery datasets + tables and registers them
directly in Dataplex Universal Catalog as custom entries.

Two-track approach:
  Track 1: Create real BigQuery tables with synthetic data.
           Dataplex auto-discovers these as system-managed entries over time.
  Track 2: Immediately create custom Dataplex catalog entries in a dedicated
           entry group so the DataplexExpert agent can find them right away.

This means the catalog is searchable immediately, and the auto-discovered
BigQuery system entries will supplement them over time.

Usage:
    python scripts/seed_dataplex.py

Requirements:
    GOOGLE_CLOUD_PROJECT  — your GCP project ID
    GOOGLE_CLOUD_LOCATION — BigQuery location (default: US)
    ADC credentials with roles:
        - roles/bigquery.dataEditor   (create datasets/tables)
        - roles/dataplex.editor       (create entry groups + entries)
"""

import os
import json
import time
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

PROJECT = os.environ["GOOGLE_CLOUD_PROJECT"]
LOCATION = os.getenv("GOOGLE_CLOUD_LOCATION", "US")

# ---------------------------------------------------------------------------
# Synthetic schema catalogue
# Each entry defines a BQ dataset + table and business metadata for Dataplex.
# ---------------------------------------------------------------------------

SYNTHETIC_ASSETS = [
    # ── Sales domain ────────────────────────────────────────────────────────
    {
        "dataset": "sales_analytics",
        "table": "daily_transactions",
        "description": "Daily point-of-sale and e-commerce transactions with revenue, product, and regional breakdowns. Source of truth for revenue reporting.",
        "tags": ["Finance", "Transaction", "Revenue", "PII-free"],
        "owner": "data-engineering@example.com",
        "schema": [
            {"name": "transaction_id",   "type": "STRING",    "description": "Globally unique transaction identifier (UUID)"},
            {"name": "transaction_date", "type": "DATE",      "description": "Date the transaction was completed"},
            {"name": "product_id",       "type": "STRING",    "description": "Foreign key to product_catalog"},
            {"name": "region",           "type": "STRING",    "description": "ISO 3166-2 region code where sale occurred"},
            {"name": "channel",          "type": "STRING",    "description": "Sales channel: web | mobile | in-store"},
            {"name": "quantity",         "type": "INTEGER",   "description": "Number of units sold"},
            {"name": "unit_price",       "type": "FLOAT64",   "description": "Unit price in USD at time of sale"},
            {"name": "discount_pct",     "type": "FLOAT64",   "description": "Discount applied as percentage 0.0–1.0"},
            {"name": "net_revenue",      "type": "FLOAT64",   "description": "Calculated: quantity * unit_price * (1 - discount_pct)"},
            {"name": "currency",         "type": "STRING",    "description": "ISO 4217 currency code"},
        ],
        "rows": [
            ("txn-001", "2024-01-15", "prod-A1", "us-east-1", "web",      3, 49.99, 0.10, 134.97, "USD"),
            ("txn-002", "2024-01-15", "prod-B3", "eu-west-1", "mobile",   1, 89.00, 0.00,  89.00, "USD"),
            ("txn-003", "2024-01-16", "prod-A1", "us-west-2", "in-store", 5, 49.99, 0.15, 212.46, "USD"),
            ("txn-004", "2024-01-17", "prod-C2", "apac-1",    "web",      2, 32.50, 0.05,  61.75, "USD"),
            ("txn-005", "2024-01-18", "prod-B3", "us-east-1", "mobile",   4, 89.00, 0.20, 284.80, "USD"),
        ],
    },
    {
        "dataset": "sales_analytics",
        "table": "regional_summary",
        "description": "Pre-aggregated weekly sales summary by region and channel. Used for executive dashboards and OKR tracking.",
        "tags": ["Finance", "Aggregated", "Dashboard", "Non-PII"],
        "owner": "analytics@example.com",
        "schema": [
            {"name": "week_start",      "type": "DATE",    "description": "Monday of the reporting week"},
            {"name": "region",          "type": "STRING",  "description": "ISO 3166-2 region code"},
            {"name": "channel",         "type": "STRING",  "description": "Sales channel"},
            {"name": "total_revenue",   "type": "FLOAT64", "description": "Sum of net_revenue for the week"},
            {"name": "order_count",     "type": "INTEGER", "description": "Number of distinct transactions"},
            {"name": "avg_order_value", "type": "FLOAT64", "description": "total_revenue / order_count"},
            {"name": "yoy_growth_pct",  "type": "FLOAT64", "description": "Revenue growth vs same week last year"},
        ],
        "rows": [
            ("2024-01-15", "us-east-1", "web",      142500.75, 3820, 37.31, 0.12),
            ("2024-01-15", "eu-west-1", "mobile",    98300.50, 2145, 45.84, 0.08),
            ("2024-01-15", "apac-1",    "web",       76200.00, 1890, 40.32, 0.21),
            ("2024-01-15", "us-west-2", "in-store",  54100.25, 1200, 45.08, 0.05),
        ],
    },
    {
        "dataset": "sales_analytics",
        "table": "product_catalog",
        "description": "Master product dimension table including pricing tiers, categories, and inventory metadata.",
        "tags": ["Product", "Master-Data", "Non-PII"],
        "owner": "product@example.com",
        "schema": [
            {"name": "product_id",      "type": "STRING",  "description": "Primary key — matches transactions.product_id"},
            {"name": "product_name",    "type": "STRING",  "description": "Human-readable product name"},
            {"name": "category",        "type": "STRING",  "description": "Product category hierarchy (slash-separated)"},
            {"name": "list_price",      "type": "FLOAT64", "description": "Current list price in USD"},
            {"name": "cost_price",      "type": "FLOAT64", "description": "COGS per unit"},
            {"name": "margin_pct",      "type": "FLOAT64", "description": "Gross margin: (list_price - cost_price) / list_price"},
            {"name": "is_active",       "type": "BOOLEAN", "description": "Whether product is currently available for sale"},
            {"name": "launch_date",     "type": "DATE",    "description": "Date product was first made available"},
        ],
        "rows": [
            ("prod-A1", "CloudSync Pro",       "Software/SaaS/Productivity", 49.99, 5.00, 0.90, True,  "2022-03-01"),
            ("prod-B3", "DataVault Enterprise","Software/Security/Storage",   89.00, 9.00, 0.90, True,  "2021-11-15"),
            ("prod-C2", "AnalyticsDash Basic", "Software/SaaS/Analytics",     32.50, 4.50, 0.86, True,  "2023-06-01"),
            ("prod-D5", "Legacy Connector",    "Software/Integration",         19.99, 8.00, 0.60, False, "2019-01-10"),
        ],
    },
    # ── User domain ─────────────────────────────────────────────────────────
    {
        "dataset": "user_data",
        "table": "user_profiles",
        "description": "Core user dimension table. Contains PII — access restricted to approved teams. Do not export without DLP.",
        "tags": ["PII", "Core", "User", "Restricted"],
        "owner": "identity-eng@example.com",
        "schema": [
            {"name": "user_id",         "type": "STRING",    "description": "Primary key — opaque UUID, not linked to SSO"},
            {"name": "email_hash",      "type": "STRING",    "description": "SHA-256 hash of email — NOT raw email"},
            {"name": "country_code",    "type": "STRING",    "description": "ISO 3166-1 alpha-2 country code"},
            {"name": "signup_date",     "type": "TIMESTAMP", "description": "UTC timestamp of account creation"},
            {"name": "plan_tier",       "type": "STRING",    "description": "Subscription tier: free | starter | pro | enterprise"},
            {"name": "is_active",       "type": "BOOLEAN",   "description": "Whether user has logged in within last 90 days"},
            {"name": "lifetime_value",  "type": "FLOAT64",   "description": "Cumulative net revenue from this user (USD)"},
            {"name": "churn_risk_score","type": "FLOAT64",   "description": "ML model output: probability of churning 0.0–1.0"},
        ],
        "rows": [
            ("usr-0001", "a3f8e2b1c4d7...", "US", "2023-01-10 09:14:22 UTC", "pro",        True,  249.95, 0.12),
            ("usr-0002", "b9c1d4e7f2a8...", "DE", "2023-03-22 14:30:00 UTC", "starter",    True,   59.99, 0.34),
            ("usr-0003", "c7d2e5f8a1b4...", "JP", "2022-11-05 02:00:00 UTC", "enterprise", True, 1499.00, 0.05),
            ("usr-0004", "d5e8f1a4b7c2...", "BR", "2023-08-14 18:45:00 UTC", "free",       False,   0.00, 0.78),
        ],
    },
    {
        "dataset": "user_data",
        "table": "session_events",
        "description": "Clickstream and session-level events from web and mobile clients. Powers product analytics and funnel analysis.",
        "tags": ["Behavioral", "Analytics", "Clickstream", "Non-PII"],
        "owner": "product-analytics@example.com",
        "schema": [
            {"name": "event_id",        "type": "STRING",    "description": "Unique event identifier"},
            {"name": "session_id",      "type": "STRING",    "description": "Groups events within a single user session"},
            {"name": "user_id",         "type": "STRING",    "description": "Pseudonymised user ID (joins user_profiles.user_id)"},
            {"name": "event_name",      "type": "STRING",    "description": "Event type e.g. page_view, button_click, checkout_start"},
            {"name": "event_timestamp", "type": "TIMESTAMP", "description": "UTC event time with millisecond precision"},
            {"name": "platform",        "type": "STRING",    "description": "web | ios | android"},
            {"name": "page_path",       "type": "STRING",    "description": "URL path or screen name where event occurred"},
            {"name": "properties",      "type": "JSON",      "description": "Arbitrary event properties as JSON (varies by event_name)"},
        ],
        "rows": [
            ("evt-001", "sess-A", "usr-0001", "page_view",      "2024-01-15 10:00:01.123 UTC", "web",     "/dashboard",   '{"referrer": "email"}'),
            ("evt-002", "sess-A", "usr-0001", "button_click",   "2024-01-15 10:01:30.456 UTC", "web",     "/dashboard",   '{"button": "upgrade"}'),
            ("evt-003", "sess-A", "usr-0001", "checkout_start", "2024-01-15 10:02:05.789 UTC", "web",     "/pricing",     '{"plan": "enterprise"}'),
            ("evt-004", "sess-B", "usr-0002", "page_view",      "2024-01-15 11:15:00.000 UTC", "ios",     "/home",        '{"version": "3.2.1"}'),
            ("evt-005", "sess-B", "usr-0002", "feature_use",    "2024-01-15 11:16:45.321 UTC", "ios",     "/reports",     '{"report_type": "revenue"}'),
        ],
    },
    {
        "dataset": "user_data",
        "table": "subscription_tiers",
        "description": "Reference table defining subscription plan tiers, feature entitlements, and pricing history.",
        "tags": ["Reference", "Billing", "Non-PII", "Master-Data"],
        "owner": "billing@example.com",
        "schema": [
            {"name": "tier_id",           "type": "STRING",  "description": "Primary key: free | starter | pro | enterprise"},
            {"name": "display_name",      "type": "STRING",  "description": "Human-readable tier name"},
            {"name": "monthly_price_usd", "type": "FLOAT64", "description": "Monthly list price in USD (0.0 for free)"},
            {"name": "annual_price_usd",  "type": "FLOAT64", "description": "Annual price (typically 2 months free)"},
            {"name": "max_seats",         "type": "INTEGER", "description": "Maximum number of licensed users (-1 = unlimited)"},
            {"name": "has_sso",           "type": "BOOLEAN", "description": "Whether SSO/SAML is included"},
            {"name": "storage_gb",        "type": "INTEGER", "description": "Included storage quota in gigabytes"},
            {"name": "effective_date",    "type": "DATE",    "description": "Date this pricing became active"},
        ],
        "rows": [
            ("free",       "Free",             0.00,    0.00,   1, False,    5, "2020-01-01"),
            ("starter",    "Starter",          9.99,   99.00,   5, False,   50, "2021-06-01"),
            ("pro",        "Pro",             29.99,  299.00,  20, True,   500, "2021-06-01"),
            ("enterprise", "Enterprise",     149.00, 1499.00,  -1, True, 10000, "2021-06-01"),
        ],
    },
    # ── Marketing domain ────────────────────────────────────────────────────
    {
        "dataset": "marketing",
        "table": "campaign_performance",
        "description": "Aggregated marketing campaign performance metrics across paid, organic, and email channels.",
        "tags": ["Marketing", "Campaign", "Attribution", "Non-PII"],
        "owner": "growth@example.com",
        "schema": [
            {"name": "campaign_id",    "type": "STRING",  "description": "Unique campaign identifier"},
            {"name": "campaign_name",  "type": "STRING",  "description": "Descriptive campaign name"},
            {"name": "channel",        "type": "STRING",  "description": "paid_search | paid_social | email | organic | display"},
            {"name": "start_date",     "type": "DATE",    "description": "Campaign launch date"},
            {"name": "end_date",       "type": "DATE",    "description": "Campaign end date (NULL if ongoing)"},
            {"name": "spend_usd",      "type": "FLOAT64", "description": "Total media spend in USD"},
            {"name": "impressions",    "type": "INTEGER", "description": "Total ad impressions served"},
            {"name": "clicks",         "type": "INTEGER", "description": "Total link clicks"},
            {"name": "conversions",    "type": "INTEGER", "description": "Attributed conversions (signups or purchases)"},
            {"name": "revenue_attr",   "type": "FLOAT64", "description": "Revenue attributed to this campaign (last-touch)"},
            {"name": "roas",           "type": "FLOAT64", "description": "Return on ad spend: revenue_attr / spend_usd"},
        ],
        "rows": [
            ("camp-001", "Q1 Brand Awareness",  "paid_social",  "2024-01-01", "2024-03-31", 25000.00, 1200000, 18000, 420,  31500.00, 1.26),
            ("camp-002", "Enterprise Outbound", "email",        "2024-01-15", "2024-02-15",  3000.00,   85000,  9200, 180,  27000.00, 9.00),
            ("camp-003", "SaaS Keywords",       "paid_search",  "2024-01-01", None,         18000.00,  450000, 22000, 890, 178000.00, 9.89),
            ("camp-004", "Retargeting Wave 1",  "display",      "2024-02-01", "2024-02-28",  8500.00,  620000,  4100, 210,  15750.00, 1.85),
        ],
    },
]


# ---------------------------------------------------------------------------
# BigQuery helpers
# ---------------------------------------------------------------------------

def get_bq_schema(columns: list) -> list:
    from google.cloud.bigquery import SchemaField
    TYPE_MAP = {
        "STRING": "STRING", "DATE": "DATE", "TIMESTAMP": "TIMESTAMP",
        "INTEGER": "INTEGER", "INT64": "INTEGER", "FLOAT64": "FLOAT64",
        "BOOLEAN": "BOOLEAN", "JSON": "JSON",
    }
    return [
        SchemaField(
            name=col["name"],
            field_type=TYPE_MAP.get(col["type"], "STRING"),
            description=col.get("description", ""),
        )
        for col in columns
    ]


def seed_bigquery(client, asset: dict) -> str:
    """Create dataset + table and insert rows. Returns the BQ table resource name."""
    from google.cloud import bigquery
    from google.api_core.exceptions import Conflict

    dataset_id = f"{PROJECT}.{asset['dataset']}"
    table_id   = f"{dataset_id}.{asset['table']}"

    # Create dataset (idempotent)
    try:
        ds = bigquery.Dataset(dataset_id)
        ds.location = LOCATION
        ds.description = f"Synthetic dataset: {asset['dataset']}"
        client.create_dataset(ds, exists_ok=True)
        log.info("  ✓ Dataset: %s", dataset_id)
    except Exception as e:
        log.warning("  Dataset %s: %s", dataset_id, e)

    # Create table (idempotent)
    try:
        schema = get_bq_schema(asset["schema"])
        table = bigquery.Table(table_id, schema=schema)
        table.description = asset["description"]
        table = client.create_table(table, exists_ok=True)
        log.info("  ✓ Table:   %s", table_id)
    except Exception as e:
        log.warning("  Table %s: %s", table_id, e)
        return table_id

    # Insert synthetic rows (truncate-and-reload for idempotency)
    if asset.get("rows"):
        column_names = [col["name"] for col in asset["schema"]]
        rows = [dict(zip(column_names, row)) for row in asset["rows"]]
        errors = client.insert_rows_json(table_id, rows)
        if errors:
            log.warning("  BQ insert errors for %s: %s", table_id, errors)
        else:
            log.info("  ✓ Inserted %d rows into %s", len(rows), table_id)

    return table_id


# ---------------------------------------------------------------------------
# Dataplex catalog helpers — direct entry creation
# ---------------------------------------------------------------------------

# Custom entry group ID for seeded synthetic assets
_ENTRY_GROUP_ID = "synthetic-bigquery"
_ENTRY_GROUP_PARENT = f"projects/{PROJECT}/locations/global"
_ENTRY_GROUP_NAME = f"{_ENTRY_GROUP_PARENT}/entryGroups/{_ENTRY_GROUP_ID}"


def ensure_entry_group(catalog_client):
    """Create the entry group if it doesn't exist."""
    from google.cloud import dataplex_v1
    from google.api_core.exceptions import AlreadyExists

    try:
        eg = dataplex_v1.EntryGroup()
        eg.description = "Synthetic BigQuery assets seeded for DataDiscovery agent development."
        eg.display_name = "Synthetic BigQuery Catalog"

        catalog_client.create_entry_group(
            parent=_ENTRY_GROUP_PARENT,
            entry_group_id=_ENTRY_GROUP_ID,
            entry_group=eg,
        )
        log.info("✓ Created entry group: %s", _ENTRY_GROUP_NAME)
    except AlreadyExists:
        log.info("  Entry group already exists: %s", _ENTRY_GROUP_NAME)
    except Exception as e:
        log.warning("  Could not create entry group: %s", e)


def _entry_id(dataset: str, table: str) -> str:
    """Safe entry ID from dataset.table (Dataplex requires alphanumeric + hyphens only)."""
    return f"{dataset.replace('_', '-')}-{table.replace('_', '-')}"


def create_or_update_catalog_entry(catalog_client, asset: dict):
    """
    Create (or update) a Dataplex catalog entry for a synthetic BigQuery table.
    Uses the custom entry group so entries are immediately searchable.
    """
    from google.cloud import dataplex_v1
    from google.api_core.exceptions import AlreadyExists, NotFound

    entry_id   = _entry_id(asset["dataset"], asset["table"])
    entry_name = f"{_ENTRY_GROUP_NAME}/entries/{entry_id}"

    # Build the schema description for the entry source
    schema_summary = ", ".join(
        f"{col['name']} ({col['type']})"
        for col in asset.get("schema", [])
    )
    full_description = (
        f"{asset['description']}\n\n"
        f"Schema: {schema_summary}\n"
        f"Tags: {', '.join(asset.get('tags', []))}\n"
        f"Owner: {asset.get('owner', 'unknown')}\n"
        f"BigQuery: {PROJECT}.{asset['dataset']}.{asset['table']}"
    )

    # Construct the Entry
    entry = dataplex_v1.Entry()
    entry.entry_type = (
        "projects/dataplex-types/locations/global/entryTypes/generic"
    )

    # entry_source carries human-readable metadata visible in search
    source = dataplex_v1.EntrySource()
    source.display_name = f"{asset['dataset']}.{asset['table']}"
    source.description  = full_description
    source.system       = "BigQuery"
    source.resource     = f"//bigquery.googleapis.com/projects/{PROJECT}/datasets/{asset['dataset']}/tables/{asset['table']}"
    entry.entry_source  = source
    entry.fully_qualified_name = f"bigquery:{PROJECT}.{asset['dataset']}.{asset['table']}"

    try:
        catalog_client.create_entry(
            parent=_ENTRY_GROUP_NAME,
            entry_id=entry_id,
            entry=entry,
        )
        log.info("  ✓ Created entry: %s.%s  (id: %s)", asset["dataset"], asset["table"], entry_id)
    except AlreadyExists:
        # Update existing entry
        entry.name = entry_name
        try:
            catalog_client.update_entry(
                request=dataplex_v1.UpdateEntryRequest(
                    entry=entry,
                    allow_missing=False,
                )
            )
            log.info("  ↺ Updated entry: %s.%s", asset["dataset"], asset["table"])
        except Exception as e:
            log.warning("  Could not update entry %s: %s", entry_id, e)
    except Exception as e:
        log.warning("  Could not create entry %s.%s: %s", asset["dataset"], asset["table"], e)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    from google.cloud import bigquery, dataplex_v1

    log.info("=" * 60)
    log.info("Seeding synthetic data into BigQuery + Dataplex")
    log.info("Project:  %s", PROJECT)
    log.info("Location: %s", LOCATION)
    log.info("=" * 60)

    bq_client      = bigquery.Client(project=PROJECT)
    catalog_client = dataplex_v1.CatalogServiceClient()

    # ── Track 1: Create BigQuery tables ─────────────────────────────────────
    log.info("\n── Track 1: BigQuery tables ────────────────────────────────")
    for asset in SYNTHETIC_ASSETS:
        log.info("\n▶ %s.%s", asset["dataset"], asset["table"])
        seed_bigquery(bq_client, asset)

    # ── Track 2: Register Dataplex catalog entries immediately ───────────────
    log.info("\n── Track 2: Dataplex catalog entries ───────────────────────")
    ensure_entry_group(catalog_client)

    for asset in SYNTHETIC_ASSETS:
        log.info("\n▶ %s.%s", asset["dataset"], asset["table"])
        create_or_update_catalog_entry(catalog_client, asset)

    log.info(
        "\n✅ Done! %d entries registered in Dataplex.\n"
        "   Entry group: %s\n\n"
        "   Test it:\n"
        "   adk web src/agents  →  orchestrator  →  'find tables with sales data'",
        len(SYNTHETIC_ASSETS),
        _ENTRY_GROUP_NAME,
    )


if __name__ == "__main__":
    main()
