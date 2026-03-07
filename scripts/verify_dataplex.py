"""Quick verification: search Dataplex for the seeded synthetic assets."""
from google.cloud import dataplex_v1
from dotenv import load_dotenv
import os

load_dotenv()
project = os.environ["GOOGLE_CLOUD_PROJECT"]
client = dataplex_v1.CatalogServiceClient()

req = dataplex_v1.SearchEntriesRequest(
    name=f"projects/{project}/locations/global",
    query="sales OR user OR marketing OR transaction OR campaign",
    page_size=20,
    semantic_search=False,
)
resp = client.search_entries(request=req)
print(f"Total matching entries: {resp.total_size}")
for r in resp.results:
    e = r.dataplex_entry
    entry_short = e.name.split("/entries/")[-1] if "/entries/" in e.name else e.name
    print(f"  [{e.entry_type}]  {entry_short}")
