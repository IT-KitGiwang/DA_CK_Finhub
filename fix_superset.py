"""Fix Superset database connection via API with CSRF token"""
import requests
import json

BASE = "http://localhost:8089"

# 1. Login to get JWT token
session = requests.Session()
login_payload = {"username": "admin", "password": "admin", "provider": "db"}
resp = session.post(f"{BASE}/api/v1/security/login", json=login_payload)
print("Login:", resp.status_code)
access_token = resp.json().get("access_token")

# 2. Get CSRF token
headers = {"Authorization": f"Bearer {access_token}"}
resp = session.get(f"{BASE}/api/v1/security/csrf_token/", headers=headers)
print("CSRF:", resp.status_code)
csrf_token = resp.json().get("result")

headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json",
    "X-CSRFToken": csrf_token,
    "Referer": BASE,
}

# 3. List existing database connections
resp = session.get(f"{BASE}/api/v1/database/", headers=headers)
print("Databases:", resp.status_code)
dbs = resp.json()
for db in dbs.get("result", []):
    print(f"  ID={db['id']}, name={db['database_name']}, backend={db.get('backend','?')}")

# 4. Delete ALL old broken connections
for db in dbs.get("result", []):
    did = db["id"]
    r = session.delete(f"{BASE}/api/v1/database/{did}", headers=headers)
    print(f"  Delete ID={did}: {r.status_code} {r.text[:100]}")

# 5. Create correct connection
new_db = {
    "database_name": "Hive Crypto",
    "sqlalchemy_uri": "hive://dack15@master:10000/default?auth=NOSASL",
    "expose_in_sqllab": True,
    "allow_ctas": True,
    "allow_cvas": True,
    "allow_dml": True,
}
resp = session.post(f"{BASE}/api/v1/database/", headers=headers, json=new_db)
print("Create DB:", resp.status_code, resp.text[:500])

# 6. Verify
resp = session.get(f"{BASE}/api/v1/database/", headers=headers)
for db in resp.json().get("result", []):
    print(f"  VERIFIED: ID={db['id']}, name={db['database_name']}")

# 7. Create dataset for crypto_trades table
resp = session.get(f"{BASE}/api/v1/database/", headers=headers)
db_id = resp.json()["result"][0]["id"]
dataset_payload = {
    "database": db_id,
    "schema": "default",
    "table_name": "crypto_trades",
}
resp = session.post(f"{BASE}/api/v1/dataset/", headers=headers, json=dataset_payload)
print("Create Dataset:", resp.status_code, resp.text[:300])
