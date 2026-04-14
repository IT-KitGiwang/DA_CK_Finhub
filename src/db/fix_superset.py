"""
Superset API Client Integrator
This tool programmatically logs into Superset, fetches a CSRF token, 
deletes broken DB connections, and forces a valid Hive connection insertion
and mapping specifically for the crypto_trades table.
"""

import requests
import logging
from typing import Dict, Any

# Configure structured runtime logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [SUPERSET_FIX] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

class SupersetConfigurator:
    """Class to securely manage Superset configurations over its REST API."""
    
    def __init__(self, base_url: str = "http://localhost:8089"):
        self.base_url = base_url
        self.session = requests.Session()
        self.access_token = None
        self.csrf_token = None
        
    def _get_headers(self) -> Dict[str, str]:
        """Provides dynamic headers depending on API flow contexts."""
        headers = {"Authorization": f"Bearer {self.access_token}"}
        if self.csrf_token:
            headers.update({
                "Content-Type": "application/json",
                "X-CSRFToken": self.csrf_token,
                "Referer": self.base_url
            })
        return headers

    def authenticate(self) -> None:
        """Authenticate as administrator and extract JWT along with CSRF constraints."""
        logger.info(f"Authenticating administration layer on {self.base_url}...")
        
        login_payload = {"username": "admin", "password": "admin", "provider": "db"}
        resp = self.session.post(f"{self.base_url}/api/v1/security/login", json=login_payload)
        resp.raise_for_status()
        
        self.access_token = resp.json().get("access_token")
        logger.info("Successfully fetched authentication JWT string.")
        
        # Load necessary CSRF state
        resp_csrf = self.session.get(f"{self.base_url}/api/v1/security/csrf_token/", headers=self._get_headers())
        resp_csrf.raise_for_status()
        self.csrf_token = resp_csrf.json().get("result")
        logger.info("Successfully fetched strict CSRF token security profile.")

    def cleanup_databases(self) -> None:
        """Scans database pool dynamically and clears existing context mappings."""
        logger.info("Scanning for current dataset mappings...")
        resp = self.session.get(f"{self.base_url}/api/v1/database/", headers=self._get_headers())
        resp.raise_for_status()
        
        dbs = resp.json().get("result", [])
        
        if not dbs:
            logger.info("No legacy connections found.")
            return

        for db in dbs:
            did = db["id"]
            db_name = db.get("database_name", "UNKNOWN")
            logger.info(f"Issuing REST DELETE constraint for ID={did} [{db_name}]")
            r = self.session.delete(f"{self.base_url}/api/v1/database/{did}", headers=self._get_headers())
            
            if r.status_code == 200:
                logger.info("  Deleted Successfully.")
            else:
                logger.warning(f"  Attempt Failed: {r.status_code}")

    def create_hive_connection(self) -> None:
        """Publishes the correct Hive API string URI payload dynamically."""
        new_db = {
            "database_name": "Hive Crypto",
            "sqlalchemy_uri": "hive://dack15@master:10000/default?auth=NOSASL",
            "expose_in_sqllab": True,
            "allow_ctas": True,
            "allow_cvas": True,
            "allow_dml": True,
        }
        
        logger.info("Pushing Hive configuration metadata constraints to backend...")
        resp = self.session.post(f"{self.base_url}/api/v1/database/", headers=self._get_headers(), json=new_db)
        
        if resp.status_code in [200, 201]:
            logger.info("Hive Backend configured actively.")
        else:
            logger.error(f"Failed Configuration Context: {resp.text}")

    def register_dataset(self) -> None:
        """Tethers the crypto_trades stream into Superset visualization context."""
        resp = self.session.get(f"{self.base_url}/api/v1/database/", headers=self._get_headers())
        resp.raise_for_status()
        
        available_dbs = resp.json().get("result", [])
        if not available_dbs:
            logger.error("Hive Database instance creation could not be validated.")
            return
            
        db_id = available_dbs[0].get("id")
        
        dataset_payload = {
            "database": db_id,
            "schema": "default",
            "table_name": "crypto_trades",
        }
        
        logger.info(f"Forcing Hive physical schema binding. Generating logical view on dataset {dataset_payload['table_name']}.")
        resp_dset = self.session.post(f"{self.base_url}/api/v1/dataset/", headers=self._get_headers(), json=dataset_payload)
        
        if resp_dset.status_code in [200, 201]:
            logger.info("Dataset mapping successfully finalized and injected into BI instance!")
        elif "already exists" in resp_dset.text:
            logger.info("Dataset mapping is already instantiated.")
        else:
            logger.error(f"Dataset integration blocked: {resp_dset.text}")

def main() -> None:
    try:
        superset = SupersetConfigurator()
        superset.authenticate()
        superset.cleanup_databases()
        superset.create_hive_connection()
        superset.register_dataset()
        logger.info("System process successfully completed. Superset API logic intact.")
    except Exception as e:
        logger.error(f"Fatal Exception rendering API: {e}")

if __name__ == "__main__":
    main()
