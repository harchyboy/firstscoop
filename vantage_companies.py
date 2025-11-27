import os
import requests
import base64
import time
from dotenv import load_dotenv

class CompaniesHouseRegistry:
    def __init__(self):
        load_dotenv()
        self.api_key = os.getenv('COMPANIES_HOUSE_KEY')
        self.base_url = "https://api.company-information.service.gov.uk"
        
        if not self.api_key:
            print("⚠️  WARNING: No COMPANIES_HOUSE_KEY found in .env")
            print("   (Data enrichment will fail or be limited)")

    def _get_headers(self):
        # Companies House Basic Auth requires the key as the username, empty password
        auth_string = f"{self.api_key}:"
        encoded_auth = base64.b64encode(auth_string.encode('utf-8')).decode('utf-8')
        return {
            "Authorization": f"Basic {encoded_auth}"
        }

    def search_company(self, company_name):
        """
        Search for a company by name to get its official Company Number.
        """
        if not self.api_key: return None

        endpoint = f"{self.base_url}/search/companies"
        params = {"q": company_name, "items_per_page": 1}
        
        try:
            response = requests.get(endpoint, headers=self._get_headers(), params=params)
            if response.status_code == 200:
                data = response.json()
                if data.get('items'):
                    return data['items'][0] # Return top match
            return None
        except Exception as e:
            print(f"   ❌ API Error (Search): {e}")
            return None

    def get_company_profile(self, company_number):
        """
        Get full details (status, address, accounts) for a company number.
        """
        if not self.api_key: return None

        endpoint = f"{self.base_url}/company/{company_number}"
        
        try:
            response = requests.get(endpoint, headers=self._get_headers())
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                print(f"   ⚠️ Company {company_number} not found.")
            return None
        except Exception as e:
            print(f"   ❌ API Error (Profile): {e}")
            return None

    def get_company_officers(self, company_number):
        """
        Get the list of directors/officers.
        """
        if not self.api_key: return []

        endpoint = f"{self.base_url}/company/{company_number}/officers"
        
        try:
            response = requests.get(endpoint, headers=self._get_headers())
            if response.status_code == 200:
                data = response.json()
                return data.get('items', [])
            return []
        except Exception as e:
            print(f"   ❌ API Error (Officers): {e}")
            return []

    def get_psc(self, company_number):
        """
        Get Persons with Significant Control (the real owners).
        """
        if not self.api_key: return []

        endpoint = f"{self.base_url}/company/{company_number}/persons-with-significant-control"
        
        try:
            response = requests.get(endpoint, headers=self._get_headers())
            if response.status_code == 200:
                data = response.json()
                return data.get('items', [])
            return []
        except Exception as e:
            print(f"   ❌ API Error (PSC): {e}")
            return []

