"""
Supabase REST API
"""

import requests
from pprint import pprint

ORIGIN_URL = "https://www.expfrance.fr/findanagent"
BASE_URL = "https://ywzpnbmomlzkcbzzkaqr.supabase.co/rest/v1/agents"

API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inl3enBuYm1vbWx6a2NienprYXFyIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDQ2NDkyMzMsImV4cCI6MjA2MDIyNTIzM30.6b8PT7DMzY2jnRgglammdCpqsT6EKR1_Na2T7djGb9A"

headers = {
    "apikey": API_KEY,
    "Authorization": f"Bearer {API_KEY}",
}


def fetch_agents_page(offset: int, limit: int): 
    params = {
        "select": "id,first_name,middle_family_name,last_name,email,phone,picture,licence_number,full_payload",
        "country_code": "eq.FR",
        "secret_agent": "eq.false", 
        "order": "first_name.asc,last_name.asc,id.asc",
        "offset": offset,
        "limit": limit,
        "source_system": "eq.modelo_france",
        "status": "eq.Active"
    }
    
    response = requests.get(url=BASE_URL, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json(), response.headers


pprint(fetch_agents_page(0, 40)[0])