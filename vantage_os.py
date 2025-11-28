import requests
import urllib.parse

class OrdnanceSurvey:
    def __init__(self):
        # In prod, load from .env. For this session, we use the provided key.
        self.api_key = "qL5Ai49ojGQUah0GR45eIaXTGUjPTixa"
        self.places_endpoint = "https://api.os.uk/search/places/v1/find"
        
    def get_uprn_from_address(self, address_query):
        """
        Queries OS Places API to find the official UPRN for a messy address string.
        """
        if not address_query: return None
        
        params = {
            'query': address_query,
            'key': self.api_key,
            'maxresults': 1
        }
        
        try:
            response = requests.get(self.places_endpoint, params=params)
            data = response.json()
            
            if 'results' in data and len(data['results']) > 0:
                result = data['results'][0]['DPA']
                return {
                    'uprn': result.get('UPRN'),
                    'address': result.get('ADDRESS'),
                    'postcode': result.get('POSTCODE'),
                    'lat': result.get('LAT'),
                    'lng': result.get('LNG'),
                    'x_coordinate': result.get('X_COORDINATE'),
                    'y_coordinate': result.get('Y_COORDINATE')
                }
            return None
            
        except Exception as e:
            print(f"❌ OS API Error: {e}")
            return None

    def get_feature_polygon(self, x, y):
        """
        Queries OS Features API to get the building footprint (Polygon) at a location.
        Note: This usually requires the WFS endpoint with a BBOX or Point query.
        """
        # Feature API implementation would go here (requires XML/GML parsing usually)
        # For now, we focus on the critical UPRN link.
        pass

# Test
if __name__ == "__main__":
    os_api = OrdnanceSurvey()
    test_addr = "311 Whitechapel Road, London"
    print(f"🔍 Resolving: {test_addr}...")
    result = os_api.get_uprn_from_address(test_addr)
    if result:
        print(f"✅ FOUND UPRN: {result['uprn']}")
        print(f"   Official Addr: {result['address']}")
        print(f"   Location: {result['lat']}, {result['lng']}")
    else:
        print("❌ No match.")
