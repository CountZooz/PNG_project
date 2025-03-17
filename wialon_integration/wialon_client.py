import requests
import json
import logging
import time
from datetime import datetime, timedelta

class WialonAPIClient:
    def __init__(self, config):
        """Initialize the Wialon API client"""
        self.base_url = config.get('base_url', "https://hst-api.wialon.com")
        self.token = config.get('token')
        self.session_id = None
        self.logger = self._setup_logger()
    
    def _setup_logger(self):
        """Set up logging for the API client"""
        logger = logging.getLogger('wialon_client')
        logger.setLevel(logging.INFO)
        handler = logging.FileHandler('wialon_client.log')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger
    
    def login(self):
        """Authenticate with Wialon API and establish a session"""
        params = {
            "token": self.token,
            "svc": "token/login"
        }
        
        try:
            response = requests.get(f"{self.base_url}/wialon/ajax.html", params=params)
            response.raise_for_status()
            data = response.json()
            
            if "eid" in data:
                self.session_id = data["eid"]
                self.logger.info(f"Successfully authenticated with Wialon API. Session ID: {self.session_id}")
                return True
            else:
                self.logger.error(f"Failed to authenticate: {data}")
                return False
        except Exception as e:
            self.logger.error(f"Exception during authentication: {str(e)}")
            return False
    
    def _call_api(self, service, params=None):
        """Make a call to Wialon API"""
        if not self.session_id:
            if not self.login():
                return None
        
        api_params = {
            "svc": service,
            "sid": self.session_id
        }
        
        if params:
            api_params["params"] = json.dumps(params)
        
        try:
            response = requests.get(f"{self.base_url}/wialon/ajax.html", params=api_params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            # Handle session expiration
            if response.status_code == 401 or "Authorization failed" in response.text:
                self.session_id = None
                self.logger.warning("Session expired, attempting to re-authenticate...")
                return self._call_api(service, params)
            else:
                self.logger.error(f"HTTP error in API call: {str(e)}")
                return None
        except Exception as e:
            self.logger.error(f"Exception in API call: {str(e)}")
            return None
    
    def get_units(self):
        """Get all units (trucks and bowsers)"""
        params = {
            "spec": {
                "itemsType": "avl_unit",
                "propName": "sys_name",
                "propValueMask": "*",
                "sortType": "sys_name"
            },
            "force": 1,
            "flags": 1,
            "from": 0,
            "to": 0
        }
        
        return self._call_api("core/search_items", params)
    
    def get_unit_data(self, unit_id):
        """Get detailed information about a specific unit"""
        params = {
            "id": unit_id,
            "flags": 1
        }
        
        return self._call_api("core/search_item", params)
    
    def get_unit_sensors(self, unit_id):
        """Get sensors for a specific unit"""
        params = {
            "unitId": unit_id
        }
        
        return self._call_api("unit/get_sensors", params)
    
    def get_unit_position(self, unit_id):
        """Get the current position of a unit"""
        params = {
            "id": unit_id
        }
        
        return self._call_api("unit/get_unit_data", params)
    
    def get_last_message(self, unit_id):
        """Get the last message from a unit with sensor data"""
        params = {
            "unitId": unit_id,
            "flags": 0x00000001 | 0x00000008  # Last message + sensor data
        }
        
        return self._call_api("unit/calc_last_message", params)
    
    def get_messages(self, unit_id, from_time, to_time, sensors=None):
        """Get messages for a specific time period with sensor data"""
        params = {
            "itemId": unit_id,
            "timeFrom": int(from_time.timestamp()),
            "timeTo": int(to_time.timestamp()),
            "flags": 0x0000000F,  # All messages with position data
            "flagsMask": 0x0000000F,
            "loadCount": 0xFFFFFFFF  # Load all messages
        }
        
        if sensors:
            params["sensors"] = sensors
        
        return self._call_api("messages/load_interval", params)
    
    def get_geozones(self, resource_id):
        """Get geozones from a resource"""
        params = {
            "itemId": resource_id,
            "flags": 1
        }
        
        return self._call_api("resource/get_zone_data", params)
    
    def check_unit_in_zone(self, unit_id, zone_ids):
        """Check if a unit is currently in any of the specified zones"""
        # Get unit position
        position_data = self.get_unit_position(unit_id)
        if not position_data or 'pos' not in position_data:
            return None
        
        # Get position coordinates
        pos = position_data['pos']
        lat, lon = pos.get('y'), pos.get('x')
        
        # Check each zone
        for zone_id in zone_ids:
            # Get zone data
            zone_data = self.get_zone_data(zone_id)
            if not zone_data:
                continue
            
            # Check if point is in zone
            if self._point_in_zone(lat, lon, zone_data):
                return zone_id
        
        return None
    
    def _point_in_zone(self, lat, lon, zone_data):
        """Check if a point is within a zone"""
        # Implementation depends on zone type (circle, polygon, etc.)
        # This is a simplified version for circle geozones
        
        if zone_data.get('t') == 1:  # Circle
            center = zone_data.get('p', {}).get('c', {})
            center_lat = center.get('y')
            center_lon = center.get('x')
            radius = zone_data.get('p', {}).get('r', 0)
            
            if center_lat and center_lon and radius:
                # Calculate distance (simplified, using spherical law of cosines)
                lat1, lon1 = math.radians(lat), math.radians(lon)
                lat2, lon2 = math.radians(center_lat), math.radians(center_lon)
                
                distance = math.acos(
                    math.sin(lat1) * math.sin(lat2) + 
                    math.cos(lat1) * math.cos(lat2) * math.cos(lon1 - lon2)
                ) * 6371000  # Earth radius in meters
                
                return distance <= radius
        
        return False