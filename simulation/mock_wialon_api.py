from flask import Flask, request, jsonify
import json
import time
from datetime import datetime, timedelta
import os
import logging

app = Flask(__name__)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('mock_wialon_api')

# Initialize simulated data storage
simulated_data = {
    'units': {'items': []},
    'sensors': {},
    'last_messages': {},
    'messages': {},
    'geozones': {'items': []},
    'session': None
}

@app.route('/wialon/ajax.html', methods=['GET'])
def wialon_api():
    """Mock Wialon API endpoint"""
    svc = request.args.get('svc')
    sid = request.args.get('sid')
    
    logger.info(f"API Request: {svc}")
    
    # Handle authentication
    if svc == 'token/login':
        token = request.args.get('token')
        if token == 'simulated_token' or token == 'test_token':
            simulated_data['session'] = "simulation_session_123456"
            return jsonify({
                "eid": simulated_data['session'],
                "user": {"nm": "Simulation User", "id": 12345}
            })
        else:
            return jsonify({"error": 1, "reason": "Invalid token"})
    
    # Check if authenticated for other services
    if svc != 'token/login' and (not sid or sid != simulated_data['session']):
        return jsonify({"error": 1, "reason": "Not authenticated"})
    
    # Handle different service endpoints
    if svc == 'core/search_items':
        # Parse parameters
        params = json.loads(request.args.get('params', '{}'))
        spec = params.get('spec', {})
        items_type = spec.get('itemsType', 'avl_unit')
        
        # Return simulated units
        return jsonify(simulated_data['units'])
    
    elif svc == 'core/search_item':
        # Parse parameters
        params = json.loads(request.args.get('params', '{}'))
        item_id = params.get('id')
        
        # Find the item in our simulated data
        for item in simulated_data['units'].get('items', []):
            if item.get('id') == item_id:
                return jsonify(item)
        
        return jsonify({"error": 1, "reason": "Item not found"})
    
    elif svc == 'unit/get_sensors':
        # Parse parameters
        params = json.loads(request.args.get('params', '{}'))
        unit_id = params.get('unitId')
        
        # Return simulated sensors for the unit
        return jsonify(simulated_data['sensors'].get(str(unit_id), {"sensors": []}))
    
    elif svc == 'unit/calc_last_message':
        # Parse parameters
        params = json.loads(request.args.get('params', '{}'))
        unit_id = params.get('unitId')
        
        # Return simulated last message for the unit
        return jsonify(simulated_data['last_messages'].get(str(unit_id), {}))
    
    elif svc == 'messages/load_interval':
        # Parse parameters
        params = json.loads(request.args.get('params', '{}'))
        unit_id = params.get('itemId')
        time_from = params.get('timeFrom')
        time_to = params.get('timeTo')
        
        # Return simulated messages for the unit within time range
        messages = simulated_data['messages'].get(str(unit_id), [])
        filtered_messages = [
            msg for msg in messages
            if time_from <= msg.get('t', 0) <= time_to
        ]
        
        return jsonify({"messages": filtered_messages})
    
    elif svc == 'resource/get_zone_data':
        # Parse parameters
        params = json.loads(request.args.get('params', '{}'))
        resource_id = params.get('itemId')
        
        # Return simulated geozones
        return jsonify(simulated_data['geozones'])
    
    # Handle other endpoints as needed
    logger.warning(f"Unhandled service: {svc}")
    return jsonify({"error": 1, "reason": "Service not implemented in mock"})

def load_simulated_data(file_path=None):
    """Load simulated data from a JSON file"""
    global simulated_data
    
    if file_path and os.path.exists(file_path):
        with open(file_path, 'r') as f:
            simulated_data = json.load(f)
        logger.info(f"Loaded simulated data from {file_path}")
    else:
        logger.warning("No data file provided or file not found. Using empty data.")

def update_simulated_data(data):
    """Update the simulated data with new values"""
    global simulated_data
    
    for key, value in data.items():
        if key in simulated_data:
            simulated_data[key] = value
    
    logger.info("Updated simulated data")

if __name__ == '__main__':
    # Check if data file was provided
    data_file = os.environ.get('SIMULATED_DATA_FILE', 'simulation/simulated_data.json')
    load_simulated_data(data_file)
    
    # Start the server
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=True)