from flask import Flask, request, jsonify, send_from_directory
import logging
import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
import sys

# Add project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import from other modules
try:
    from wialon_integration.data_collector import WialonDataCollector
    from data_processing.fuel_processor import FuelDataProcessor
except ImportError as e:
    print(f"Warning: Could not import module: {e}")
    # Create placeholder classes for development without dependencies
    class WialonDataCollector:
        def __init__(self, config):
            print("Warning: Using placeholder WialonDataCollector")
        def start(self):
            pass
        def request_on_demand_data(self, unit_ids=None, data_types=None, callback=None):
            return datetime.now()
    
    class FuelDataProcessor:
        def __init__(self, config):
            print("Warning: Using placeholder FuelDataProcessor")
        def process_new_data(self):
            pass

# Initialize Flask app
app = Flask(__name__, static_folder='../ui')

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('api_server')

# Load configuration
config = {
    'database': {
        'host': os.environ.get('DB_HOST', 'localhost'),
        'database': os.environ.get('DB_NAME', 'fuel_management'),
        'user': os.environ.get('DB_USER', 'fuel_admin'),
        'password': os.environ.get('DB_PASSWORD', 'password123')
    },
    'wialon': {
        'base_url': os.environ.get('WIALON_API_URL', 'http://localhost:8080'),
        'token': os.environ.get('WIALON_TOKEN', 'test_token')
    },
    'enable_hourly_polling': os.environ.get('ENABLE_POLLING', 'true').lower() == 'true',
    'poll_interval_minutes': int(os.environ.get('POLL_INTERVAL', '60'))
}

# Initialize components
try:
    data_collector = WialonDataCollector(config)
    data_collector.start()
    fuel_processor = FuelDataProcessor(config)
except Exception as e:
    logger.error(f"Error initializing components: {str(e)}")

# Database connection function
def get_db_connection():
    """Get a connection to the PostgreSQL database"""
    try:
        return psycopg2.connect(
            host=config['database']['host'],
            database=config['database']['database'],
            user=config['database']['user'],
            password=config['database']['password'],
            cursor_factory=RealDictCursor  # Return results as dictionaries
        )
    except psycopg2.Error as e:
        logger.error(f"Database connection error: {str(e)}")
        raise

# Serve UI files
@app.route('/')
def index():
    """Serve the main frontend page"""
    return send_from_directory(app.static_folder, 'admin-dashboard/index.html')

@app.route('/driver-app/')
@app.route('/driver-app')
def driver_app():
    """Serve the driver app frontend page"""
    return send_from_directory(app.static_folder + '/driver-app', 'index.html')

@app.route('/<path:path>')
def static_files(path):
    """Serve static files"""
    try:
        # Check if file exists in static folder
        full_path = os.path.join(app.static_folder, path)
        if os.path.exists(full_path) and os.path.isfile(full_path):
            return send_from_directory(app.static_folder, path)
        
        # Special case for driver-app
        if path.startswith('driver-app/'):
            remainder = path[len('driver-app/'):]
            driver_app_path = os.path.join(app.static_folder, 'driver-app', remainder)
            if os.path.exists(driver_app_path) and os.path.isfile(driver_app_path):
                return send_from_directory(os.path.join(app.static_folder, 'driver-app'), remainder)
        
        # Default fallback for client-side routing in SPA
        if path.startswith('admin-dashboard/'):
            return send_from_directory(app.static_folder, 'admin-dashboard/index.html')
            
        # If nothing matches, return 404
        return "File not found", 404
    except Exception as e:
        logger.error(f"Error serving static file {path}: {str(e)}")
        return "Error serving file", 500

# API endpoints
@app.route('/api/status', methods=['GET'])
def get_status():
    """Get system status"""
    return jsonify({
        'status': 'online',
        'version': '0.1.0',
        'time': datetime.now().isoformat()
    })

@app.route('/api/refresh-data', methods=['POST'])
def refresh_data():
    """API endpoint to request immediate data refresh"""
    data = request.json or {}
    unit_ids = data.get('unit_ids')
    data_types = data.get('data_types')
    
    # Create a response event
    from threading import Event
    response_event = Event()
    response_data = {'success': False}
    
    # Callback function to signal completion
    def on_data_refresh(success, start_time, end_time):
        response_data['success'] = success
        response_data['start_time'] = start_time.isoformat() if start_time else None
        response_data['end_time'] = end_time.isoformat() if end_time else None
        response_event.set()
    
    try:
        # Request the data refresh
        request_id = data_collector.request_on_demand_data(unit_ids, data_types, on_data_refresh)
        
        # Wait for the response (with timeout)
        if response_event.wait(timeout=30):  # 30-second timeout
            # Also process the data
            fuel_processor.process_new_data()
            return jsonify(response_data)
        else:
            # If timeout, return status indicating request is in progress
            return jsonify({
                'success': None,
                'message': 'Request is still processing',
                'request_id': request_id
            })
    except Exception as e:
        logger.error(f"Error in refresh_data: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/vehicles', methods=['GET'])
def get_vehicles():
    """Get all vehicles"""
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT v.*, vs.fuel_level, vs.odometer, vs.latitude, vs.longitude, vs.timestamp as last_updated
            FROM fuel_management.vehicles v
            LEFT JOIN fuel_management.vehicle_status vs ON v.wialon_unit_id = vs.unit_id
            WHERE v.active = TRUE
            ORDER BY v.name
        """)
        
        vehicles = cursor.fetchall()
        return jsonify(vehicles)
    except Exception as e:
        logger.error(f"Error getting vehicles: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/bowsers', methods=['GET'])
def get_bowsers():
    """Get all bowsers"""
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT b.*, bs.fuel_level, bs.total_dispensed, bs.latitude, bs.longitude, bs.timestamp as last_updated
            FROM fuel_management.bowsers b
            LEFT JOIN fuel_management.bowser_status bs ON b.wialon_unit_id = bs.unit_id
            WHERE b.active = TRUE
            ORDER BY b.name
        """)
        
        bowsers = cursor.fetchall()
        return jsonify(bowsers)
    except Exception as e:
        logger.error(f"Error getting bowsers: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/drivers', methods=['GET'])
def get_drivers():
    """Get all drivers"""
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT * FROM fuel_management.drivers
            WHERE active = TRUE
            ORDER BY name
        """)
        
        drivers = cursor.fetchall()
        return jsonify(drivers)
    except Exception as e:
        logger.error(f"Error getting drivers: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/transactions', methods=['GET'])
def get_transactions():
    """Get transactions with filtering"""
    # Parse query parameters
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    vehicle_id = request.args.get('vehicle_id')
    driver_id = request.args.get('driver_id')
    bowser_id = request.args.get('bowser_id')
    status = request.args.get('status')
    limit = int(request.args.get('limit', 100))
    offset = int(request.args.get('offset', 0))
    
    # Build query
    query = """
        SELECT t.*, 
               d.name as driver_name, 
               v.name as vehicle_name, 
               v.registration as vehicle_registration,
               b.name as bowser_name
        FROM fuel_management.transactions t
        JOIN fuel_management.drivers d ON t.driver_id = d.driver_id
        JOIN fuel_management.vehicles v ON t.vehicle_id = v.vehicle_id
        JOIN fuel_management.bowsers b ON t.bowser_id = b.bowser_id
        WHERE 1=1
    """
    
    params = []
    
    # Add filters
    if start_date:
        query += " AND t.timestamp >= %s"
        params.append(start_date)
    
    if end_date:
        query += " AND t.timestamp <= %s"
        params.append(end_date)
    
    if vehicle_id:
        query += " AND t.vehicle_id = %s"
        params.append(vehicle_id)
    
    if driver_id:
        query += " AND t.driver_id = %s"
        params.append(driver_id)
    
    if bowser_id:
        query += " AND t.bowser_id = %s"
        params.append(bowser_id)
    
    if status:
        query += " AND t.status = %s"
        params.append(status)
    
    # Add ordering and pagination
    query += " ORDER BY t.timestamp DESC LIMIT %s OFFSET %s"
    params.extend([limit, offset])
    
    # Execute query
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute(query, params)
        transactions = cursor.fetchall()
        
        # Get total count for pagination
        count_query = """
            SELECT COUNT(*) FROM fuel_management.transactions t
            WHERE 1=1
        """
        
        count_params = []
        
        # Add filters to count query
        if start_date:
            count_query += " AND t.timestamp >= %s"
            count_params.append(start_date)
        
        if end_date:
            count_query += " AND t.timestamp <= %s"
            count_params.append(end_date)
        
        if vehicle_id:
            count_query += " AND t.vehicle_id = %s"
            count_params.append(vehicle_id)
        
        if driver_id:
            count_query += " AND t.driver_id = %s"
            count_params.append(driver_id)
        
        if bowser_id:
            count_query += " AND t.bowser_id = %s"
            count_params.append(bowser_id)
        
        if status:
            count_query += " AND t.status = %s"
            count_params.append(status)
        
        cursor.execute(count_query, count_params)
        total_count = cursor.fetchone()['count']
        
        return jsonify({
            'transactions': transactions,
            'total': total_count,
            'limit': limit,
            'offset': offset
        })
    except Exception as e:
        logger.error(f"Error getting transactions: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/transactions/<transaction_id>', methods=['GET'])
def get_transaction(transaction_id):
    """Get details of a specific transaction"""
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get transaction data
        cursor.execute("""
            SELECT t.*, 
                   d.name as driver_name, 
                   v.name as vehicle_name, 
                   v.registration as vehicle_registration,
                   b.name as bowser_name
            FROM fuel_management.transactions t
            JOIN fuel_management.drivers d ON t.driver_id = d.driver_id
            JOIN fuel_management.vehicles v ON t.vehicle_id = v.vehicle_id
            JOIN fuel_management.bowsers b ON t.bowser_id = b.bowser_id
            WHERE t.transaction_id = %s
        """, (transaction_id,))
        
        transaction = cursor.fetchone()
        
        if not transaction:
            return jsonify({'error': 'Transaction not found'}), 404
        
        # Get odometer readings
        cursor.execute("""
            SELECT * FROM fuel_management.odometer_readings
            WHERE transaction_id = %s
            ORDER BY timestamp
        """, (transaction_id,))
        
        odometer_readings = cursor.fetchall()
        
        # Get fuel events
        cursor.execute("""
            SELECT * FROM fuel_management.fuel_events
            WHERE transaction_id = %s
            ORDER BY timestamp
        """, (transaction_id,))
        
        fuel_events = cursor.fetchall()
        
        return jsonify({
            'transaction': transaction,
            'odometer_readings': odometer_readings,
            'fuel_events': fuel_events
        })
    except Exception as e:
        logger.error(f"Error getting transaction details: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/dashboard/summary', methods=['GET'])
def get_dashboard_summary():
    """Get summary statistics for dashboard"""
    # Parse date range
    start_date = request.args.get('start_date', (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'))
    end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
    
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get transaction summary
        cursor.execute("""
            SELECT 
                COUNT(*) as total_transactions,
                SUM(dispensed_amount) as total_fuel_dispensed,
                SUM(received_amount) as total_fuel_received,
                COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_transactions,
                COUNT(CASE WHEN status = 'discrepancy' THEN 1 END) as discrepancy_transactions,
                COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending_transactions,
                COUNT(CASE WHEN status = 'timed_out' THEN 1 END) as timed_out_transactions
            FROM fuel_management.transactions
            WHERE timestamp BETWEEN %s AND %s
        """, (start_date, end_date))
        
        transaction_summary = cursor.fetchone()
        
        # Get vehicle summary
        cursor.execute("""
            SELECT COUNT(*) as total_vehicles,
                   COUNT(CASE WHEN vs.fuel_level < v.fuel_capacity * 0.2 THEN 1 END) as low_fuel_vehicles
            FROM fuel_management.vehicles v
            LEFT JOIN fuel_management.vehicle_status vs ON v.wialon_unit_id = vs.unit_id
            WHERE v.active = TRUE
        """)
        
        vehicle_summary = cursor.fetchone()
        
        # Get bowser summary
        cursor.execute("""
            SELECT COUNT(*) as total_bowsers,
                   COUNT(CASE WHEN bs.fuel_level < b.capacity * 0.2 THEN 1 END) as low_fuel_bowsers
            FROM fuel_management.bowsers b
            LEFT JOIN fuel_management.bowser_status bs ON b.wialon_unit_id = bs.unit_id
            WHERE b.active = TRUE
        """)
        
        bowser_summary = cursor.fetchone()
        
        # Get recent transactions
        cursor.execute("""
            SELECT t.*, 
                   d.name as driver_name, 
                   v.name as vehicle_name, 
                   v.registration as vehicle_registration,
                   b.name as bowser_name
            FROM fuel_management.transactions t
            JOIN fuel_management.drivers d ON t.driver_id = d.driver_id
            JOIN fuel_management.vehicles v ON t.vehicle_id = v.vehicle_id
            JOIN fuel_management.bowsers b ON t.bowser_id = b.bowser_id
            ORDER BY t.timestamp DESC
            LIMIT 5
        """)
        
        recent_transactions = cursor.fetchall()
        
        return jsonify({
            'transaction_summary': transaction_summary,
            'vehicle_summary': vehicle_summary,
            'bowser_summary': bowser_summary,
            'recent_transactions': recent_transactions
        })
    except Exception as e:
        logger.error(f"Error getting dashboard summary: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/simulate/transaction', methods=['POST'])
def simulate_transaction():
    """Simulate a fuel transaction (for testing)"""
    # Import the simulator
    try:
        sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
        try:
            from simulation.data_generator import WialonSimulator
            
            # Initialize simulator
            simulator = WialonSimulator(config['database'])
            
            # Simulate transaction
            transaction = simulator.simulate_fuel_transaction()
            
            # Process the new data
            fuel_processor.process_new_data()
            
            return jsonify({
                'success': True,
                'transaction': transaction
            })
        except ImportError as e:
            logger.error(f"Could not import simulator: {str(e)}")
            return jsonify({'error': 'Simulator not available', 'details': str(e)}), 500
    except Exception as e:
        logger.error(f"Error simulating transaction: {str(e)}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug_mode = os.environ.get('FLASK_DEBUG', 'true').lower() == 'true'
    app.run(host='0.0.0.0', port=port, debug=debug_mode)
