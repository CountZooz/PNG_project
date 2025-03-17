import schedule
import time
import threading
import queue
import logging
from datetime import datetime, timedelta
import psycopg2
import json
from wialon_client import WialonAPIClient

class WialonDataCollector:
    def __init__(self, config):
        """Initialize the data collector"""
        self.wialon_client = WialonAPIClient(config.get('wialon', {}))
        self.db_config = config.get('database', {})
        self.hourly_poll_enabled = config.get('enable_hourly_polling', True)
        self.poll_interval = config.get('poll_interval_minutes', 60)
        self.last_poll_time = None
        self.polling_lock = threading.Lock()
        self.on_demand_queue = queue.Queue()
        self.logger = self._setup_logger()
    
    def _setup_logger(self):
        """Set up logging for the data collector"""
        logger = logging.getLogger('wialon_data_collector')
        logger.setLevel(logging.INFO)
        handler = logging.FileHandler('wialon_data_collector.log')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger
    
    def _get_db_connection(self):
        """Get a connection to the PostgreSQL database"""
        return psycopg2.connect(
            host=self.db_config.get('host', 'localhost'),
            database=self.db_config.get('database', 'fuel_management'),
            user=self.db_config.get('user', 'fuel_admin'),
            password=self.db_config.get('password', 'your_password')
        )
    
    def start(self):
        """Start the data collection services"""
        # Initialize scheduled polling if enabled
        if self.hourly_poll_enabled:
            # Schedule the job to run at the specified interval
            minutes = self.poll_interval
            schedule.every(minutes).minutes.do(self.scheduled_poll)
            
            # Start the scheduler in a background thread
            scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
            scheduler_thread.start()
        
        # Start the on-demand request processor
        on_demand_thread = threading.Thread(target=self._process_on_demand_requests, daemon=True)
        on_demand_thread.start()
        
        self.logger.info("Wialon data collector started")
    
    def _run_scheduler(self):
        """Run the scheduler for periodic polling"""
        while True:
            schedule.run_pending()
            time.sleep(1)
    
    def _process_on_demand_requests(self):
        """Process on-demand data retrieval requests"""
        while True:
            try:
                # Get request from queue with timeout
                request = self.on_demand_queue.get(timeout=5)
                
                # Process the request
                self._fulfill_on_demand_request(request)
                
                # Mark as done
                self.on_demand_queue.task_done()
            except queue.Empty:
                # No requests, just continue waiting
                pass
            except Exception as e:
                self.logger.error(f"Error processing on-demand request: {str(e)}")
    
    def scheduled_poll(self):
        """Perform scheduled data collection"""
        with self.polling_lock:
            try:
                self.logger.info(f"Starting scheduled data collection (every {self.poll_interval} minutes)")
                current_time = datetime.now()
                
                # If this is the first poll, set a reasonable start time
                if not self.last_poll_time:
                    self.last_poll_time = current_time - timedelta(minutes=self.poll_interval)
                
                # Collect data since last poll
                success = self._collect_data(self.last_poll_time, current_time)
                
                if success:
                    self.last_poll_time = current_time
                    self.logger.info("Scheduled data collection completed successfully")
                else:
                    self.logger.error("Scheduled data collection failed")
            except Exception as e:
                self.logger.error(f"Error in scheduled poll: {str(e)}")
    
    def request_on_demand_data(self, unit_ids=None, data_types=None, callback=None):
        """Request immediate data retrieval for specific units/data"""
        request = {
            'unit_ids': unit_ids,  # List of unit IDs or None for all
            'data_types': data_types,  # List of data types or None for all
            'timestamp': datetime.now(),
            'callback': callback  # Optional callback function
        }
        
        self.on_demand_queue.put(request)
        self.logger.info(f"On-demand data request queued: {request['timestamp']}")
        
        return request['timestamp']  # Return request ID for reference
    
    def _fulfill_on_demand_request(self, request):
        """Process an on-demand data request"""
        try:
            self.logger.info(f"Processing on-demand request from {request['timestamp']}")
            
            # Get the most recent data for requested units
            unit_ids = request['unit_ids']
            data_types = request['data_types']
            
            # Determine time range - we'll get the latest data only
            end_time = datetime.now()
            
            # If we did an hourly poll recently, only get data since then
            if self.last_poll_time and (end_time - self.last_poll_time).total_seconds() < 3600:
                start_time = self.last_poll_time
            else:
                # Otherwise get last 15 minutes of data
                start_time = end_time - timedelta(minutes=15)
            
            # Collect the data
            success = self._collect_data(start_time, end_time, unit_ids, data_types)
            
            # Call the callback if provided
            if request['callback']:
                request['callback'](success, start_time, end_time)
            
            self.logger.info(f"On-demand request completed: {success}")
        except Exception as e:
            self.logger.error(f"Error fulfilling on-demand request: {str(e)}")
            if request['callback']:
                request['callback'](False, None, None)
    
    def _collect_data(self, start_time, end_time, unit_ids=None, data_types=None):
        """Collect data from Wialon for the specified time period"""
        try:
            # Ensure we're logged in
            if not self.wialon_client.session_id and not self.wialon_client.login():
                self.logger.error("Failed to authenticate with Wialon API")
                return False
            
            # Determine which units to query
            if unit_ids is None:
                # Get all units if not specified
                units_data = self.wialon_client.get_units()
                if not units_data or "items" not in units_data:
                    self.logger.error("Failed to retrieve units data")
                    return False
                
                unit_ids = [unit["id"] for unit in units_data["items"]]
            
            # Determine which data types to collect
            if data_types is None:
                # Collect all data types by default
                data_types = ['messages', 'sensors', 'positions']
            
            # Connect to the database
            conn = self._get_db_connection()
            cursor = conn.cursor()
            
            try:
                # Process each unit
                for unit_id in unit_ids:
                    self._collect_unit_data(conn, cursor, unit_id, start_time, end_time, data_types)
                
                conn.commit()
                return True
            except Exception as e:
                conn.rollback()
                self.logger.error(f"Database error in data collection: {str(e)}")
                return False
            finally:
                cursor.close()
                conn.close()
                
        except Exception as e:
            self.logger.error(f"Error in data collection: {str(e)}")
            return False
    
    def _collect_unit_data(self, conn, cursor, unit_id, start_time, end_time, data_types):
        """Collect data for a specific unit"""
        # Get unit details
        unit_data = self.wialon_client.get_unit_data(unit_id)
        if not unit_data:
            self.logger.warning(f"Could not get data for unit {unit_id}")
            return
        
        unit_name = unit_data.get('nm', f"Unit {unit_id}")
        
        # Store or update unit in database
        cursor.execute("""
            INSERT INTO wialon_data.units 
            (unit_id, name, updated_at)
            VALUES (%s, %s, %s)
            ON CONFLICT (unit_id) DO UPDATE
            SET name = EXCLUDED.name,
                updated_at = EXCLUDED.updated_at
        """, (unit_id, unit_name, datetime.now()))
        
        # Collect sensors if needed
        if 'sensors' in data_types:
            sensors_data = self.wialon_client.get_unit_sensors(unit_id)
            if sensors_data and 'sensors' in sensors_data:
                self._process_sensors(conn, cursor, unit_id, sensors_data['sensors'])
        
        # Collect messages with sensor data if needed
        if 'messages' in data_types:
            messages = self.wialon_client.get_messages(
                unit_id, 
                start_time, 
                end_time,
                sensors=True
            )
            
            if messages and 'messages' in messages:
                self._process_messages(conn, cursor, unit_id, unit_name, messages['messages'])
    
    def _process_sensors(self, conn, cursor, unit_id, sensors):
        """Process and store sensor data"""
        for sensor in sensors:
            sensor_id = sensor.get('id')
            sensor_name = sensor.get('n', f"Sensor {sensor_id}")
            sensor_type = sensor.get('t', 0)
            parameter = sensor.get('p', '')
            formula = sensor.get('f', '')
            
            try:
                cursor.execute("""
                    INSERT INTO wialon_data.sensors
                    (sensor_id, unit_id, name, type, parameter, formula, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (sensor_id) DO UPDATE
                    SET unit_id = EXCLUDED.unit_id,
                        name = EXCLUDED.name,
                        type = EXCLUDED.type,
                        parameter = EXCLUDED.parameter,
                        formula = EXCLUDED.formula,
                        updated_at = EXCLUDED.updated_at
                """, (
                    sensor_id, unit_id, sensor_name, sensor_type,
                    parameter, formula, datetime.now()
                ))
            except Exception as e:
                self.logger.error(f"Error storing sensor {sensor_id}: {str(e)}")
    
    def _process_messages(self, conn, cursor, unit_id, unit_name, messages):
        """Process and store message data with sensor readings"""
        # Implementation placeholder
        self.logger.debug(f"Processing {len(messages)} messages for unit {unit_id}")
        
        for message in messages:
            timestamp = datetime.fromtimestamp(message.get('t', 0))
            lat = message.get('pos', {}).get('y')
            lon = message.get('pos', {}).get('x')
            speed = message.get('pos', {}).get('s', 0)
            
            # Process sensor readings in the message
            if 'p' in message and 'sensors' in message['p']:
                for sensor_id, value in message['p']['sensors'].items():
                    # Get sensor information
                    cursor.execute("""
                        SELECT name, type FROM wialon_data.sensors
                        WHERE sensor_id = %s
                    """, (sensor_id,))
                    
                    sensor_info = cursor.fetchone()
                    if sensor_info:
                        sensor_name, sensor_type = sensor_info
                    else:
                        sensor_name, sensor_type = f"Sensor {sensor_id}", 0
                    
                    # Store the sensor reading
                    try:
                        cursor.execute("""
                            INSERT INTO wialon_data.sensor_readings
                            (unit_id, unit_name, sensor_id, sensor_name, sensor_type,
                             value, timestamp, latitude, longitude, speed,
                             collection_method)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            unit_id, unit_name, sensor_id, sensor_name, sensor_type,
                            value, timestamp, lat, lon, speed,
                            'on_demand' if self.last_poll_time and timestamp > self.last_poll_time else 'scheduled'
                        ))
                    except Exception as e:
                        self.logger.error(f"Error storing sensor reading: {str(e)}")

if __name__ == "__main__":
    # Example configuration
    config = {
        'wialon': {
            'base_url': 'http://localhost:8080',  # Use mock server for testing
            'token': 'test_token'
        },
        'database': {
            'host': 'localhost',
            'database': 'fuel_management',
            'user': 'fuel_admin',
            'password': 'your_password'
        },
        'enable_hourly_polling': True,
        'poll_interval_minutes': 15  # Poll every 15 minutes for testing
    }
    
    # Create and start the collector
    collector = WialonDataCollector(config)
    collector.start()
    
    # Keep the main thread running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Data collector stopped by user")