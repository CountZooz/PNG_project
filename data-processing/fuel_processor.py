import psycopg2
import json
import logging
from datetime import datetime, timedelta
import math
from geopy.distance import geodesic

class FuelDataProcessor:
    def __init__(self, config):
        """Initialize the fuel data processor"""
        self.db_config = config.get('database', {})
        self.logger = self._setup_logger()
        self.geozone_proximity_threshold = config.get('geozone_proximity_meters', 50)
        
    def _setup_logger(self):
        """Set up logging for the fuel processor"""
        logger = logging.getLogger('fuel_processor')
        logger.setLevel(logging.INFO)
        handler = logging.FileHandler('fuel_processor.log')
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
    
    def process_new_data(self):
        """Process new data from Wialon tables into application tables"""
        conn = self._get_db_connection()
        try:
            # Process in this order:
            self._process_new_sensor_readings(conn)
            self._process_new_geozone_events(conn)
            self._process_new_ibutton_events(conn)
            self._match_transactions(conn)
            
            conn.commit()
            self.logger.info("Processed new data successfully")
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Error processing data: {str(e)}")
        finally:
            conn.close()
    
    def _process_new_sensor_readings(self, conn):
        """Process new sensor readings"""
        cursor = conn.cursor()
        
        try:
            # Get unprocessed sensor readings
            cursor.execute("""
                SELECT reading_id, unit_id, unit_name, sensor_id, sensor_name, value, timestamp, 
                       latitude, longitude
                FROM wialon_data.sensor_readings
                WHERE processed = FALSE
                ORDER BY timestamp
            """)
            
            readings = cursor.fetchall()
            self.logger.info(f"Processing {len(readings)} new sensor readings")
            
            for reading in readings:
                reading_id, unit_id, unit_name, sensor_id, sensor_name, value, timestamp, lat, lon = reading
                
                # Determine reading type based on sensor name
                if 'fuel_level' in sensor_name.lower():
                    self._process_fuel_level_reading(conn, reading)
                elif 'fuel_flow' in sensor_name.lower() or 'flow_meter' in sensor_name.lower():
                    self._process_fuel_flow_reading(conn, reading)
                
                # Mark as processed
                cursor.execute("""
                    UPDATE wialon_data.sensor_readings
                    SET processed = TRUE
                    WHERE reading_id = %s
                """, (reading_id,))
            
            conn.commit()
        except Exception as e:
            self.logger.error(f"Error processing sensor readings: {str(e)}")
            raise
    
    def _process_fuel_level_reading(self, conn, reading):
        """Process a fuel level sensor reading"""
        reading_id, unit_id, unit_name, sensor_id, sensor_name, value, timestamp, lat, lon = reading
        cursor = conn.cursor()
        
        try:
            # Get previous reading to detect significant changes
            cursor.execute("""
                SELECT value, timestamp
                FROM wialon_data.sensor_readings
                WHERE unit_id = %s AND sensor_id = %s AND timestamp < %s
                ORDER BY timestamp DESC
                LIMIT 1
            """, (unit_id, sensor_id, timestamp))
            
            prev_reading = cursor.fetchone()
            
            if prev_reading:
                prev_value, prev_timestamp = prev_reading
                time_diff = (timestamp - prev_timestamp).total_seconds()
                value_diff = value - prev_value
                
                # If significant fuel level increase (received fuel)
                if value_diff > 5 and time_diff < 3600:  # 5 liter threshold, within 1 hour
                    # Record a fuel reception event
                    cursor.execute("""
                        INSERT INTO fuel_management.fuel_events
                        (unit_id, event_type, timestamp, amount, latitude, longitude)
                        VALUES (%s, 'received', %s, %s, %s, %s)
                        RETURNING event_id
                    """, (unit_id, timestamp, value_diff, lat, lon))
                    
                    event_id = cursor.fetchone()[0]
                    self.logger.info(f"Recorded fuel reception event {event_id} for unit {unit_id}: {value_diff} liters")
            
            # Find the vehicle ID from the unit ID
            cursor.execute("""
                SELECT vehicle_id FROM fuel_management.vehicles
                WHERE wialon_unit_id = %s
            """, (unit_id,))
            
            vehicle_result = cursor.fetchone()
            if vehicle_result:
                vehicle_id = vehicle_result[0]
                
                # Update vehicle status
                cursor.execute("""
                    UPDATE fuel_management.vehicle_status
                    SET fuel_level = %s,
                        latitude = %s,
                        longitude = %s,
                        timestamp = %s
                    WHERE unit_id = %s
                """, (value, lat, lon, timestamp, unit_id))
                
                if cursor.rowcount == 0:
                    # Insert new status if not exists
                    cursor.execute("""
                        INSERT INTO fuel_management.vehicle_status
                        (unit_id, fuel_level, latitude, longitude, timestamp)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (unit_id, value, lat, lon, timestamp))
        
        except Exception as e:
            self.logger.error(f"Error processing fuel level reading: {str(e)}")
            raise
    
    def _process_fuel_flow_reading(self, conn, reading):
        """Process a fuel flow meter reading"""
        reading_id, unit_id, unit_name, sensor_id, sensor_name, value, timestamp, lat, lon = reading
        cursor = conn.cursor()
        
        try:
            # Get previous reading to calculate flow
            cursor.execute("""
                SELECT value, timestamp
                FROM wialon_data.sensor_readings
                WHERE unit_id = %s AND sensor_id = %s AND timestamp < %s
                ORDER BY timestamp DESC
                LIMIT 1
            """, (unit_id, sensor_id, timestamp))
            
            prev_reading = cursor.fetchone()
            
            if prev_reading:
                prev_value, prev_timestamp = prev_reading
                time_diff = (timestamp - prev_timestamp).total_seconds()
                value_diff = value - prev_value
                
                # If positive flow and reasonable time difference
                if value_diff > 0 and time_diff < 3600:  # within 1 hour
                    # Record a fuel dispensation event
                    cursor.execute("""
                        INSERT INTO fuel_management.fuel_events
                        (unit_id, event_type, timestamp, amount, latitude, longitude)
                        VALUES (%s, 'dispensed', %s, %s, %s, %s)
                        RETURNING event_id
                    """, (unit_id, timestamp, value_diff, lat, lon))
                    
                    event_id = cursor.fetchone()[0]
                    self.logger.info(f"Recorded fuel dispensation event {event_id} for bowser {unit_id}: {value_diff} liters")
            
            # Find the bowser ID from the unit ID
            cursor.execute("""
                SELECT bowser_id FROM fuel_management.bowsers
                WHERE wialon_unit_id = %s
            """, (unit_id,))
            
            bowser_result = cursor.fetchone()
            if bowser_result:
                bowser_id = bowser_result[0]
                
                # Update bowser status
                cursor.execute("""
                    UPDATE fuel_management.bowser_status
                    SET total_dispensed = %s,
                        latitude = %s,
                        longitude = %s,
                        timestamp = %s
                    WHERE unit_id = %s
                """, (value, lat, lon, timestamp, unit_id))
                
                if cursor.rowcount == 0:
                    # Insert new status if not exists
                    cursor.execute("""
                        INSERT INTO fuel_management.bowser_status
                        (unit_id, total_dispensed, latitude, longitude, timestamp)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (unit_id, value, lat, lon, timestamp))
        
        except Exception as e:
            self.logger.error(f"Error processing fuel flow reading: {str(e)}")
            raise
    
    def _process_new_geozone_events(self, conn):
        """Process new geozone events"""
        cursor = conn.cursor()
        
        try:
            # Get unprocessed geozone events
            cursor.execute("""
                SELECT event_id, unit_id, geozone_id, event_type, timestamp, 
                       latitude, longitude
                FROM wialon_data.geozone_events
                WHERE processed = FALSE
                ORDER BY timestamp
            """)
            
            events = cursor.fetchall()
            self.logger.info(f"Processing {len(events)} new geozone events")
            
            for event in events:
                event_id, unit_id, geozone_id, event_type, timestamp, lat, lon = event
                
                # Find the vehicle ID from the unit ID
                cursor.execute("""
                    SELECT vehicle_id FROM fuel_management.vehicles
                    WHERE wialon_unit_id = %s
                """, (unit_id,))
                
                vehicle_result = cursor.fetchone()
                if not vehicle_result:
                    # Mark as processed even if we couldn't find a vehicle
                    cursor.execute("""
                        UPDATE wialon_data.geozone_events
                        SET processed = TRUE
                        WHERE event_id = %s
                    """, (event_id,))
                    continue
                
                vehicle_id = vehicle_result[0]
                
                # Check if this geozone is associated with a bowser
                cursor.execute("""
                    SELECT bowser_id FROM fuel_management.bowsers
                    WHERE geozone_id = %s
                """, (geozone_id,))
                
                bowser_result = cursor.fetchone()
                if bowser_result:
                    bowser_id = bowser_result[0]
                    
                    # Record vehicle proximity to bowser
                    cursor.execute("""
                        INSERT INTO fuel_management.vehicle_bowser_proximity
                        (vehicle_id, bowser_id, event_type, timestamp, latitude, longitude)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (vehicle_id, bowser_id, event_type, timestamp, lat, lon))
                    
                    self.logger.info(f"Recorded proximity event: Vehicle {vehicle_id} {event_type} bowser {bowser_id} geozone")
                
                # Mark as processed
                cursor.execute("""
                    UPDATE wialon_data.geozone_events
                    SET processed = TRUE
                    WHERE event_id = %s
                """, (event_id,))
            
            conn.commit()
        except Exception as e:
            self.logger.error(f"Error processing geozone events: {str(e)}")
            raise
    
    def _process_new_ibutton_events(self, conn):
        """Process new iButton events"""
        cursor = conn.cursor()
        
        try:
            # Get unprocessed iButton events
            cursor.execute("""
                SELECT event_id, unit_id, ibutton_code, timestamp, 
                       latitude, longitude
                FROM wialon_data.ibutton_events
                WHERE processed = FALSE
                ORDER BY timestamp
            """)
            
            events = cursor.fetchall()
            self.logger.info(f"Processing {len(events)} new iButton events")
            
            for event in events:
                event_id, unit_id, ibutton_code, timestamp, lat, lon = event
                
                # Find the vehicle ID from the unit ID
                cursor.execute("""
                    SELECT vehicle_id FROM fuel_management.vehicles
                    WHERE wialon_unit_id = %s
                """, (unit_id,))
                
                vehicle_result = cursor.fetchone()
                if not vehicle_result:
                    # Mark as processed even if we couldn't find a vehicle
                    cursor.execute("""
                        UPDATE wialon_data.ibutton_events
                        SET processed = TRUE
                        WHERE event_id = %s
                    """, (event_id,))
                    continue
                
                vehicle_id = vehicle_result[0]
                
                # Look up driver by iButton code
                cursor.execute("""
                    SELECT driver_id, name FROM fuel_management.drivers
                    WHERE ibutton_code = %s
                """, (ibutton_code,))
                
                driver_result = cursor.fetchone()
                if driver_result:
                    driver_id, driver_name = driver_result
                    
                    # Record authentication event
                    cursor.execute("""
                        INSERT INTO fuel_management.authentication_events
                        (driver_id, vehicle_id, ibutton_code, timestamp, latitude, longitude)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        RETURNING event_id
                    """, (driver_id, vehicle_id, ibutton_code, timestamp, lat, lon))
                    
                    auth_event_id = cursor.fetchone()[0]
                    self.logger.info(f"Recorded authentication event {auth_event_id}: Driver {driver_name} on vehicle {vehicle_id}")
                    
                    # Check if vehicle is near a bowser
                    self._check_and_initiate_transaction(conn, driver_id, vehicle_id, timestamp, lat, lon)
                else:
                    self.logger.warning(f"Unknown iButton code: {ibutton_code}")
                
                # Mark as processed
                cursor.execute("""
                    UPDATE wialon_data.ibutton_events
                    SET processed = TRUE
                    WHERE event_id = %s
                """, (event_id,))
            
            conn.commit()
        except Exception as e:
            self.logger.error(f"Error processing iButton events: {str(e)}")
            raise
    
    def _check_and_initiate_transaction(self, conn, driver_id, vehicle_id, timestamp, lat, lon):
        """Check if vehicle is near a bowser and initiate transaction if so"""
        cursor = conn.cursor()
        
        try:
            # Get all bowser locations
            cursor.execute("""
                SELECT b.bowser_id, b.name, g.center_latitude, g.center_longitude, g.radius
                FROM fuel_management.bowsers b
                JOIN wialon_data.geozones g ON b.geozone_id = g.geozone_id
                WHERE b.active = TRUE
            """)
            
            bowsers = cursor.fetchall()
            
            for bowser in bowsers:
                bowser_id, name, bowser_lat, bowser_lon, radius = bowser
                
                # Calculate distance between vehicle and bowser
                if lat and lon and bowser_lat and bowser_lon:
                    distance = geodesic((lat, lon), (bowser_lat, bowser_lon)).meters
                    
                    # If within proximity threshold
                    if distance <= (radius or self.geozone_proximity_threshold):
                        # Check if there's already a pending transaction
                        cursor.execute("""
                            SELECT transaction_id FROM fuel_management.transactions
                            WHERE driver_id = %s AND vehicle_id = %s AND bowser_id = %s
                            AND status = 'pending'
                            AND timestamp > %s
                        """, (driver_id, vehicle_id, bowser_id, timestamp - timedelta(hours=1)))
                        
                        if cursor.fetchone():
                            # Already have a pending transaction
                            continue
                        
                        # Initiate a pending transaction
                        import uuid
                        transaction_id = str(uuid.uuid4())
                        
                        cursor.execute("""
                            INSERT INTO fuel_management.transactions
                            (transaction_id, driver_id, vehicle_id, bowser_id, timestamp, status)
                            VALUES (%s, %s, %s, %s, %s, 'pending')
                            RETURNING transaction_id
                        """, (transaction_id, driver_id, vehicle_id, bowser_id, timestamp))
                        
                        transaction_id = cursor.fetchone()[0]
                        self.logger.info(f"Initiated transaction {transaction_id}: Driver {driver_id}, Vehicle {vehicle_id}, Bowser {bowser_id}")
                        
                        # Return after finding the first match
                        return transaction_id
            
            return None
        except Exception as e:
            self.logger.error(f"Error checking bowser proximity: {str(e)}")
            raise
    
    def _match_transactions(self, conn):
        """Match dispensation events with transactions and vehicle fuel level changes"""
        cursor = conn.cursor()
        
        try:
            # Get pending transactions
            cursor.execute("""
                SELECT transaction_id, driver_id, vehicle_id, bowser_id, timestamp
                FROM fuel_management.transactions
                WHERE status = 'pending'
                ORDER BY timestamp
            """)
            
            transactions = cursor.fetchall()
            self.logger.info(f"Processing {len(transactions)} pending transactions")
            
            for transaction in transactions:
                transaction_id, driver_id, vehicle_id, bowser_id, timestamp = transaction
                
                # Get the bowser's Wialon unit ID
                cursor.execute("""
                    SELECT wialon_unit_id FROM fuel_management.bowsers
                    WHERE bowser_id = %s
                """, (bowser_id,))
                
                bowser_unit_result = cursor.fetchone()
                if not bowser_unit_result:
                    continue
                
                bowser_unit_id = bowser_unit_result[0]
                
                # Look for dispensation events from this bowser shortly after the transaction was initiated
                cursor.execute("""
                    SELECT event_id, amount, timestamp
                    FROM fuel_management.fuel_events
                    WHERE unit_id = %s 
                    AND event_type = 'dispensed'
                    AND timestamp BETWEEN %s AND %s
                    AND transaction_id IS NULL
                    ORDER BY timestamp
                    LIMIT 1
                """, (bowser_unit_id, timestamp, timestamp + timedelta(minutes=30)))
                
                dispensation = cursor.fetchone()
                
                if dispensation:
                    dispensation_id, dispensed_amount, dispensation_time = dispensation
                    
                    # Update the dispensation event with the transaction ID
                    cursor.execute("""
                        UPDATE fuel_management.fuel_events
                        SET transaction_id = %s
                        WHERE event_id = %s
                    """, (transaction_id, dispensation_id))
                    
                    # Get the vehicle's Wialon unit ID
                    cursor.execute("""
                        SELECT wialon_unit_id FROM fuel_management.vehicles
                        WHERE vehicle_id = %s
                    """, (vehicle_id,))
                    
                    vehicle_unit_result = cursor.fetchone()
                    if not vehicle_unit_result:
                        continue
                    
                    vehicle_unit_id = vehicle_unit_result[0]
                    
                    # Look for corresponding fuel reception in the vehicle
                    cursor.execute("""
                        SELECT event_id, amount, timestamp
                        FROM fuel_management.fuel_events
                        WHERE unit_id = %s 
                        AND event_type = 'received'
                        AND timestamp BETWEEN %s AND %s
                        AND transaction_id IS NULL
                        ORDER BY timestamp
                        LIMIT 1
                    """, (vehicle_unit_id, dispensation_time, dispensation_time + timedelta(minutes=30)))
                    
                    reception = cursor.fetchone()
                    
                    if reception:
                        reception_id, received_amount, reception_time = reception
                        
                        # Update the reception event with the transaction ID
                        cursor.execute("""
                            UPDATE fuel_management.fuel_events
                            SET transaction_id = %s
                            WHERE event_id = %s
                        """, (transaction_id, reception_id))
                        
                        # Calculate discrepancy
                        discrepancy = dispensed_amount - received_amount
                        discrepancy_percentage = (discrepancy / dispensed_amount * 100) if dispensed_amount > 0 else 0
                        
                        # Determine status based on discrepancy
                        status = 'completed'
                        if abs(discrepancy_percentage) > 5:  # 5% threshold
                            status = 'discrepancy'
                        
                        # Update transaction
                        cursor.execute("""
                            UPDATE fuel_management.transactions
                            SET dispensed_amount = %s,
                                received_amount = %s,
                                discrepancy = %s,
                                discrepancy_percentage = %s,
                                status = %s
                            WHERE transaction_id = %s
                        """, (
                            dispensed_amount,
                            received_amount,
                            discrepancy,
                            discrepancy_percentage,
                            status,
                            transaction_id
                        ))
                        
                        self.logger.info(f"Matched transaction {transaction_id}: Dispensed {dispensed_amount}, Received {received_amount}, Status: {status}")
                    else:
                        # Update transaction with just the dispensed amount
                        cursor.execute("""
                            UPDATE fuel_management.transactions
                            SET dispensed_amount = %s,
                                status = 'partial'
                            WHERE transaction_id = %s
                        """, (dispensed_amount, transaction_id))
                        
                        self.logger.info(f"Partially matched transaction {transaction_id}: Dispensed {dispensed_amount}, no reception event found")
                
                # Check if the transaction has been pending for too long
                elapsed_time = datetime.now() - timestamp
                if elapsed_time > timedelta(hours=1):
                    # Update status to timed out
                    cursor.execute("""
                        UPDATE fuel_management.transactions
                        SET status = 'timed_out'
                        WHERE transaction_id = %s
                        AND status = 'pending'
                    """, (transaction_id,))
                    
                    self.logger.info(f"Transaction {transaction_id} timed out")
            
            conn.commit()
        except Exception as e:
            self.logger.error(f"Error matching transactions: {str(e)}")
            raise

if __name__ == "__main__":
    # Example configuration
    config = {
        'database': {
            'host': 'localhost',
            'database': 'fuel_management',
            'user': 'fuel_admin',
            'password': 'your_password'
        },
        'geozone_proximity_meters': 50
    }
    
    # Create and run the processor
    processor = FuelDataProcessor(config)
    processor.process_new_data()