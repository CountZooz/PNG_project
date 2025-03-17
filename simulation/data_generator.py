import random
import time
import json
import psycopg2
from datetime import datetime, timedelta
import uuid
import logging

class WialonSimulator:
    def __init__(self, db_config):
        """Initialize the simulator with database configuration"""
        self.db_config = db_config
        self.vehicles = self._create_sample_vehicles(10)  # 10 sample vehicles
        self.bowsers = self._create_sample_bowsers(3)     # 3 sample bowsers
        self.drivers = self._create_sample_drivers(15)    # 15 sample drivers
        self.logger = self._setup_logger()
        
    def _setup_logger(self):
        """Set up logging for the simulator"""
        logger = logging.getLogger('wialon_simulator')
        logger.setLevel(logging.INFO)
        handler = logging.FileHandler('simulation.log')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger
        
    def _get_db_connection(self):
        """Create a connection to the PostgreSQL database"""
        return psycopg2.connect(
            host=self.db_config['host'],
            database=self.db_config['database'],
            user=self.db_config['user'],
            password=self.db_config['password']
        )
    
    def _create_sample_vehicles(self, count):
        """Create sample vehicle data"""
        vehicle_types = [
            "ISUZU FVR flat top", "HINO 300 FLAT TOP", "CAMC 6x4 RIGID",
            "TOYOTA HILUX", "CAMC PRIME MOVER 6x4", "Nissan Urvan"
        ]
        
        vehicles = []
        for i in range(count):
            vehicle_type = random.choice(vehicle_types)
            vehicle = {
                "id": f"V{i+1}",
                "wialon_id": 10000 + i,
                "name": f"EFM {100+i} POM - {vehicle_type} (BGX {100+i})",
                "registration": f"BGX{100+i}",
                "fuel_capacity": random.randint(100, 400),
                "current_fuel": random.randint(50, 350),
                "odometer": random.randint(10000, 100000),
                "lat": -9.4438 + (random.random() - 0.5) * 0.1,  # Centered around Port Moresby
                "lon": 147.1803 + (random.random() - 0.5) * 0.1,
                "standard_burn_rate": random.choice([1.5, 2.5, 6.0, 8.5])
            }
            vehicles.append(vehicle)
        
        return vehicles
    
    def _create_sample_bowsers(self, count):
        """Create sample bowser data"""
        bowsers = []
        for i in range(count):
            bowser = {
                "id": f"B{i+1}",
                "wialon_id": 20000 + i,
                "name": f"Fuel Bowser {i+1}",
                "capacity": 20000,
                "current_level": random.randint(5000, 15000),
                "total_dispensed": random.randint(50000, 100000),
                "lat": -9.4438 + (random.random() - 0.5) * 0.05,  # Centered around Port Moresby
                "lon": 147.1803 + (random.random() - 0.5) * 0.05,
                "geozone_id": 3000 + i,
                "geozone_radius": 50  # meters
            }
            bowsers.append(bowser)
        
        return bowsers
    
    def _create_sample_drivers(self, count):
        """Create sample driver data"""
        driver_types = ["Semi-trailer Driver", "Rigid Truck Driver", "Workshop Specialist"]
        first_names = ["John", "Mary", "Peter", "David", "Moses", "Ruth", "Sarah", "Michael", "Joseph", "Patrick"]
        last_names = ["Koitau", "Aua", "Arua", "Garden", "Ray", "Hitolo", "Karah", "Mea", "Sale", "Wame"]
        
        drivers = []
        for i in range(count):
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)
            driver = {
                "id": f"D{i+1}",
                "name": f"{first_name} {last_name}",
                "role": random.choice(driver_types),
                "ibutton_id": f"0000{i+1:04d}"
            }
            drivers.append(driver)
        
        return drivers
    
    def initialize_database(self):
        """Initialize database with sample data"""
        conn = self._get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Store vehicles
            for vehicle in self.vehicles:
                cursor.execute("""
                    INSERT INTO wialon_data.units 
                    (unit_id, name, type, custom_fields)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (unit_id) DO UPDATE
                    SET name = EXCLUDED.name,
                        type = EXCLUDED.type,
                        custom_fields = EXCLUDED.custom_fields
                """, (
                    vehicle["wialon_id"], 
                    vehicle["name"], 
                    "truck",
                    json.dumps({
                        "registration": vehicle["registration"],
                        "fuel_capacity": vehicle["fuel_capacity"],
                        "standard_burn_rate": vehicle["standard_burn_rate"]
                    })
                ))
                
                # Add to vehicles table
                cursor.execute("""
                    INSERT INTO fuel_management.vehicles
                    (vehicle_id, wialon_unit_id, name, registration, fuel_capacity, standard_burn_rate)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (vehicle_id) DO UPDATE
                    SET wialon_unit_id = EXCLUDED.wialon_unit_id,
                        name = EXCLUDED.name,
                        registration = EXCLUDED.registration,
                        fuel_capacity = EXCLUDED.fuel_capacity,
                        standard_burn_rate = EXCLUDED.standard_burn_rate
                """, (
                    vehicle["id"],
                    vehicle["wialon_id"],
                    vehicle["name"],
                    vehicle["registration"],
                    vehicle["fuel_capacity"],
                    vehicle["standard_burn_rate"]
                ))
                
                # Add initial vehicle status
                cursor.execute("""
                    INSERT INTO fuel_management.vehicle_status
                    (unit_id, fuel_level, odometer, latitude, longitude, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (unit_id) DO UPDATE
                    SET fuel_level = EXCLUDED.fuel_level,
                        odometer = EXCLUDED.odometer,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        timestamp = EXCLUDED.timestamp
                """, (
                    vehicle["wialon_id"],
                    vehicle["current_fuel"],
                    vehicle["odometer"],
                    vehicle["lat"],
                    vehicle["lon"],
                    datetime.now()
                ))
            
            # Store bowsers
            for bowser in self.bowsers:
                cursor.execute("""
                    INSERT INTO wialon_data.units 
                    (unit_id, name, type, custom_fields)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (unit_id) DO UPDATE
                    SET name = EXCLUDED.name,
                        type = EXCLUDED.type,
                        custom_fields = EXCLUDED.custom_fields
                """, (
                    bowser["wialon_id"], 
                    bowser["name"], 
                    "bowser",
                    json.dumps({
                        "capacity": bowser["capacity"]
                    })
                ))
                
                # Add to bowsers table
                cursor.execute("""
                    INSERT INTO fuel_management.bowsers
                    (bowser_id, wialon_unit_id, name, capacity, critical_level, geozone_id)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (bowser_id) DO UPDATE
                    SET wialon_unit_id = EXCLUDED.wialon_unit_id,
                        name = EXCLUDED.name,
                        capacity = EXCLUDED.capacity,
                        critical_level = EXCLUDED.critical_level,
                        geozone_id = EXCLUDED.geozone_id
                """, (
                    bowser["id"],
                    bowser["wialon_id"],
                    bowser["name"],
                    bowser["capacity"],
                    bowser["capacity"] * 0.2,  # Set critical level at 20% of capacity
                    bowser["geozone_id"]
                ))
                
                # Add initial bowser status
                cursor.execute("""
                    INSERT INTO fuel_management.bowser_status
                    (unit_id, fuel_level, total_dispensed, latitude, longitude, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (unit_id) DO UPDATE
                    SET fuel_level = EXCLUDED.fuel_level,
                        total_dispensed = EXCLUDED.total_dispensed,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        timestamp = EXCLUDED.timestamp
                """, (
                    bowser["wialon_id"],
                    bowser["current_level"],
                    bowser["total_dispensed"],
                    bowser["lat"],
                    bowser["lon"],
                    datetime.now()
                ))
                
                # Add geozone
                cursor.execute("""
                    INSERT INTO wialon_data.geozones
                    (geozone_id, resource_id, name, type, center_latitude, center_longitude, radius)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (geozone_id) DO UPDATE
                    SET name = EXCLUDED.name,
                        center_latitude = EXCLUDED.center_latitude,
                        center_longitude = EXCLUDED.center_longitude,
                        radius = EXCLUDED.radius
                """, (
                    bowser["geozone_id"],
                    1,  # Default resource ID
                    f"Bowser {bowser['id']} Zone",
                    1,  # Circle type
                    bowser["lat"],
                    bowser["lon"],
                    bowser["geozone_radius"]
                ))
            
            # Store drivers
            for driver in self.drivers:
                cursor.execute("""
                    INSERT INTO fuel_management.drivers
                    (driver_id, name, role, ibutton_code)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (driver_id) DO UPDATE
                    SET name = EXCLUDED.name,
                        role = EXCLUDED.role,
                        ibutton_code = EXCLUDED.ibutton_code
                """, (
                    driver["id"],
                    driver["name"],
                    driver["role"],
                    driver["ibutton_id"]
                ))
            
            conn.commit()
            self.logger.info("Database initialized with sample data")
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Error initializing database: {str(e)}")
        finally:
            cursor.close()
            conn.close()
    
    def simulate_fuel_transaction(self):
        """Simulate a complete fuel transaction"""
        # 1. Choose random vehicle and driver
        vehicle = random.choice(self.vehicles)
        driver = random.choice(self.drivers)
        
        # 2. Choose random bowser
        bowser = random.choice(self.bowsers)
        
        # 3. Generate event timestamps
        current_time = datetime.now()
        arrival_time = current_time - timedelta(minutes=random.randint(5, 30))
        auth_time = arrival_time + timedelta(minutes=1)
        dispensing_start = auth_time + timedelta(minutes=1)
        dispensing_end = dispensing_start + timedelta(minutes=random.randint(3, 10))
        departure_time = dispensing_end + timedelta(minutes=1)
        
        # 4. Generate transaction data
        fuel_amount = random.randint(50, 200)
        
        # Small random discrepancy between dispensed and received
        discrepancy_factor = random.uniform(0.97, 1.03)
        received_amount = round(fuel_amount * discrepancy_factor, 2)
        
        # 5. Generate distance data
        trip_distance = random.randint(50, 500)
        odo_before, odo_after, odo_distance = self.simulate_odometer_readings(vehicle, trip_distance)
        
        # 6. Store events in database
        transaction_id = str(uuid.uuid4())
        self._store_simulated_transaction(
            transaction_id, vehicle, driver, bowser,
            arrival_time, auth_time, dispensing_start, dispensing_end, departure_time,
            fuel_amount, received_amount, odo_before, odo_after
        )
        
        self.logger.info(f"Simulated transaction {transaction_id}: {vehicle['name']} received {received_amount} liters")
        
        return {
            'transaction_id': transaction_id,
            'vehicle': vehicle["name"],
            'driver': driver["name"],
            'bowser': bowser["name"],
            'timestamp': auth_time,
            'fuel_amount': fuel_amount,
            'received_amount': received_amount,
            'odometer_before': odo_before,
            'odometer_after': odo_after
        }
    
    def simulate_odometer_readings(self, vehicle, distance):
        """Generate realistic odometer readings based on real-world patterns"""
        # Add some realistic variance between GPS and odometer
        variance_factor = random.uniform(0.95, 1.07)  # Odometer typically reads 5% under to 7% over GPS
        
        # Calculate odometer distance with variance
        odo_distance = distance * variance_factor
        
        # Update vehicle's simulated odometer
        odo_before = vehicle["odometer"]
        odo_after = odo_before + odo_distance
        vehicle["odometer"] = odo_after
        
        return odo_before, odo_after, odo_distance
    
    def _store_simulated_transaction(self, transaction_id, vehicle, driver, bowser,
                                    arrival_time, auth_time, dispensing_start, 
                                    dispensing_end, departure_time,
                                    fuel_amount, received_amount, odo_before, odo_after):
        """Store a simulated transaction in the database"""
        conn = self._get_db_connection()
        cursor = conn.cursor()
        
        try:
            # 1. Store geozone entry event
            cursor.execute("""
                INSERT INTO wialon_data.geozone_events
                (unit_id, geozone_id, event_type, timestamp, latitude, longitude)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                vehicle["wialon_id"],
                bowser["geozone_id"],
                "enter",
                arrival_time,
                bowser["lat"] + (random.random() - 0.5) * 0.001,
                bowser["lon"] + (random.random() - 0.5) * 0.001
            ))
            
            # 2. Store iButton authentication event
            cursor.execute("""
                INSERT INTO wialon_data.ibutton_events
                (unit_id, ibutton_code, timestamp, latitude, longitude)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                vehicle["wialon_id"],
                driver["ibutton_id"],
                auth_time,
                bowser["lat"] + (random.random() - 0.5) * 0.001,
                bowser["lon"] + (random.random() - 0.5) * 0.001
            ))
            
            # 3. Store fuel dispensation sensor readings (flow meter)
            cursor.execute("""
                INSERT INTO wialon_data.sensor_readings
                (unit_id, unit_name, sensor_id, sensor_name, sensor_type, 
                 value, timestamp, latitude, longitude)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                bowser["wialon_id"],
                bowser["name"],
                30000 + int(bowser["id"][1:]),
                "Fuel Flow Meter",
                2,  # Analog sensor
                bowser["total_dispensed"] + fuel_amount,
                dispensing_end,
                bowser["lat"],
                bowser["lon"]
            ))
            
            # 4. Store vehicle fuel level sensor readings (before)
            cursor.execute("""
                INSERT INTO wialon_data.sensor_readings
                (unit_id, unit_name, sensor_id, sensor_name, sensor_type, 
                 value, timestamp, latitude, longitude)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                vehicle["wialon_id"],
                vehicle["name"],
                40000 + int(vehicle["id"][1:]),
                "Fuel Level Sensor",
                2,  # Analog sensor
                vehicle["current_fuel"],
                dispensing_start,
                bowser["lat"] + (random.random() - 0.5) * 0.001,
                bowser["lon"] + (random.random() - 0.5) * 0.001
            ))
            
            # 5. Store vehicle fuel level sensor readings (after)
            cursor.execute("""
                INSERT INTO wialon_data.sensor_readings
                (unit_id, unit_name, sensor_id, sensor_name, sensor_type, 
                 value, timestamp, latitude, longitude)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                vehicle["wialon_id"],
                vehicle["name"],
                40000 + int(vehicle["id"][1:]),
                "Fuel Level Sensor",
                2,  # Analog sensor
                vehicle["current_fuel"] + received_amount,
                dispensing_end + timedelta(seconds=30),
                bowser["lat"] + (random.random() - 0.5) * 0.001,
                bowser["lon"] + (random.random() - 0.5) * 0.001
            ))
            
            # 6. Store odometer readings
            cursor.execute("""
                INSERT INTO fuel_management.odometer_readings
                (vehicle_id, reading, timestamp, input_method, verified, transaction_id)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                vehicle["id"],
                odo_before,
                auth_time,
                "simulated",
                True,
                transaction_id
            ))
            
            cursor.execute("""
                INSERT INTO fuel_management.odometer_readings
                (vehicle_id, reading, timestamp, input_method, verified, transaction_id)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                vehicle["id"],
                odo_after,
                departure_time,
                "simulated",
                True,
                transaction_id
            ))
            
            # 7. Store geozone exit event
            cursor.execute("""
                INSERT INTO wialon_data.geozone_events
                (unit_id, geozone_id, event_type, timestamp, latitude, longitude)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                vehicle["wialon_id"],
                bowser["geozone_id"],
                "exit",
                departure_time,
                bowser["lat"] + (random.random() - 0.5) * 0.002,
                bowser["lon"] + (random.random() - 0.5) * 0.002
            ))
            
            # 8. Store transaction record
            odo_distance = odo_after - odo_before
            gps_distance = odo_distance / random.uniform(0.95, 1.05)  # Simulate GPS distance
            discrepancy = fuel_amount - received_amount
            discrepancy_percentage = (discrepancy / fuel_amount * 100) if fuel_amount > 0 else 0
            distance_discrepancy = abs((odo_distance - gps_distance) / gps_distance * 100) if gps_distance > 0 else 0
            
            cursor.execute("""
                INSERT INTO fuel_management.transactions
                (transaction_id, driver_id, vehicle_id, bowser_id, timestamp,
                 dispensed_amount, received_amount, discrepancy, discrepancy_percentage,
                 odometer_before, odometer_after, odo_distance, gps_distance,
                 distance_discrepancy_percent, trip_start, trip_end, trip_duration,
                 status, verification_method)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                transaction_id,
                driver["id"],
                vehicle["id"],
                bowser["id"],
                auth_time,
                fuel_amount,
                received_amount,
                discrepancy,
                discrepancy_percentage,
                odo_before,
                odo_after,
                odo_distance,
                gps_distance,
                distance_discrepancy,
                arrival_time,
                departure_time,
                int((departure_time - arrival_time).total_seconds()),
                "completed",
                "simulated"
            ))
            
            # 9. Update vehicle and bowser status
            # Update vehicle fuel level
            vehicle["current_fuel"] += received_amount
            cursor.execute("""
                UPDATE fuel_management.vehicle_status
                SET fuel_level = %s,
                    odometer = %s,
                    latitude = %s,
                    longitude = %s,
                    timestamp = %s
                WHERE unit_id = %s
            """, (
                vehicle["current_fuel"],
                odo_after,
                bowser["lat"] + (random.random() - 0.5) * 0.002,
                bowser["lon"] + (random.random() - 0.5) * 0.002,
                departure_time,
                vehicle["wialon_id"]
            ))
            
            # Update bowser fuel level and dispensed total
            bowser["current_level"] -= fuel_amount
            bowser["total_dispensed"] += fuel_amount
            cursor.execute("""
                UPDATE fuel_management.bowser_status
                SET fuel_level = %s,
                    total_dispensed = %s,
                    timestamp = %s
                WHERE unit_id = %s
            """, (
                bowser["current_level"],
                bowser["total_dispensed"],
                dispensing_end,
                bowser["wialon_id"]
            ))
            
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Error storing simulated transaction: {str(e)}")
        finally:
            cursor.close()
            conn.close()
            
    def run_simulation(self, num_transactions=10, time_period=None):
        """Run a simulation with multiple transactions"""
        print(f"Starting simulation with {num_transactions} transactions")
        
        for i in range(num_transactions):
            transaction = self.simulate_fuel_transaction()
            print(f"Simulated transaction {i+1}: {transaction['vehicle']} received {transaction['received_amount']} liters")
            
            # Wait a bit between transactions
            time.sleep(random.uniform(0.5, 2.0))