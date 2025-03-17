import argparse
import time
import json
import logging
import requests
from datetime import datetime, timedelta
from simulation.data_generator import WialonSimulator

class SimulationController:
    def __init__(self, db_config, api_url='http://localhost:8080'):
        """Initialize the simulation controller"""
        self.db_config = db_config
        self.api_url = api_url
        self.simulator = WialonSimulator(db_config)
        self.logger = self._setup_logger()
        
    def _setup_logger(self):
        """Set up logging for the simulator controller"""
        logger = logging.getLogger('simulation_controller')
        logger.setLevel(logging.INFO)
        handler = logging.FileHandler('simulation_controller.log')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger
    
    def initialize(self):
        """Initialize the simulation environment"""
        # Initialize the database with sample data
        self.logger.info("Initializing database with sample data...")
        self.simulator.initialize_database()
        
        # Update the mock API with simulated data
        self._update_mock_api()
        
        self.logger.info("Simulation environment initialized")
        
    def _update_mock_api(self):
        """Update the mock API with current simulation data"""
        try:
            # Create simulated data structure expected by the mock API
            data = {
                'units': {
                    'items': [
                        {
                            'id': vehicle['wialon_id'],
                            'nm': vehicle['name'],
                            'mu': 0,  # Measurement units
                            'cls': 2,  # Class: 2 for vehicle
                            'uacl': 1  # User access level
                        }
                        for vehicle in self.simulator.vehicles
                    ] + [
                        {
                            'id': bowser['wialon_id'],
                            'nm': bowser['name'],
                            'mu': 0,  # Measurement units
                            'cls': 2,  # Class: 2 for vehicle
                            'uacl': 1  # User access level
                        }
                        for bowser in self.simulator.bowsers
                    ]
                },
                'sensors': {
                    str(vehicle['wialon_id']): {
                        'sensors': [
                            {
                                'id': 40000 + int(vehicle['id'][1:]),
                                'n': 'Fuel Level Sensor',
                                't': 2,  # Analog
                                'p': 'fuel_level',
                                'm': vehicle['current_fuel']
                            }
                        ]
                    }
                    for vehicle in self.simulator.vehicles
                },
                'last_messages': {
                    str(vehicle['wialon_id']): {
                        't': int(datetime.now().timestamp()),
                        'pos': {
                            'x': vehicle['lon'],
                            'y': vehicle['lat'],
                            's': 0,  # Speed
                            'c': 0,  # Course
                            'sc': 0  # Satellites count
                        },
                        'p': {
                            'sensors': {
                                str(40000 + int(vehicle['id'][1:])): vehicle['current_fuel']
                            }
                        }
                    }
                    for vehicle in self.simulator.vehicles
                },
                'geozones': {
                    'items': [
                        {
                            'id': bowser['geozone_id'],
                            'n': f"Bowser {bowser['id']} Zone",
                            't': 1,  # Circle
                            'p': {
                                'c': {
                                    'x': bowser['lon'],
                                    'y': bowser['lat']
                                },
                                'r': bowser['geozone_radius']
                            }
                        }
                        for bowser in self.simulator.bowsers
                    ]
                }
            }
            
            # Send the data to the mock API
            response = requests.post(
                f"{self.api_url}/update_simulated_data",
                json=data
            )
            
            if response.status_code == 200:
                self.logger.info("Mock API updated with simulated data")
            else:
                self.logger.error(f"Failed to update mock API: {response.status_code} - {response.text}")
                
        except Exception as e:
            self.logger.error(f"Error updating mock API: {str(e)}")
    
    def run_simulation(self, num_transactions=10, interval=5):
        """Run a simulation with multiple transactions"""
        self.logger.info(f"Starting simulation with {num_transactions} transactions, interval: {interval} seconds")
        
        for i in range(num_transactions):
            try:
                # Simulate a transaction
                transaction = self.simulator.simulate_fuel_transaction()
                self.logger.info(f"Transaction {i+1}/{num_transactions}: {transaction['vehicle']} received {transaction['received_amount']} liters")
                
                # Update the mock API with current state
                self._update_mock_api()
                
                # Wait between transactions
                if i < num_transactions - 1:  # Don't wait after the last one
                    time.sleep(interval)
                    
            except Exception as e:
                self.logger.error(f"Error in transaction {i+1}: {str(e)}")
        
        self.logger.info(f"Simulation completed: {num_transactions} transactions")
        
    def run_continuous_simulation(self, transactions_per_hour=5, duration_hours=24):
        """Run a continuous simulation for a specified duration"""
        self.logger.info(f"Starting continuous simulation: {transactions_per_hour}/hour for {duration_hours} hours")
        
        interval = 3600 / transactions_per_hour  # Seconds between transactions
        total_transactions = int(transactions_per_hour * duration_hours)
        
        self.run_simulation(num_transactions=total_transactions, interval=interval)

def main():
    """Main entry point for the simulation controller"""
    parser = argparse.ArgumentParser(description='Fuel Management System Simulation Controller')
    parser.add_argument('--host', default='localhost', help='Database host')
    parser.add_argument('--database', default='fuel_management', help='Database name')
    parser.add_argument('--user', default='fuel_admin', help='Database user')
    parser.add_argument('--password', default='your_password', help='Database password')
    parser.add_argument('--api-url', default='http://localhost:8080', help='Mock API URL')
    parser.add_argument('--transactions', type=int, default=10, help='Number of transactions to simulate')
    parser.add_argument('--interval', type=int, default=5, help='Interval between transactions (seconds)')
    parser.add_argument('--continuous', action='store_true', help='Run in continuous mode')
    parser.add_argument('--tph', type=int, default=5, help='Transactions per hour (for continuous mode)')
    parser.add_argument('--duration', type=int, default=24, help='Duration in hours (for continuous mode)')
    
    args = parser.parse_args()
    
    # Set up database configuration
    db_config = {
        'host': args.host,
        'database': args.database,
        'user': args.user,
        'password': args.password
    }
    
    # Create and initialize the controller
    controller = SimulationController(db_config, api_url=args.api_url)
    controller.initialize()
    
    # Run the simulation
    if args.continuous:
        controller.run_continuous_simulation(args.tph, args.duration)
    else:
        controller.run_simulation(args.transactions, args.interval)

if __name__ == '__main__':
    main()