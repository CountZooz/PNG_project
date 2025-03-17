import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from simulation.data_generator import WialonSimulator

# Database config
db_config = {
    'host': 'localhost',
    'database': 'fuel_management',
    'user': 'fuel_admin',
    'password': 'password123'
}

# Initialize simulator
simulator = WialonSimulator(db_config)

# Initialize database with sample data
print("Initializing database with sample data...")
simulator.initialize_database()
print("Database initialized.")

# Simulate some transactions
print("Simulating transactions...")
for i in range(10):
    transaction = simulator.simulate_fuel_transaction()
    print(f"Simulated transaction: {transaction['transaction_id']}")
print("Simulation complete.")
