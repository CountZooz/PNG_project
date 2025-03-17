"""
Configuration settings for the Wialon integration
"""

# Wialon API configuration
WIALON_CONFIG = {
    # For production
    'production': {
        'base_url': 'https://hst-api.wialon.com',
        'token': 'your_wialon_token_here',  # Replace with your actual token
    },
    # For development and testing with mock server
    'development': {
        'base_url': 'http://localhost:8080',
        'token': 'test_token',
    }
}

# Database configuration
DATABASE_CONFIG = {
    # For production
    'production': {
        'host': 'your_production_db_host',
        'database': 'fuel_management',
        'user': 'your_db_user',
        'password': 'your_db_password',
    },
    # For local development
    'development': {
        'host': 'localhost',
        'database': 'fuel_management',
        'user': 'fuel_admin',
        'password': 'your_password',
    }
}

# Data collection configuration
COLLECTOR_CONFIG = {
    # For production
    'production': {
        'enable_hourly_polling': True,
        'poll_interval_minutes': 60,  # 1 hour
    },
    # For development
    'development': {
        'enable_hourly_polling': True,
        'poll_interval_minutes': 15,  # 15 minutes for faster testing
    }
}

# Get the complete configuration for a specific environment
def get_config(environment='development'):
    """Get the complete configuration for a specific environment"""
    return {
        'wialon': WIALON_CONFIG.get(environment, WIALON_CONFIG['development']),
        'database': DATABASE_CONFIG.get(environment, DATABASE_CONFIG['development']),
        'enable_hourly_polling': COLLECTOR_CONFIG.get(environment, COLLECTOR_CONFIG['development'])['enable_hourly_polling'],
        'poll_interval_minutes': COLLECTOR_CONFIG.get(environment, COLLECTOR_CONFIG['development'])['poll_interval_minutes'],
    }