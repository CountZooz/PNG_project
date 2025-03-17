// Global variables
let countdown;
let currentDriver;
let currentVehicle;
let currentBowser;

// API URL
const API_BASE_URL = '/api';

// Initialize the app when the page loads
document.addEventListener('DOMContentLoaded', () => {
    // Load dropdown data
    loadDropdownData();
    
    // Set up event listeners
    document.getElementById('authenticate-btn').addEventListener('click', authenticate);
    document.getElementById('capture-photo-btn').addEventListener('click', capturePhoto);
    document.getElementById('remove-photo-btn').addEventListener('click', removePhoto);
    document.getElementById('cancel-btn').addEventListener('click', resetForm);
    document.getElementById('done-btn').addEventListener('click', resetForm);
    
    // Handle form submission
    document.getElementById('odometer-form').addEventListener('submit', (e) => {
        e.preventDefault();
        submitOdometerReading();
    });
});

// Load dropdown data (drivers, vehicles, bowsers)
function loadDropdownData() {
    // Load drivers
    fetch(`${API_BASE_URL}/drivers`)
        .then(response => response.json())
        .then(drivers => {
            const driverSelect = document.getElementById('driver-id');
            driverSelect.innerHTML = '<option value="">Select Driver</option>';
            
            drivers.forEach(driver => {
                const option = document.createElement('option');
                option.value = driver.driver_id;
                option.textContent = driver.name;
                driverSelect.appendChild(option);
            });
        })
        .catch(error => {
            console.error('Error loading drivers:', error);
        });
    
    // Load vehicles
    fetch(`${API_BASE_URL}/vehicles`)
        .then(response => response.json())
        .then(vehicles => {
            const vehicleSelect = document.getElementById('vehicle-id');
            vehicleSelect.innerHTML = '<option value="">Select Vehicle</option>';
            
            vehicles.forEach(vehicle => {
                const option = document.createElement('option');
                option.value = vehicle.vehicle_id;
                option.textContent = vehicle.name;
                option.dataset.registration = vehicle.registration || '';
                vehicleSelect.appendChild(option);
            });
        })
        .catch(error => {
            console.error('Error loading vehicles:', error);
        });
    
    // Load bowsers
    fetch(`${API_BASE_URL}/bowsers`)
        .then(response => response.json())
        .then(bowsers => {
            const bowserSelect = document.getElementById('bowser-id');
            bowserSelect.innerHTML = '<option value="">Select Bowser</option>';
            
            bowsers.forEach(bowser => {
                const option = document.createElement('option');
                option.value = bowser.bowser_id;
                option.textContent = bowser.name;
                bowserSelect.appendChild(option);
            });
        })
        .catch(error => {
            console.error('Error loading bowsers:', error);
        });
}

// Authenticate driver
function authenticate() {
    const driverId = document.getElementById('driver-id').value;
    const vehicleId = document.getElementById('vehicle-id').value;
    const bowserId = document.getElementById('bowser-id').value;
    
    if (!driverId || !vehicleId || !bowserId) {
        alert('Please select a driver, vehicle, and bowser.');
        return;
    }
    
    // In a real system, this would verify the iButton authentication
    // For this simulation, we'll just get the selected items
    
    // Get driver details
    fetch(`${API_BASE_URL}/drivers`)
        .then(response => response.json())
        .then(drivers => {
            currentDriver = drivers.find(d => d.driver_id === driverId);
            
            // Get vehicle details
            return fetch(`${API_BASE_URL}/vehicles`);
        })
        .then(response => response.json())
        .then(vehicles => {
            currentVehicle = vehicles.find(v => v.vehicle_id === vehicleId);
            
            // Get bowser details
            return fetch(`${API_BASE_URL}/bowsers`);
        })
        .then(response => response.json())
        .then(bowsers => {
            currentBowser = bowsers.find(b => b.bowser_id === bowserId);
            
            // If all found, show the odometer form
            if (currentDriver && currentVehicle && currentBowser) {
                document.getElementById('authentication-card').classList.add('d-none');
                document.getElementById('odometer-card').classList.remove('d-none');
                
                // Update UI with driver and vehicle info
                document.getElementById('driver-badge').textContent = 'Driver: ' + currentDriver.name;
                document.getElementById('vehicle-name').textContent = currentVehicle.name;
                document.getElementById('vehicle-registration').textContent = currentVehicle.registration || 'N/A';
                
                // Pre-fill last known odometer reading if available
                if (currentVehicle.odometer) {
                    document.getElementById('odometer-reading').value = currentVehicle.odometer;
                    document.getElementById('odometer-reading').min = currentVehicle.odometer;
                }
            } else {
                alert('Error retrieving details. Please try again.');
            }
        })
        .catch(error => {
            console.error('Error during authentication:', error);
            alert('Error during authentication. Please try again.');
        });
}

// Capture odometer photo (simulated)
function capturePhoto() {
    // In a real app, this would access the device camera
    // For this simulation, we'll just show a placeholder image
    
    const previewDiv = document.getElementById('photo-preview');
    const previewImage = document.getElementById('preview-image');
    const photoData = document.getElementById('photo-data');
    
    // Use a placeholder image
    previewImage.src = 'data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIyMDAiIGhlaWdodD0iMTAwIiB2aWV3Qm94PSIwIDAgMjAwIDEwMCI+PHJlY3Qgd2lkdGg9IjIwMCIgaGVpZ2h0PSIxMDAiIGZpbGw9IiNlZWUiLz48dGV4dCB4PSI1MCUiIHk9IjUwJSIgZm9udC1mYW1pbHk9IkFyaWFsLCBzYW5zLXNlcmlmIiBmb250LXNpemU9IjE4IiB0ZXh0LWFuY2hvcj0ibWlkZGxlIiBhbGlnbm1lbnQtYmFzZWxpbmU9Im1pZGRsZSIgZmlsbD0iIzg4OCI+T2RvbWV0ZXIgUGhvdG88L3RleHQ+PC9zdmc+';
    photoData.value = 'simulated-photo-data';
    
    // Show the preview
    previewDiv.classList.remove('d-none');
}

// Remove the captured photo
function removePhoto() {
    const previewDiv = document.getElementById('photo-preview');
    const photoData = document.getElementById('photo-data');
    
    previewDiv.classList.add('d-none');
    photoData.value = '';
}

// Submit odometer reading
function submitOdometerReading() {
    const odometerReading = parseFloat(document.getElementById('odometer-reading').value);
    const photoData = document.getElementById('photo-data').value;
    
    // Validate odometer reading
    if (isNaN(odometerReading) || odometerReading <= 0) {
        alert('Please enter a valid odometer reading.');
        return;
    }
    
    // Check if reading is less than previous
    if (currentVehicle.odometer && odometerReading < currentVehicle.odometer) {
        if (!confirm(`The entered reading (${odometerReading} km) is less than the previous reading (${currentVehicle.odometer} km). Continue anyway?`)) {
            return;
        }
    }
    
    // In a real system, this would send the data to the API to create a transaction
    // For this simulation, we'll just show the confirmation
    
    document.getElementById('odometer-card').classList.add('d-none');
    document.getElementById('confirmation-card').classList.remove('d-none');
    
    // Generate a fake transaction ID
    const transactionId = 'SIM-' + Math.floor(Math.random() * 1000000).toString().padStart(6, '0');
    document.getElementById('transaction-id').textContent = transactionId;
    
    // Start countdown
    let secondsLeft = 30;
    const countdownElement = document.getElementById('countdown');
    
    clearInterval(countdown);
    countdown = setInterval(() => {
        secondsLeft--;
        countdownElement.textContent = secondsLeft;
        
        if (secondsLeft <= 0) {
            clearInterval(countdown);
            resetForm();
        }
    }, 1000);
}

// Reset the form and go back to authentication
function resetForm() {
    // Clear countdown if active
    clearInterval(countdown);
    
    // Hide all cards except authentication
    document.getElementById('authentication-card').classList.remove('d-none');
    document.getElementById('odometer-card').classList.add('d-none');
    document.getElementById('confirmation-card').classList.add('d-none');
    
    // Reset form fields
    document.getElementById('driver-id').value = '';
    document.getElementById('vehicle-id').value = '';
    document.getElementById('bowser-id').value = '';
    document.getElementById('odometer-reading').value = '';
    
    // Clear photo
    removePhoto();
    
    // Clear current selections
    currentDriver = null;
    currentVehicle = null;
    currentBowser = null;
}