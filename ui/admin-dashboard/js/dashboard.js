// Global variables
let currentPage = 'dashboard';
let transactionPage = 1;
let transactionLimit = 10;
let transactionsTotal = 0;
let currentFilters = {};

// Initialize date range pickers with default values (last 7 days)
const now = new Date();
const oneWeekAgo = new Date();
oneWeekAgo.setDate(now.getDate() - 7);

// DOM elements
const contentSections = document.querySelectorAll('.content-section');
const navLinks = document.querySelectorAll('.nav-link');
const pageTitle = document.getElementById('page-title');

// API URL
const API_BASE_URL = '/api';

// Debug logging function
function logDebug(message, data = null) {
    console.log(`[DEBUG] ${message}`, data || '');
}

// Initialize the dashboard when the page loads
document.addEventListener('DOMContentLoaded', () => {
    logDebug('Dashboard initialization starting...');
    
    // Add initial debug logging to check data loading
    logDebug('Fetching initial dashboard data...');
    fetch(`${API_BASE_URL}/dashboard/summary`)
        .then(response => {
            logDebug('Dashboard data response status:', response.status);
            return response.json();
        })
        .then(data => {
            logDebug('Dashboard data loaded successfully:', data);
        })
        .catch(error => {
            console.error('Error fetching dashboard data:', error);
        });
    
    // Set date pickers
    try {
        document.getElementById('date-start').valueAsDate = oneWeekAgo;
        document.getElementById('date-end').valueAsDate = now;
        logDebug('Date pickers initialized');
    } catch (e) {
        console.error('Error setting date values:', e);
    }
    
    // Set up navigation
    setupNavigation();
    
    // Load initial data
    loadDashboardData();
    loadDropdownData();
    
    // Set up event listeners
    try {
        document.getElementById('refresh-data-btn').addEventListener('click', refreshData);
        document.getElementById('simulate-transaction-btn').addEventListener('click', simulateTransaction);
        document.getElementById('apply-date-range').addEventListener('click', () => {
            loadDashboardData();
        });
        document.getElementById('apply-filters').addEventListener('click', () => {
            transactionPage = 1;
            loadTransactions();
        });
        document.getElementById('prev-page').addEventListener('click', () => {
            if (transactionPage > 1) {
                transactionPage--;
                loadTransactions();
            }
        });
        document.getElementById('next-page').addEventListener('click', () => {
            if (transactionPage * transactionLimit < transactionsTotal) {
                transactionPage++;
                loadTransactions();
            }
        });
        logDebug('Event listeners set up');
    } catch (e) {
        console.error('Error setting up event listeners:', e);
    }
    
    logDebug('Dashboard initialization complete');
});

// Set up navigation links
function setupNavigation() {
    logDebug('Setting up navigation');
    navLinks.forEach(link => {
        link.addEventListener('click', (e) => {
            e.preventDefault();
            
            // Get the target section ID from the link ID
            const targetId = link.id.replace('nav-', '');
            logDebug(`Navigation clicked: ${targetId}`);
            
            // Update active nav link
            navLinks.forEach(navLink => navLink.classList.remove('active'));
            link.classList.add('active');
            
            // Show the corresponding content section
            contentSections.forEach(section => {
                section.classList.add('d-none');
            });
            const targetSection = document.getElementById(`content-${targetId}`);
            if (targetSection) {
                targetSection.classList.remove('d-none');
                logDebug(`Displayed section: content-${targetId}`);
            } else {
                console.error(`Target section content-${targetId} not found!`);
            }
            
            // Update page title
            pageTitle.textContent = targetId.charAt(0).toUpperCase() + targetId.slice(1);
            
            // Update current page
            currentPage = targetId;
            
            // Load section-specific data
            if (targetId === 'dashboard') {
                loadDashboardData();
            } else if (targetId === 'transactions') {
                loadTransactions();
            } else if (targetId === 'vehicles') {
                loadVehicles();
            } else if (targetId === 'bowsers') {
                loadBowsers();
            } else if (targetId === 'drivers') {
                loadDrivers();
            }
        });
    });
}

// Load data for dashboard
function loadDashboardData() {
    logDebug('Loading dashboard data');
    const startDate = document.getElementById('date-start').value;
    const endDate = document.getElementById('date-end').value;
    
    logDebug(`Date range: ${startDate} to ${endDate}`);
    
    fetch(`${API_BASE_URL}/dashboard/summary?start_date=${startDate}&end_date=${endDate}`)
        .then(response => {
            if (!response.ok) {
                throw new Error(`API responded with status ${response.status}`);
            }
            return response.json();
        })
        .then(data => {
            logDebug('Dashboard data received', data);
            
            // Update summary cards
            try {
                document.getElementById('total-fuel-dispensed').textContent = formatNumber(data.transaction_summary.total_fuel_dispensed || 0);
                document.getElementById('total-transactions').textContent = formatNumber(data.transaction_summary.completed_transactions || 0);
                document.getElementById('discrepancy-transactions').textContent = formatNumber(data.transaction_summary.discrepancy_transactions || 0);
                
                const lowFuelVehicles = data.vehicle_summary.low_fuel_vehicles || 0;
                const lowFuelBowsers = data.bowser_summary.low_fuel_bowsers || 0;
                document.getElementById('low-fuel-alerts').textContent = formatNumber(lowFuelVehicles + lowFuelBowsers);
                logDebug('Summary cards updated');
            } catch (e) {
                console.error('Error updating summary cards:', e);
            }
            
            // Update recent transactions
            try {
                updateRecentTransactionsTable(data.recent_transactions);
                logDebug('Recent transactions updated');
            } catch (e) {
                console.error('Error updating recent transactions:', e);
            }
        })
        .catch(error => {
            console.error('Error loading dashboard data:', error);
        });
}

// Load dropdown data (vehicles, drivers, bowsers)
function loadDropdownData() {
    logDebug('Loading dropdown data');
    
    // Load vehicles for dropdown
    fetch(`${API_BASE_URL}/vehicles`)
        .then(response => response.json())
        .then(vehicles => {
            logDebug(`Loaded ${vehicles.length} vehicles`);
            const vehicleSelect = document.getElementById('filter-vehicle');
            if (vehicleSelect) {
                vehicleSelect.innerHTML = '<option value="">All Vehicles</option>';
                
                vehicles.forEach(vehicle => {
                    const option = document.createElement('option');
                    option.value = vehicle.vehicle_id;
                    option.textContent = vehicle.name;
                    vehicleSelect.appendChild(option);
                });
            } else {
                console.error('Vehicle select element not found');
            }
        })
        .catch(error => {
            console.error('Error loading vehicles:', error);
        });
    
    // Load drivers for dropdown
    fetch(`${API_BASE_URL}/drivers`)
        .then(response => response.json())
        .then(drivers => {
            logDebug(`Loaded ${drivers.length} drivers`);
            const driverSelect = document.getElementById('filter-driver');
            if (driverSelect) {
                driverSelect.innerHTML = '<option value="">All Drivers</option>';
                
                drivers.forEach(driver => {
                    const option = document.createElement('option');
                    option.value = driver.driver_id;
                    option.textContent = driver.name;
                    driverSelect.appendChild(option);
                });
            } else {
                console.error('Driver select element not found');
            }
        })
        .catch(error => {
            console.error('Error loading drivers:', error);
        });
    
    // Load bowsers for dropdown
    fetch(`${API_BASE_URL}/bowsers`)
        .then(response => response.json())
        .then(bowsers => {
            logDebug(`Loaded ${bowsers.length} bowsers`);
            const bowserSelect = document.getElementById('filter-bowser');
            if (bowserSelect) {
                bowserSelect.innerHTML = '<option value="">All Bowsers</option>';
                
                bowsers.forEach(bowser => {
                    const option = document.createElement('option');
                    option.value = bowser.bowser_id;
                    option.textContent = bowser.name;
                    bowserSelect.appendChild(option);
                });
            } else {
                console.error('Bowser select element not found');
            }
        })
        .catch(error => {
            console.error('Error loading bowsers:', error);
        });
}

// Load transactions with filtering and pagination
function loadTransactions() {
    logDebug('Loading transactions');
    // Build query parameters
    const params = new URLSearchParams();
    params.append('limit', transactionLimit);
    params.append('offset', (transactionPage - 1) * transactionLimit);
    
    // Add date range
    const startDate = document.getElementById('date-start').value;
    const endDate = document.getElementById('date-end').value;
    if (startDate) params.append('start_date', startDate);
    if (endDate) params.append('end_date', endDate);
    
    // Add filters
    const vehicleId = document.getElementById('filter-vehicle')?.value;
    const driverId = document.getElementById('filter-driver')?.value;
    const bowserId = document.getElementById('filter-bowser')?.value;
    const status = document.getElementById('filter-status')?.value;
    
    if (vehicleId) params.append('vehicle_id', vehicleId);
    if (driverId) params.append('driver_id', driverId);
    if (bowserId) params.append('bowser_id', bowserId);
    if (status) params.append('status', status);
    
    // Store current filters
    currentFilters = {
        startDate,
        endDate,
        vehicleId,
        driverId,
        bowserId,
        status
    };
    
    logDebug('Transaction filter params', params.toString());
    
    // Fetch transactions
    fetch(`${API_BASE_URL}/transactions?${params.toString()}`)
        .then(response => response.json())
        .then(data => {
            logDebug('Transactions loaded', data);
            updateTransactionsTable(data.transactions);
            updatePagination(data.offset, data.limit, data.total);
            transactionsTotal = data.total;
        })
        .catch(error => {
            console.error('Error loading transactions:', error);
        });
}

// Load vehicles
function loadVehicles() {
    logDebug('Loading vehicles data');
    fetch(`${API_BASE_URL}/vehicles`)
        .then(response => response.json())
        .then(vehicles => {
            logDebug(`Loaded ${vehicles.length} vehicles`);
            const vehiclesTable = document.getElementById('vehicles-table');
            if (!vehiclesTable) {
                console.error('Vehicles table element not found');
                return;
            }
            
            vehiclesTable.innerHTML = '';
            
            vehicles.forEach(vehicle => {
                const row = document.createElement('tr');
                
                // Calculate fuel percentage
                const fuelPercentage = vehicle.fuel_level && vehicle.fuel_capacity
                    ? (vehicle.fuel_level / vehicle.fuel_capacity * 100).toFixed(1)
                    : 'N/A';
                
                // Determine if fuel is low (< 20%)
                const isLowFuel = vehicle.fuel_level && vehicle.fuel_capacity
                    ? vehicle.fuel_level / vehicle.fuel_capacity < 0.2
                    : false;
                
                row.innerHTML = `
                    <td>${vehicle.name}</td>
                    <td>${vehicle.registration || 'N/A'}</td>
                    <td>
                        <div class="fuel-gauge">
                            <div class="fuel-gauge-fill ${isLowFuel ? 'low' : ''}" style="width: ${fuelPercentage !== 'N/A' ? fuelPercentage + '%' : '0%'}"></div>
                        </div>
                        ${vehicle.fuel_level ? formatNumber(vehicle.fuel_level) : 'N/A'} / ${vehicle.fuel_capacity ? formatNumber(vehicle.fuel_capacity) : 'N/A'} L
                        (${fuelPercentage !== 'N/A' ? fuelPercentage + '%' : 'N/A'})
                    </td>
                    <td>${vehicle.odometer ? formatNumber(vehicle.odometer) + ' km' : 'N/A'}</td>
                    <td>${vehicle.standard_burn_rate ? vehicle.standard_burn_rate + ' L/100km' : 'N/A'}</td>
                    <td>${vehicle.last_updated ? formatDateTime(vehicle.last_updated) : 'N/A'}</td>
                `;
                
                vehiclesTable.appendChild(row);
            });
        })
        .catch(error => {
            console.error('Error loading vehicles:', error);
        });
}

// Load bowsers
function loadBowsers() {
    logDebug('Loading bowsers data');
    fetch(`${API_BASE_URL}/bowsers`)
        .then(response => response.json())
        .then(bowsers => {
            logDebug(`Loaded ${bowsers.length} bowsers`);
            const bowsersTable = document.getElementById('bowsers-table');
            if (!bowsersTable) {
                console.error('Bowsers table element not found');
                return;
            }
            
            bowsersTable.innerHTML = '';
            
            bowsers.forEach(bowser => {
                const row = document.createElement('tr');
                
                // Calculate fuel percentage
                const fuelPercentage = bowser.fuel_level && bowser.capacity
                    ? (bowser.fuel_level / bowser.capacity * 100).toFixed(1)
                    : 'N/A';
                
                // Determine if fuel is low (< 20%)
                const isLowFuel = bowser.fuel_level && bowser.capacity
                    ? bowser.fuel_level / bowser.capacity < 0.2
                    : false;
                
                row.innerHTML = `
                    <td>${bowser.name}</td>
                    <td>
                        <div class="fuel-gauge">
                            <div class="fuel-gauge-fill ${isLowFuel ? 'low' : ''}" style="width: ${fuelPercentage !== 'N/A' ? fuelPercentage + '%' : '0%'}"></div>
                        </div>
                        ${bowser.fuel_level ? formatNumber(bowser.fuel_level) : 'N/A'} / ${bowser.capacity ? formatNumber(bowser.capacity) : 'N/A'} L
                        (${fuelPercentage !== 'N/A' ? fuelPercentage + '%' : 'N/A'})
                    </td>
                    <td>${bowser.capacity ? formatNumber(bowser.capacity) + ' L' : 'N/A'}</td>
                    <td>${bowser.critical_level ? formatNumber(bowser.critical_level) + ' L' : 'N/A'}</td>
                    <td>${bowser.total_dispensed ? formatNumber(bowser.total_dispensed) + ' L' : 'N/A'}</td>
                    <td>${bowser.last_updated ? formatDateTime(bowser.last_updated) : 'N/A'}</td>
                `;
                
                bowsersTable.appendChild(row);
            });
        })
        .catch(error => {
            console.error('Error loading bowsers:', error);
        });
}

// Load drivers
function loadDrivers() {
    logDebug('Loading drivers data');
    fetch(`${API_BASE_URL}/drivers`)
        .then(response => response.json())
        .then(drivers => {
            logDebug(`Loaded ${drivers.length} drivers`);
            const driversTable = document.getElementById('drivers-table');
            if (!driversTable) {
                console.error('Drivers table element not found');
                return;
            }
            
            driversTable.innerHTML = '';
            
            drivers.forEach(driver => {
                const row = document.createElement('tr');
                
                row.innerHTML = `
                    <td>${driver.name}</td>
                    <td>${driver.role || 'N/A'}</td>
                    <td>${driver.ibutton_code || 'N/A'}</td>
                `;
                
                driversTable.appendChild(row);
            });
        })
        .catch(error => {
            console.error('Error loading drivers:', error);
        });
}

// Update recent transactions table
function updateRecentTransactionsTable(transactions) {
    logDebug('Updating recent transactions table', transactions);
    const tableBody = document.getElementById('recent-transactions-table');
    if (!tableBody) {
        console.error('Recent transactions table body not found');
        return;
    }
    
    tableBody.innerHTML = '';
    
    if (!transactions || transactions.length === 0) {
        const row = document.createElement('tr');
        row.innerHTML = '<td colspan="8" class="text-center">No transactions found</td>';
        tableBody.appendChild(row);
        return;
    }
    
    transactions.forEach(transaction => {
        const row = document.createElement('tr');
        
        // Status badge class
        const statusClass = `badge-${transaction.status}`;
        
        row.innerHTML = `
            <td>${formatDateTime(transaction.timestamp)}</td>
            <td>${transaction.driver_name}</td>
            <td>${transaction.vehicle_name}</td>
            <td>${transaction.bowser_name}</td>
            <td>${transaction.dispensed_amount ? formatNumber(transaction.dispensed_amount) + ' L' : 'N/A'}</td>
            <td>${transaction.received_amount ? formatNumber(transaction.received_amount) + ' L' : 'N/A'}</td>
            <td><span class="badge ${statusClass}">${transaction.status}</span></td>
            <td>
                <button class="btn btn-sm btn-primary view-transaction" data-transaction-id="${transaction.transaction_id}">View</button>
            </td>
        `;
        
        tableBody.appendChild(row);
    });
    
    // Add event listeners to view buttons
    document.querySelectorAll('.view-transaction').forEach(button => {
        button.addEventListener('click', () => {
            const transactionId = button.getAttribute('data-transaction-id');
            viewTransactionDetails(transactionId);
        });
    });
}

// Update transactions table
function updateTransactionsTable(transactions) {
    logDebug('Updating transactions table', transactions);
    const tableBody = document.getElementById('transactions-table');
    if (!tableBody) {
        console.error('Transactions table body not found');
        return;
    }
    
    tableBody.innerHTML = '';
    
    if (!transactions || transactions.length === 0) {
        const row = document.createElement('tr');
        row.innerHTML = '<td colspan="9" class="text-center">No transactions found</td>';
        tableBody.appendChild(row);
        return;
    }
    
    transactions.forEach(transaction => {
        const row = document.createElement('tr');
        
        // Status badge class
        const statusClass = `badge-${transaction.status}`;
        
        // Calculate discrepancy
        const discrepancy = transaction.dispensed_amount && transaction.received_amount
            ? (transaction.dispensed_amount - transaction.received_amount).toFixed(1) + ' L'
            : 'N/A';
        
        row.innerHTML = `
            <td>${formatDateTime(transaction.timestamp)}</td>
            <td>${transaction.driver_name}</td>
            <td>${transaction.vehicle_name}</td>
            <td>${transaction.bowser_name}</td>
            <td>${transaction.dispensed_amount ? formatNumber(transaction.dispensed_amount) + ' L' : 'N/A'}</td>
            <td>${transaction.received_amount ? formatNumber(transaction.received_amount) + ' L' : 'N/A'}</td>
            <td>${discrepancy}</td>
            <td><span class="badge ${statusClass}">${transaction.status}</span></td>
            <td>
                <button class="btn btn-sm btn-primary view-transaction" data-transaction-id="${transaction.transaction_id}">View</button>
            </td>
        `;
        
        tableBody.appendChild(row);
    });
    
    // Add event listeners to view buttons
    document.querySelectorAll('.view-transaction').forEach(button => {
        button.addEventListener('click', () => {
            const transactionId = button.getAttribute('data-transaction-id');
            viewTransactionDetails(transactionId);
        });
    });
}

// Update pagination
function updatePagination(offset, limit, total) {
    logDebug(`Updating pagination: offset=${offset}, limit=${limit}, total=${total}`);
    const paginationInfo = document.getElementById('pagination-info');
    const prevButton = document.getElementById('prev-page');
    const nextButton = document.getElementById('next-page');
    
    if (!paginationInfo || !prevButton || !nextButton) {
        console.error('Pagination elements not found');
        return;
    }
    
    const start = offset + 1;
    const end = Math.min(offset + limit, total);
    
    paginationInfo.textContent = `Showing ${start}-${end} of ${total}`;
    
    prevButton.disabled = offset === 0;
    nextButton.disabled = end >= total;
}

// View transaction details
function viewTransactionDetails(transactionId) {
    logDebug(`Loading transaction details for ID: ${transactionId}`);
    fetch(`${API_BASE_URL}/transactions/${transactionId}`)
        .then(response => response.json())
        .then(data => {
            logDebug('Transaction details loaded', data);
            const transaction = data.transaction;
            const odometer_readings = data.odometer_readings;
            const fuel_events = data.fuel_events;
            
            // Create modal content
            const modalContent = document.getElementById('transaction-details');
            if (!modalContent) {
                console.error('Transaction details modal content element not found');
                return;
            }
            
            // Status badge class
            const statusClass = `badge-${transaction.status}`;
            
            // Calculate discrepancy
            const discrepancy = transaction.dispensed_amount && transaction.received_amount
                ? (transaction.dispensed_amount - transaction.received_amount).toFixed(1) + ' L'
                : 'N/A';
            
            const discrepancyPercentage = transaction.discrepancy_percentage
                ? transaction.discrepancy_percentage.toFixed(1) + '%'
                : 'N/A';
            
            modalContent.innerHTML = `
                <div class="row mb-3">
                    <div class="col-md-6">
                        <h6>Transaction Information</h6>
                        <table class="table table-sm">
                            <tr>
                                <th>ID</th>
                                <td>${transaction.transaction_id}</td>
                            </tr>
                            <tr>
                                <th>Date/Time</th>
                                <td>${formatDateTime(transaction.timestamp)}</td>
                            </tr>
                            <tr>
                                <th>Status</th>
                                <td><span class="badge ${statusClass}">${transaction.status}</span></td>
                            </tr>
                            <tr>
                                <th>Driver</th>
                                <td>${transaction.driver_name}</td>
                            </tr>
                            <tr>
                                <th>Vehicle</th>
                                <td>${transaction.vehicle_name} (${transaction.vehicle_registration})</td>
                            </tr>
                            <tr>
                                <th>Bowser</th>
                                <td>${transaction.bowser_name}</td>
                            </tr>
                        </table>
                    </div>
                    <div class="col-md-6">
                        <h6>Fuel Information</h6>
                        <table class="table table-sm">
                            <tr>
                                <th>Dispensed Amount</th>
                                <td>${transaction.dispensed_amount ? formatNumber(transaction.dispensed_amount) + ' L' : 'N/A'}</td>
                            </tr>
                            <tr>
                                <th>Received Amount</th>
                                <td>${transaction.received_amount ? formatNumber(transaction.received_amount) + ' L' : 'N/A'}</td>
                            </tr>
                            <tr>
                                <th>Discrepancy</th>
                                <td>${discrepancy} (${discrepancyPercentage})</td>
                            </tr>
                            <tr>
                                <th>Odometer Before</th>
                                <td>${transaction.odometer_before ? formatNumber(transaction.odometer_before) + ' km' : 'N/A'}</td>
                            </tr>
                            <tr>
                                <th>Odometer After</th>
                                <td>${transaction.odometer_after ? formatNumber(transaction.odometer_after) + ' km' : 'N/A'}</td>
                            </tr>
                            <tr>
                                <th>Distance</th>
                                <td>${transaction.odo_distance ? formatNumber(transaction.odo_distance) + ' km' : 'N/A'}</td>
                            </tr>
                        </table>
                    </div>
                </div>
                
                <div class="row">
                    <div class="col-md-6">
                        <h6>Odometer Readings</h6>
                        <table class="table table-sm">
                            <thead>
                                <tr>
                                    <th>Time</th>
                                    <th>Reading</th>
                                    <th>Method</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${odometer_readings && odometer_readings.length > 0
                                    ? odometer_readings.map(reading => `
                                        <tr>
                                            <td>${formatDateTime(reading.timestamp)}</td>
                                            <td>${formatNumber(reading.reading)} km</td>
                                            <td>${reading.input_method}</td>
                                        </tr>
                                    `).join('')
                                    : '<tr><td colspan="3" class="text-center">No odometer readings</td></tr>'
                                }
                            </tbody>
                        </table>
                    </div>
                    <div class="col-md-6">
                        <h6>Fuel Events</h6>
                        <table class="table table-sm">
                            <thead>
                                <tr>
                                    <th>Time</th>
                                    <th>Type</th>
                                    <th>Amount</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${fuel_events && fuel_events.length > 0
                                    ? fuel_events.map(event => `
                                        <tr>
                                            <td>${formatDateTime(event.timestamp)}</td>
                                            <td>${event.event_type}</td>
                                            <td>${formatNumber(event.amount)} L</td>
                                        </tr>
                                    `).join('')
                                    : '<tr><td colspan="3" class="text-center">No fuel events</td></tr>'
                                }
                            </tbody>
                        </table>
                    </div>
                </div>
            `;
            
            // Show modal
            try {
                const transactionModal = new bootstrap.Modal(document.getElementById('transactionModal'));
                transactionModal.show();
                logDebug('Transaction modal displayed');
            } catch (e) {
                console.error('Error showing transaction modal:', e);
            }
        })
        .catch(error => {
            console.error('Error loading transaction details:', error);
        });
}

// Refresh data
function refreshData() {
    logDebug('Refreshing data');
    // Show spinner
    const refreshIcon = document.querySelector('#refresh-data-btn i');
    refreshIcon.classList.add('spin');
    
    // Disable refresh button
    const refreshButton = document.getElementById('refresh-data-btn');
    refreshButton.disabled = true;
    
    // Call API
    fetch(`${API_BASE_URL}/refresh-data`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        }
    })
        .then(response => response.json())
        .then(data => {
            logDebug('Refresh data response', data);
            if (data.success) {
                // Reload current page data
                if (currentPage === 'dashboard') {
                    loadDashboardData();
                } else if (currentPage === 'transactions') {
                    loadTransactions();
                } else if (currentPage === 'vehicles') {
                    loadVehicles();
                } else if (currentPage === 'bowsers') {
                    loadBowsers();
                } else if (currentPage === 'drivers') {
                    loadDrivers();
                }
                
                // Show success message
                alert('Data refreshed successfully!');
            } else {
                // Show error message
                alert('Error refreshing data: ' + (data.message || 'Unknown error'));
            }
        })
        .catch(error => {
            console.error('Error refreshing data:', error);
            alert('Error refreshing data. See console for details.');
        })
        .finally(() => {
            // Remove spinner
            refreshIcon.classList.remove('spin');
            
            // Enable refresh button
            refreshButton.disabled = false;
        });
}

// Simulate transaction
function simulateTransaction() {
    logDebug('Simulating transaction');
    // Disable button
    const simulateButton = document.getElementById('simulate-transaction-btn');
    simulateButton.disabled = true;
    
    // Call API
    fetch(`${API_BASE_URL}/simulate/transaction`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        }
    })
        .then(response => response.json())
        .then(data => {
            logDebug('Simulate transaction response', data);
            if (data.success) {
                // Show success message
                alert(`Transaction simulated: ${data.transaction.vehicle} received ${data.transaction.received_amount} liters.`);
                
                // Reload current page data
                if (currentPage === 'dashboard') {
                    loadDashboardData();
                } else if (currentPage === 'transactions') {
                    loadTransactions();
                } else if (currentPage === 'vehicles') {
                    loadVehicles();
                } else if (currentPage === 'bowsers') {
                    loadBowsers();
                }
            } else {
                // Show error message
                alert('Error simulating transaction: ' + (data.error || 'Unknown error'));
            }
        })
        .catch(error => {
            console.error('Error simulating transaction:', error);
            alert('Error simulating transaction. See console for details.');