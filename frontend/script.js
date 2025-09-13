// Olist E-commerce Analytics Dashboard
class OlistDashboard {
    constructor() {
        this.data = {};
        this.charts = {};
        this.init();
    }

    async init() {
        await this.loadData();
        this.setupEventListeners();
        this.renderCharts();
        this.populateTables();
        this.hideLoading();
    }

    async loadData() {
        const dataFiles = [
            'revenue_by_month_year.json',
            'revenue_per_state.json',
            'top_10_revenue_categories.json',
            'top_10_least_revenue_categories.json',
            'delivery_date_difference.json',
            'real_vs_estimated_delivered_time.json',
            'global_ammount_order_status.json'
        ];

        for (const file of dataFiles) {
            try {
                const response = await fetch(`data/${file}`);
                const data = await response.json();
                const key = file.replace('.json', '');
                this.data[key] = data;
            } catch (error) {
                console.error(`Error loading ${file}:`, error);
            }
        }
    }

    setupEventListeners() {
        const tabButtons = document.querySelectorAll('.tab-btn');
        tabButtons.forEach(btn => {
            btn.addEventListener('click', (e) => {
                const tabName = e.target.getAttribute('onclick').match(/'([^']+)'/)[1];
                this.showTab(tabName);
            });
        });
    }

    showTab(tabName) {
        // Hide all tabs
        document.querySelectorAll('.tab-content').forEach(tab => {
            tab.classList.remove('active');
        });
        
        // Remove active class from all buttons
        document.querySelectorAll('.tab-btn').forEach(btn => {
            btn.classList.remove('active');
        });
        
        // Show selected tab
        document.getElementById(tabName).classList.add('active');
        
        // Add active class to clicked button
        event.target.classList.add('active');
    }

    renderCharts() {
        this.renderMonthlyRevenueChart();
        this.renderStateRevenueChart();
        this.renderDeliveryDifferenceChart();
        this.renderDeliveryTimeChart();
        this.renderTopCategoriesChart();
        this.renderBottomCategoriesChart();
        this.renderOrderStatusCharts();
    }

    renderMonthlyRevenueChart() {
        const ctx = document.getElementById('monthlyRevenueChart').getContext('2d');
        const data = this.data.revenue_by_month_year;
        
        this.charts.monthlyRevenue = new Chart(ctx, {
            type: 'line',
            data: {
                labels: data.map(d => d.month),
                datasets: [
                    {
                        label: '2016',
                        data: data.map(d => d.Year2016),
                        borderColor: '#e74c3c',
                        backgroundColor: 'rgba(231, 76, 60, 0.1)',
                        tension: 0.4,
                        fill: true
                    },
                    {
                        label: '2017',
                        data: data.map(d => d.Year2017),
                        borderColor: '#f39c12',
                        backgroundColor: 'rgba(243, 156, 18, 0.1)',
                        tension: 0.4,
                        fill: true
                    },
                    {
                        label: '2018',
                        data: data.map(d => d.Year2018),
                        borderColor: '#27ae60',
                        backgroundColor: 'rgba(39, 174, 96, 0.1)',
                        tension: 0.4,
                        fill: true
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'top',
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                return context.dataset.label + ': $' + context.parsed.y.toLocaleString();
                            }
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            callback: function(value) {
                                return '$' + value.toLocaleString();
                            }
                        }
                    }
                }
            }
        });
    }

    renderStateRevenueChart() {
        const ctx = document.getElementById('stateRevenueChart').getContext('2d');
        const data = this.data.revenue_per_state;
        
        this.charts.stateRevenue = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.map(d => d.customer_state),
                datasets: [{
                    label: 'Ingresos',
                    data: data.map(d => d.Revenue),
                    backgroundColor: [
                        '#3498db', '#e74c3c', '#f39c12', '#27ae60', '#9b59b6',
                        '#1abc9c', '#34495e', '#e67e22', '#95a5a6', '#2c3e50'
                    ],
                    borderWidth: 0,
                    borderRadius: 8
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: false
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                return 'Ingresos: $' + context.parsed.y.toLocaleString();
                            }
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            callback: function(value) {
                                return '$' + (value / 1000000).toFixed(1) + 'M';
                            }
                        }
                    }
                }
            }
        });
    }

    renderDeliveryDifferenceChart() {
        const ctx = document.getElementById('deliveryDifferenceChart').getContext('2d');
        const data = this.data.delivery_date_difference.slice(0, 15); // Top 15 states
        
        this.charts.deliveryDifference = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.map(d => d.State),
                datasets: [{
                    label: 'Diferencia en Días',
                    data: data.map(d => d.Delivery_Difference),
                    backgroundColor: data.map(d => d.Delivery_Difference < 0 ? '#27ae60' : '#e74c3c'),
                    borderWidth: 0,
                    borderRadius: 8
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: false
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const value = context.parsed.y;
                                const status = value < 0 ? 'temprano' : 'tarde';
                                return `${Math.abs(value)} días ${status}`;
                            }
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            callback: function(value) {
                                return value + ' días';
                            }
                        }
                    }
                }
            }
        });
    }

    renderDeliveryTimeChart() {
        const ctx = document.getElementById('deliveryTimeChart').getContext('2d');
        const data = this.data.real_vs_estimated_delivered_time;
        
        this.charts.deliveryTime = new Chart(ctx, {
            type: 'line',
            data: {
                labels: data.map(d => d.month),
                datasets: [
                    {
                        label: 'Tiempo Real de Entrega 2017',
                        data: data.map(d => d.Year2017_real_time),
                        borderColor: '#e74c3c',
                        backgroundColor: 'rgba(231, 76, 60, 0.1)',
                        tension: 0.4
                    },
                    {
                        label: 'Tiempo Estimado de Entrega 2017',
                        data: data.map(d => d.Year2017_estimated_time),
                        borderColor: '#3498db',
                        backgroundColor: 'rgba(52, 152, 219, 0.1)',
                        tension: 0.4,
                        borderDash: [5, 5]
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'top',
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                return context.dataset.label + ': ' + context.parsed.y.toFixed(1) + ' días';
                            }
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            callback: function(value) {
                                return value.toFixed(1) + ' días';
                            }
                        }
                    }
                }
            }
        });
    }

    renderTopCategoriesChart() {
        const ctx = document.getElementById('topCategoriesChart').getContext('2d');
        const data = this.data.top_10_revenue_categories;
        
        this.charts.topCategories = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: data.map(d => d.Category),
                datasets: [{
                    data: data.map(d => d.Revenue),
                    backgroundColor: [
                        '#3498db', '#e74c3c', '#f39c12', '#27ae60', '#9b59b6',
                        '#1abc9c', '#34495e', '#e67e22', '#95a5a6', '#2c3e50'
                    ],
                    borderWidth: 0
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'right',
                        labels: {
                            boxWidth: 12,
                            padding: 15
                        }
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const total = context.dataset.data.reduce((a, b) => a + b, 0);
                                const percentage = ((context.parsed / total) * 100).toFixed(1);
                                return context.label + ': $' + context.parsed.toLocaleString() + ' (' + percentage + '%)';
                            }
                        }
                    }
                }
            }
        });
    }

    renderBottomCategoriesChart() {
        const ctx = document.getElementById('bottomCategoriesChart').getContext('2d');
        const data = this.data.top_10_least_revenue_categories;
        
        this.charts.bottomCategories = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.map(d => d.Category),
                datasets: [{
                    label: 'Ingresos',
                    data: data.map(d => d.Revenue),
                    backgroundColor: '#95a5a6',
                    borderWidth: 0,
                    borderRadius: 8
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                indexAxis: 'y',
                plugins: {
                    legend: {
                        display: false
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                return 'Ingresos: $' + context.parsed.x.toLocaleString();
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        beginAtZero: true,
                        ticks: {
                            callback: function(value) {
                                return '$' + value.toLocaleString();
                            }
                        }
                    }
                }
            }
        });
    }

    renderOrderStatusCharts() {
        // Pie Chart
        const pieCtx = document.getElementById('orderStatusChart').getContext('2d');
        const data = this.data.global_ammount_order_status;
        
        this.charts.orderStatusPie = new Chart(pieCtx, {
            type: 'pie',
            data: {
                labels: data.map(d => d.order_status),
                datasets: [{
                    data: data.map(d => d.Ammount),
                    backgroundColor: [
                        '#27ae60', '#e74c3c', '#f39c12', '#3498db', 
                        '#9b59b6', '#1abc9c', '#34495e', '#e67e22'
                    ],
                    borderWidth: 0
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: {
                            padding: 20
                        }
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const total = context.dataset.data.reduce((a, b) => a + b, 0);
                                const percentage = ((context.parsed / total) * 100).toFixed(1);
                                return context.label + ': ' + context.parsed.toLocaleString() + ' (' + percentage + '%)';
                            }
                        }
                    }
                }
            }
        });

        // Bar Chart
        const barCtx = document.getElementById('orderStatusBarChart').getContext('2d');
        
        this.charts.orderStatusBar = new Chart(barCtx, {
            type: 'bar',
            data: {
                labels: data.map(d => d.order_status),
                datasets: [{
                    label: 'Cantidad de Pedidos',
                    data: data.map(d => d.Ammount),
                    backgroundColor: [
                        '#27ae60', '#e74c3c', '#f39c12', '#3498db', 
                        '#9b59b6', '#1abc9c', '#34495e', '#e67e22'
                    ],
                    borderWidth: 0,
                    borderRadius: 8
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: false
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            callback: function(value) {
                                return value.toLocaleString();
                            }
                        }
                    }
                }
            }
        });
    }

    populateTables() {
        this.populateRevenueTable();
        this.populateDeliveryTable();
        this.populateOrderStatusTable();
        this.populateCategoryStats();
    }

    populateRevenueTable() {
        const tbody = document.querySelector('#revenueTable tbody');
        const data = this.data.revenue_by_month_year;
        
        tbody.innerHTML = data.map(row => `
            <tr>
                <td>${row.month}</td>
                <td class="currency">$${row.Year2016.toLocaleString()}</td>
                <td class="currency">$${row.Year2017.toLocaleString()}</td>
                <td class="currency">$${row.Year2018.toLocaleString()}</td>
            </tr>
        `).join('');
    }

    populateDeliveryTable() {
        const tbody = document.querySelector('#deliveryTable tbody');
        const data = this.data.delivery_date_difference;
        
        tbody.innerHTML = data.map(row => `
            <tr>
                <td>${row.State}</td>
                <td class="${row.Delivery_Difference < 0 ? 'positive' : 'negative'}">
                    ${row.Delivery_Difference} days
                </td>
            </tr>
        `).join('');
    }

    populateOrderStatusTable() {
        const tbody = document.querySelector('#orderStatusTable tbody');
        const data = this.data.global_ammount_order_status;
        const total = data.reduce((sum, row) => sum + row.Ammount, 0);
        
        tbody.innerHTML = data.map(row => {
            const percentage = ((row.Ammount / total) * 100).toFixed(1);
            return `
                <tr>
                    <td>${row.order_status}</td>
                    <td>${row.Ammount.toLocaleString()}</td>
                    <td>${percentage}%</td>
                </tr>
            `;
        }).join('');
    }

    populateCategoryStats() {
        // Top categories
        const topStats = document.getElementById('topCategoriesStats');
        const topData = this.data.top_10_revenue_categories.slice(0, 5);
        
        topStats.innerHTML = topData.map(item => `
            <div class="stat-item">
                <span class="stat-label">${item.Category}</span>
                <span class="stat-value currency">$${item.Revenue.toLocaleString()}</span>
            </div>
        `).join('');

        // Bottom categories
        const bottomStats = document.getElementById('bottomCategoriesStats');
        const bottomData = this.data.top_10_least_revenue_categories.slice(0, 5);
        
        bottomStats.innerHTML = bottomData.map(item => `
            <div class="stat-item">
                <span class="stat-label">${item.Category}</span>
                <span class="stat-value currency">$${item.Revenue.toLocaleString()}</span>
            </div>
        `).join('');
    }

    hideLoading() {
        document.getElementById('loading').style.display = 'none';
    }
}

// Global function for tab switching
function showTab(tabName) {
    // Hide all tabs
    document.querySelectorAll('.tab-content').forEach(tab => {
        tab.classList.remove('active');
    });
    
    // Remove active class from all buttons
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.remove('active');
    });
    
    // Show selected tab
    document.getElementById(tabName).classList.add('active');
    
    // Add active class to clicked button
    event.target.classList.add('active');
}

// Initialize dashboard when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    new OlistDashboard();
});
