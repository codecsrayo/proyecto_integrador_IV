# Olist E-commerce Analytics Dashboard

A comprehensive analytics dashboard for Olist e-commerce data, featuring interactive visualizations and insights into revenue performance, delivery metrics, and product categories.

## Features

### 📊 Revenue Analysis
- Monthly revenue trends across 2016-2018
- Top 10 states by revenue performance
- Interactive charts with detailed breakdowns

### 🚚 Delivery Performance
- Delivery date difference analysis by state
- Real vs estimated delivery time comparisons
- Performance metrics visualization

### 🏷️ Product Categories
- Top 10 highest revenue categories
- Bottom 10 lowest revenue categories
- Category performance statistics

### 📦 Order Status
- Order status distribution analysis
- Interactive pie and bar charts
- Comprehensive order metrics

## Technology Stack

- **Frontend**: HTML5, CSS3, JavaScript (ES6+)
- **Charts**: Chart.js for interactive visualizations
- **Styling**: Modern CSS with gradients and animations
- **Icons**: Font Awesome
- **Deployment**: Netlify

## Data Sources

The dashboard uses pre-processed JSON data from SQL queries executed against the Olist database:

- `revenue_by_month_year.json` - Monthly revenue by year
- `revenue_per_state.json` - Revenue by Brazilian states
- `top_10_revenue_categories.json` - Highest performing categories
- `top_10_least_revenue_categories.json` - Lowest performing categories
- `delivery_date_difference.json` - Delivery performance by state
- `real_vs_estimated_delivered_time.json` - Delivery time analysis
- `global_ammount_order_status.json` - Order status distribution

## Features

✨ **Modern UI/UX**
- Responsive design for all devices
- Smooth animations and transitions
- Professional color scheme
- Interactive hover effects

📱 **Mobile Responsive**
- Optimized for mobile and tablet viewing
- Flexible grid layouts
- Touch-friendly navigation

📈 **Interactive Charts**
- Hover tooltips with detailed information
- Multiple chart types (line, bar, pie, doughnut)
- Color-coded data visualization
- Responsive chart sizing

## Local Development

1. Clone the repository
2. Navigate to the `frontend` directory
3. Open `index.html` in a web browser
4. Or serve with a local server:
   ```bash
   python3 -m http.server 8000
   ```

## Deployment

This dashboard is optimized for Netlify deployment with:
- Static site configuration
- Proper headers for security
- JSON content type handling
- No build process required

## Browser Support

- Chrome (recommended)
- Firefox
- Safari
- Edge
- Mobile browsers

## License

This project is part of an academic integration project (Proyecto Integrador IV).
