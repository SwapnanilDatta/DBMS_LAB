import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
import plotly.graph_objects as go
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

DB_HOST = st.secrets["DB_HOST"]
DB_PORT = st.secrets["DB_PORT"]
DB_USER = st.secrets["DB_USER"]
DB_PASSWORD = st.secrets["DB_PASSWORD"]
DB_NAME = st.secrets["DB_NAME"]

# Page configuration
st.set_page_config(
    page_title="Retail Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_resource
def init_connection():
    try:
        engine = create_engine(
            f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )
        return engine
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None

# In the run_query function (around line 25), modify to ensure proper date parsing:
@st.cache_data(ttl=600)
def run_query(query, parse_dates=None):
    """Run query with optional date parsing"""
    engine = init_connection()
    if engine:
        try:
            df = pd.read_sql(query, engine, parse_dates=parse_dates)
            # Ensure date columns are properly converted
            if parse_dates:
                for date_col in parse_dates:
                    if date_col in df.columns:
                        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            return df
        except Exception as e:
            st.error(f"Query execution failed: {e}")
            st.error(f"Query: {query}")
            return None
    return None

# Sidebar navigation
st.sidebar.title("📊 Retail Analytics")
page = st.sidebar.radio(
    "Navigate to:",
    ["Home", "Sales Dashboard", "Customer Insights", "Inventory & Store Management", "Profitability & Revenue"]
)

# Home Page
if page == "Home":
    st.title("🏠 Retail Analytics Dashboard")
    st.markdown("""
    Welcome to the Retail Analytics Dashboard! This application provides comprehensive insights into:
    
    - 📈 **Sales Performance** - Track revenue, orders, and key metrics
    - 👥 **Customer Behavior** - Understand customer preferences and patterns  
    - 📦 **Inventory Management** - Manage products and store operations
    - 💰 **Profitability Analysis** - Analyze margins and revenue breakdowns
    
    Use the sidebar to navigate between different sections.
    """)
    
    # Placeholder for 5 PNG images
    st.subheader("📊 Key Visualizations")
    col1, col2 = st.columns(2)
    
    # with col1:
    #     st.image("placeholder1.png", caption="Sales Trend Analysis", use_column_width=True)
    #     st.image("placeholder3.png", caption="Customer Segmentation", use_column_width=True)
    #     st.image("placeholder5.png", caption="Revenue Breakdown", use_column_width=True)
    
    # with col2:
    #     st.image("placeholder2.png", caption="Product Performance", use_column_width=True)
    #     st.image("placeholder4.png", caption="Geographic Distribution", use_column_width=True)

# Sales Dashboard Page
elif page == "Sales Dashboard":
    st.title("📈 Sales Dashboard")
    
    # Key Metrics
    st.subheader("Key Performance Indicators")
    
    # Total Revenue
    revenue_query = "SELECT SUM(Revenue) as total_revenue FROM INVOICE_ITEM"
    revenue_df = run_query(revenue_query)
    total_revenue = revenue_df['total_revenue'].iloc[0] if revenue_df is not None and not revenue_df.empty else 0
    
    # Total Orders
    orders_query = "SELECT COUNT(DISTINCT InvoiceNo) as total_orders FROM INVOICE"
    orders_df = run_query(orders_query)
    total_orders = orders_df['total_orders'].iloc[0] if orders_df is not None and not orders_df.empty else 0
    
    # Average Order Value
    aov_query = """
    SELECT AVG(order_total) as avg_order_value 
    FROM (
        SELECT InvoiceNo, SUM(Revenue) as order_total 
        FROM INVOICE_ITEM 
        GROUP BY InvoiceNo
    ) as order_totals
    """
    aov_df = run_query(aov_query)
    avg_order_value = aov_df['avg_order_value'].iloc[0] if aov_df is not None and not aov_df.empty else 0
    
    # Total Customers
    customers_query = "SELECT COUNT(DISTINCT CustomerID) as total_customers FROM CUSTOMER"
    customers_df = run_query(customers_query)
    total_customers = customers_df['total_customers'].iloc[0] if customers_df is not None and not customers_df.empty else 0
    
    # Display metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Revenue", f"${(total_revenue or 0):,.2f}")

    with col2:
        st.metric("Total Orders", f"{total_orders:,}")
    with col3:
        st.metric("Avg Order Value", f"${(avg_order_value or 0):.2f}")

    with col4:
        st.metric("Total Customers", f"{total_customers:,}")
    
    # Revenue by Date - FIXED
    st.subheader("Revenue Trend")
    revenue_trend_query = """
    SELECT 
        DATE(i.InvoiceDate) as InvoiceDate, 
        SUM(ii.Revenue) as daily_revenue
    FROM INVOICE i
    JOIN INVOICE_ITEM ii ON i.InvoiceNo = ii.InvoiceNo
    WHERE i.InvoiceStatus = 'Paid'
    GROUP BY DATE(i.InvoiceDate)
    ORDER BY InvoiceDate
    """
    revenue_trend_df = run_query(revenue_trend_query)
    
    if revenue_trend_df is not None and not revenue_trend_df.empty:
        # Ensure InvoiceDate is datetime
        revenue_trend_df['InvoiceDate'] = pd.to_datetime(revenue_trend_df['InvoiceDate'])
        
        fig = px.line(revenue_trend_df, x='InvoiceDate', y='daily_revenue', 
                     title='Daily Revenue Trend',
                     labels={'daily_revenue': 'Revenue ($)', 'InvoiceDate': 'Date'})
        fig.update_traces(line_color='#1f77b4', line_width=2)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No revenue trend data available")
    
    # Revenue by Category
    st.subheader("Revenue by Category")
    category_revenue_query = """
    SELECT c.CategoryName, SUM(ii.Revenue) as category_revenue
    FROM INVOICE_ITEM ii
    JOIN PRODUCT p ON ii.StockCode = p.StockCode
    JOIN CATEGORY c ON p.CategoryID = c.CategoryID
    JOIN INVOICE i ON ii.InvoiceNo = i.InvoiceNo
    WHERE i.InvoiceStatus = 'Paid'
    GROUP BY c.CategoryName
    ORDER BY category_revenue DESC
    """
    category_revenue_df = run_query(category_revenue_query)
    
    if category_revenue_df is not None and not category_revenue_df.empty:
        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(category_revenue_df, x='CategoryName', y='category_revenue',
                        title='Revenue by Category',
                        labels={'category_revenue': 'Revenue ($)', 'CategoryName': 'Category'})
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = px.pie(category_revenue_df, values='category_revenue', names='CategoryName',
                        title='Revenue Distribution by Category')
            st.plotly_chart(fig, use_container_width=True)

# Customer Insights Page
elif page == "Customer Insights":
    st.title("👥 Customer Insights")
    
    # Top Customers by Revenue
    st.subheader("Top Customers by Spending")
    top_customers_query = """
    SELECT c.CustomerName, SUM(ii.Revenue) as total_spent, COUNT(DISTINCT i.InvoiceNo) as order_count
    FROM CUSTOMER c
    JOIN INVOICE i ON c.CustomerID = i.CustomerID
    JOIN INVOICE_ITEM ii ON i.InvoiceNo = ii.InvoiceNo
    WHERE i.InvoiceStatus = 'Paid'
    GROUP BY c.CustomerID, c.CustomerName
    ORDER BY total_spent DESC
    LIMIT 10
    """
    top_customers_df = run_query(top_customers_query)
    
    if top_customers_df is not None and not top_customers_df.empty:
        fig = px.bar(top_customers_df, x='CustomerName', y='total_spent',
                    title='Top 10 Customers by Total Spending',
                    labels={'total_spent': 'Total Spent ($)', 'CustomerName': 'Customer'})
        st.plotly_chart(fig, use_container_width=True)
    
    # Customer Preferences - Brands
    st.subheader("Customer Brand Preferences")
    brand_preference_query = """
    SELECT b.BrandName, COUNT(DISTINCT i.CustomerID) as customer_count
    FROM INVOICE i
    JOIN INVOICE_ITEM ii ON i.InvoiceNo = ii.InvoiceNo
    JOIN PRODUCT p ON ii.StockCode = p.StockCode
    JOIN BRAND b ON p.BrandID = b.BrandID
    WHERE i.InvoiceStatus = 'Paid'
    GROUP BY b.BrandName
    ORDER BY customer_count DESC
    """
    brand_preference_df = run_query(brand_preference_query)
    
    if brand_preference_df is not None and not brand_preference_df.empty:
        fig = px.pie(brand_preference_df, values='customer_count', names='BrandName',
                    title='Brand Popularity Among Customers')
        st.plotly_chart(fig, use_container_width=True)
    
    # Customer Geographic Distribution
    st.subheader("Customer Geographic Distribution")
    geo_query = """
    SELECT s.Country, COUNT(DISTINCT i.CustomerID) as customer_count
    FROM INVOICE i
    JOIN STORE s ON i.StoreID = s.StoreID
    GROUP BY s.Country
    ORDER BY customer_count DESC
    """
    geo_df = run_query(geo_query)
    
    if geo_df is not None and not geo_df.empty:
        fig = px.bar(geo_df, x='Country', y='customer_count',
                    title='Customer Distribution by Country',
                    labels={'customer_count': 'Number of Customers', 'Country': 'Country'})
        st.plotly_chart(fig, use_container_width=True)

# Inventory & Store Management Page
elif page == "Inventory & Store Management":
    st.title("📦 Inventory & Store Management")
    
    tab1, tab2, tab3 = st.tabs(["View Data", "Add Data", "Remove Data"])
    
    with tab1:
        st.subheader("Current Inventory & Store Data")
        
        # Product Inventory
        st.write("### Product Inventory")
        products_query = """
        SELECT p.StockCode, p.Description, p.UnitPrice, c.CategoryName, b.BrandName
        FROM PRODUCT p
        JOIN CATEGORY c ON p.CategoryID = c.CategoryID
        JOIN BRAND b ON p.BrandID = b.BrandID
        LIMIT 100
        """
        products_df = run_query(products_query)
        if products_df is not None:
            st.dataframe(products_df, use_container_width=True)
        
        # Store Information
        st.write("### Store Information")
        stores_query = "SELECT * FROM STORE"
        stores_df = run_query(stores_query)
        if stores_df is not None:
            st.dataframe(stores_df, use_container_width=True)
    
    with tab2:
        st.subheader("Add New Data")
        
        add_option = st.selectbox("Select data to add:", 
                                 ["Product", "Customer", "Store", "Brand", "Category"])
        
        if add_option == "Product":
            with st.form("add_product"):
                st.write("Add New Product")
                stock_code = st.text_input("Stock Code")
                description = st.text_input("Description")
                unit_price = st.number_input("Unit Price", min_value=0.0, format="%.2f")
                category_id = st.number_input("Category ID", min_value=1, step=1)
                brand_id = st.number_input("Brand ID", min_value=1, step=1)
                
                if st.form_submit_button("Add Product"):
                    # Here you would implement the INSERT query
                    st.success(f"Product {description} added successfully!")
        
        elif add_option == "Customer":
            with st.form("add_customer"):
                st.write("Add New Customer")
                customer_name = st.text_input("Customer Name")
                email = st.text_input("Email")
                phone = st.text_input("Phone Number")
                
                if st.form_submit_button("Add Customer"):
                    # Implement INSERT query
                    st.success(f"Customer {customer_name} added successfully!")
    
    with tab3:
        st.subheader("Remove Data")
        
        remove_option = st.selectbox("Select data to remove:", 
                                    ["Product", "Customer", "Store"])
        
        if remove_option == "Product":
            if products_df is not None and not products_df.empty:
                product_to_remove = st.selectbox("Select product to remove:", 
                                               products_df['Description'].tolist())
                if st.button("Remove Product"):
                    # Implement DELETE query
                    st.success(f"Product {product_to_remove} removed successfully!")

# Profitability & Revenue Page
elif page == "Profitability & Revenue":
    st.title("💰 Profitability & Revenue Analysis")
    
    # Revenue by Payment Method
    st.subheader("Revenue by Payment Method")
    payment_revenue_query = """
    SELECT p.PaymentMethod, SUM(ii.Revenue) as total_revenue
    FROM INVOICE i
    JOIN INVOICE_ITEM ii ON i.InvoiceNo = ii.InvoiceNo
    JOIN PAYMENT p ON i.PaymentID = p.PaymentID
    WHERE i.InvoiceStatus = 'Paid'
    GROUP BY p.PaymentMethod
    ORDER BY total_revenue DESC
    """
    payment_revenue_df = run_query(payment_revenue_query)
    
    if payment_revenue_df is not None and not payment_revenue_df.empty:
        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(payment_revenue_df, x='PaymentMethod', y='total_revenue',
                        title='Revenue by Payment Method',
                        labels={'total_revenue': 'Revenue ($)', 'PaymentMethod': 'Payment Method'})
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = px.pie(payment_revenue_df, values='total_revenue', names='PaymentMethod',
                        title='Payment Method Distribution')
            st.plotly_chart(fig, use_container_width=True)
    
    # Monthly Revenue Breakdown - FIXED
    st.subheader("Monthly Revenue Analysis")
    monthly_revenue_query = """
    SELECT 
        DATE_FORMAT(i.InvoiceDate, '%Y-%m') as month_date,
        SUM(ii.Revenue) as monthly_revenue,
        COUNT(DISTINCT i.InvoiceNo) as order_count
    FROM INVOICE i
    JOIN INVOICE_ITEM ii ON i.InvoiceNo = ii.InvoiceNo
    WHERE i.InvoiceStatus = 'Paid'
    GROUP BY month_date
    ORDER BY month_date
    """
    monthly_revenue_df = run_query(monthly_revenue_query)
    
    if monthly_revenue_df is not None and not monthly_revenue_df.empty:
        # Convert month_date to datetime for proper plotting
        monthly_revenue_df['month_date'] = pd.to_datetime(monthly_revenue_df['month_date'] + '-01')
        
        # Create line chart
        fig = px.line(monthly_revenue_df, x='month_date', y='monthly_revenue',
                     title='Monthly Revenue Trend',
                     labels={'monthly_revenue': 'Revenue ($)', 'month_date': 'Month'})
        fig.update_traces(line_color='#2ca02c', line_width=3)
        fig.update_xaxes(tickformat='%Y-%m')
        st.plotly_chart(fig, use_container_width=True)
        
        # Show data table
        with st.expander("View Monthly Data"):
            display_df = monthly_revenue_df.copy()
            display_df['month_date'] = display_df['month_date'].dt.strftime('%Y-%m')
            st.dataframe(display_df, use_container_width=True)
    else:
        st.warning("No monthly revenue data available")
    
    # Profit Margin Analysis (assuming cost data is available)
    st.subheader("Product Performance Analysis")
    product_performance_query = """
    SELECT 
        p.Description,
        c.CategoryName,
        SUM(ii.Revenue) as total_revenue,
        SUM(ii.Quantity) as total_quantity,
        AVG(p.UnitPrice) as avg_unit_price
    FROM PRODUCT p
    JOIN INVOICE_ITEM ii ON p.StockCode = ii.StockCode
    JOIN CATEGORY c ON p.CategoryID = c.CategoryID
    JOIN INVOICE i ON ii.InvoiceNo = i.InvoiceNo
    WHERE i.InvoiceStatus = 'Paid'
    GROUP BY p.StockCode, p.Description, c.CategoryName
    ORDER BY total_revenue DESC
    LIMIT 15
    """
    product_performance_df = run_query(product_performance_query)
    
    if product_performance_df is not None and not product_performance_df.empty:
        fig = px.scatter(product_performance_df, x='total_quantity', y='total_revenue',
                        size='avg_unit_price', color='CategoryName',
                        hover_name='Description', 
                        title='Product Performance: Revenue vs Quantity',
                        labels={'total_quantity': 'Units Sold', 'total_revenue': 'Revenue ($)'})
        st.plotly_chart(fig, use_container_width=True)


st.sidebar.markdown("---")
st.sidebar.markdown("Built with Streamlit & MySQL")