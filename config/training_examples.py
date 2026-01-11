"""
DSPy Training Examples
Examples for optimizing agent programs with MIPROv2
"""

import dspy

# =============================================================================
# SQL Generation Training Examples
# =============================================================================

SQL_TRAINING_EXAMPLES = [
    # Basic queries
    dspy.Example(
        requirement="Find total revenue by region for the last 30 days",
        schema="sales_transactions(transaction_id, transaction_date, store_id, revenue), stores_locations(store_id, region)",
        business_rules="",
        sql_query="""
SELECT s.region, SUM(t.revenue) as total_revenue
FROM sales_transactions t
INNER JOIN stores_locations s ON t.store_id = s.store_id
WHERE t.transaction_date >= DATE('now', '-30 days')
GROUP BY s.region
ORDER BY total_revenue DESC
""".strip(),
        explanation="Joins transactions with stores to aggregate revenue by region for the past 30 days"
    ).with_inputs("requirement", "schema", "business_rules"),
    
    dspy.Example(
        requirement="Show top 10 products by sales quantity",
        schema="sales_transactions(transaction_id, product_id, quantity), products_catalog(product_id, product_name)",
        business_rules="",
        sql_query="""
SELECT p.product_name, SUM(t.quantity) as total_quantity
FROM sales_transactions t
INNER JOIN products_catalog p ON t.product_id = p.product_id
GROUP BY p.product_id, p.product_name
ORDER BY total_quantity DESC
LIMIT 10
""".strip(),
        explanation="Aggregates sales quantity by product and returns top 10"
    ).with_inputs("requirement", "schema", "business_rules"),
    
    # Regional analysis
    dspy.Example(
        requirement="Compare sales performance between Northeast and Southeast regions",
        schema="sales_transactions(transaction_date, store_id, revenue), stores_locations(store_id, region)",
        business_rules="regions: Northeast, Southeast, Midwest, West",
        sql_query="""
SELECT 
    s.region,
    COUNT(*) as transaction_count,
    SUM(t.revenue) as total_revenue,
    AVG(t.revenue) as avg_transaction_value
FROM sales_transactions t
INNER JOIN stores_locations s ON t.store_id = s.store_id
WHERE s.region IN ('Northeast', 'Southeast')
GROUP BY s.region
""".strip(),
        explanation="Compares transaction count, total revenue, and average transaction value between two regions"
    ).with_inputs("requirement", "schema", "business_rules"),
    
    # Product line analysis
    dspy.Example(
        requirement="Show weekly sales trend for outdoor products in the Northeast",
        schema="sales_transactions(transaction_date, store_id, product_id, revenue), stores_locations(store_id, region), products_catalog(product_id, product_line)",
        business_rules="product_lines: Electronics, Apparel, Home, Outdoor, Food",
        sql_query="""
SELECT 
    strftime('%Y-%W', t.transaction_date) as week,
    SUM(t.revenue) as weekly_revenue,
    COUNT(*) as transaction_count
FROM sales_transactions t
INNER JOIN stores_locations s ON t.store_id = s.store_id
INNER JOIN products_catalog p ON t.product_id = p.product_id
WHERE s.region = 'Northeast'
  AND p.product_line = 'Outdoor'
GROUP BY week
ORDER BY week
""".strip(),
        explanation="Weekly aggregation of outdoor product sales in Northeast region"
    ).with_inputs("requirement", "schema", "business_rules"),
    
    # Price tier analysis
    dspy.Example(
        requirement="Find revenue by price tier for premium outdoor products in Northeast",
        schema="sales_transactions(transaction_date, store_id, product_id, revenue), stores_locations(store_id, region), products_catalog(product_id, product_line, price_tier)",
        business_rules="price_tiers: Budget, Standard, Premium",
        sql_query="""
SELECT 
    p.price_tier,
    SUM(t.revenue) as total_revenue,
    COUNT(DISTINCT t.transaction_id) as transactions
FROM sales_transactions t
INNER JOIN stores_locations s ON t.store_id = s.store_id
INNER JOIN products_catalog p ON t.product_id = p.product_id
WHERE s.region = 'Northeast'
  AND p.product_line = 'Outdoor'
  AND p.price_tier = 'Premium'
GROUP BY p.price_tier
""".strip(),
        explanation="Filters for premium outdoor products in Northeast and aggregates revenue"
    ).with_inputs("requirement", "schema", "business_rules"),
    
    # Customer analysis
    dspy.Example(
        requirement="Find top customers by lifetime value in Gold and Platinum tiers",
        schema="customers_profiles(customer_id, first_name, last_name, loyalty_tier, lifetime_value)",
        business_rules="loyalty_tiers: Bronze, Silver, Gold, Platinum",
        sql_query="""
SELECT 
    customer_id,
    first_name || ' ' || last_name as customer_name,
    loyalty_tier,
    lifetime_value
FROM customers_profiles
WHERE loyalty_tier IN ('Gold', 'Platinum')
ORDER BY lifetime_value DESC
LIMIT 20
""".strip(),
        explanation="Retrieves top 20 high-value customers in premium loyalty tiers"
    ).with_inputs("requirement", "schema", "business_rules"),
    
    # Time comparison
    dspy.Example(
        requirement="Compare this month's sales to last month by product line",
        schema="sales_transactions(transaction_date, product_id, revenue), products_catalog(product_id, product_line)",
        business_rules="",
        sql_query="""
SELECT 
    p.product_line,
    SUM(CASE WHEN t.transaction_date >= DATE('now', 'start of month') 
             THEN t.revenue ELSE 0 END) as current_month,
    SUM(CASE WHEN t.transaction_date >= DATE('now', 'start of month', '-1 month')
             AND t.transaction_date < DATE('now', 'start of month')
             THEN t.revenue ELSE 0 END) as last_month
FROM sales_transactions t
INNER JOIN products_catalog p ON t.product_id = p.product_id
WHERE t.transaction_date >= DATE('now', 'start of month', '-1 month')
GROUP BY p.product_line
""".strip(),
        explanation="Uses conditional aggregation to compare current and previous month sales"
    ).with_inputs("requirement", "schema", "business_rules"),
    
    # Weather correlation
    dspy.Example(
        requirement="Correlate outdoor product sales with temperature in Northeast",
        schema="sales_transactions(transaction_date, store_id, product_id, revenue), stores_locations(store_id, region), products_catalog(product_id, product_line), weather_data(date, region, avg_temperature)",
        business_rules="",
        sql_query="""
SELECT 
    t.transaction_date,
    w.avg_temperature,
    SUM(t.revenue) as daily_revenue
FROM sales_transactions t
INNER JOIN stores_locations s ON t.store_id = s.store_id
INNER JOIN products_catalog p ON t.product_id = p.product_id
INNER JOIN weather_data w ON t.transaction_date = w.date AND s.region = w.region
WHERE s.region = 'Northeast'
  AND p.product_line = 'Outdoor'
GROUP BY t.transaction_date, w.avg_temperature
ORDER BY t.transaction_date
""".strip(),
        explanation="Joins sales with weather data to analyze temperature impact on outdoor sales"
    ).with_inputs("requirement", "schema", "business_rules"),
    
    # Campaign analysis
    dspy.Example(
        requirement="Find active marketing campaigns targeting outdoor products",
        schema="marketing_campaigns(campaign_id, campaign_name, target_product_line, status, start_date, end_date)",
        business_rules="",
        sql_query="""
SELECT 
    campaign_id,
    campaign_name,
    start_date,
    end_date,
    status
FROM marketing_campaigns
WHERE (target_product_line = 'Outdoor' OR target_product_line IS NULL)
  AND status = 'active'
ORDER BY start_date DESC
""".strip(),
        explanation="Finds campaigns targeting outdoor products or all products that are currently active"
    ).with_inputs("requirement", "schema", "business_rules"),
    
    # Competitor analysis
    dspy.Example(
        requirement="Show competitor promotions on outdoor products in the last month",
        schema="competitors_pricing(date, competitor_name, product_category, discount_percentage, promotion_active)",
        business_rules="",
        sql_query="""
SELECT 
    competitor_name,
    date,
    discount_percentage,
    promotion_active
FROM competitors_pricing
WHERE product_category = 'Outdoor'
  AND date >= DATE('now', '-30 days')
  AND promotion_active = 1
ORDER BY date DESC, competitor_name
""".strip(),
        explanation="Retrieves competitor outdoor promotions from the past 30 days"
    ).with_inputs("requirement", "schema", "business_rules"),
]


# =============================================================================
# Data Discovery Training Examples
# =============================================================================

DATA_DISCOVERY_EXAMPLES = [
    dspy.Example(
        question="What tables contain sales information?",
        relevant_tables="sales_transactions, products_catalog, stores_locations",
        explanation="sales_transactions contains transaction-level data, products_catalog has product details, stores_locations provides store context"
    ).with_inputs("question"),
    
    dspy.Example(
        question="Where can I find customer loyalty data?",
        relevant_tables="customers_profiles",
        explanation="customers_profiles contains loyalty_tier, signup_date, and lifetime_value for customer analysis"
    ).with_inputs("question"),
    
    dspy.Example(
        question="Which tables have regional information?",
        relevant_tables="stores_locations, weather_data, customers_profiles",
        explanation="stores_locations has store regions, weather_data tracks weather by region, customers_profiles has customer region"
    ).with_inputs("question"),
    
    dspy.Example(
        question="What data do we have about outdoor products?",
        relevant_tables="products_catalog, sales_transactions, inventory_levels",
        explanation="products_catalog has product_line='Outdoor' filter, sales_transactions for sales data, inventory_levels for stock"
    ).with_inputs("question"),
    
    dspy.Example(
        question="How can I analyze the impact of weather on sales?",
        relevant_tables="sales_transactions, stores_locations, weather_data",
        explanation="Join sales with weather through stores_locations.region = weather_data.region and transaction_date = weather.date"
    ).with_inputs("question"),
]


# =============================================================================
# Task Decomposition Training Examples
# =============================================================================

TASK_DECOMPOSITION_EXAMPLES = [
    dspy.Example(
        user_question="Why are sales declining for outdoor products in the Northeast?",
        business_context="RetailCo is a national retailer with stores across 4 regions",
        task_plan="""
1. Data Discovery: Identify tables for sales, products, stores, weather, campaigns, competitors
2. Sales Analysis: Query weekly sales trend for outdoor products in Northeast (last 60 days)
3. Weather Correlation: Analyze temperature patterns vs outdoor sales
4. Campaign Check: Find active/recent marketing campaigns for outdoor products
5. Competitor Analysis: Check competitor promotions on outdoor products
6. Synthesis: Combine findings into root cause analysis and recommendations
""".strip(),
        required_agents="data_discovery, sql_generation, analysis, synthesis"
    ).with_inputs("user_question", "business_context"),
    
    dspy.Example(
        user_question="What are our best-selling products this quarter?",
        business_context="RetailCo is a national retailer with 5 product lines",
        task_plan="""
1. Data Discovery: Identify sales and product tables
2. Sales Query: Aggregate sales by product for current quarter
3. Analysis: Rank products by revenue and quantity
4. Synthesis: Summarize top performers with key metrics
""".strip(),
        required_agents="data_discovery, sql_generation, synthesis"
    ).with_inputs("user_question", "business_context"),
    
    dspy.Example(
        user_question="Which stores are underperforming and why?",
        business_context="RetailCo has 500 stores across 4 regions",
        task_plan="""
1. Data Discovery: Identify store, sales, inventory, and staffing tables
2. Performance Query: Calculate revenue per store vs regional average
3. Inventory Check: Analyze stock levels at underperforming stores
4. Regional Analysis: Compare underperformers to regional benchmarks
5. Synthesis: Identify patterns and recommend interventions
""".strip(),
        required_agents="data_discovery, sql_generation, analysis, synthesis"
    ).with_inputs("user_question", "business_context"),
]


def get_sql_examples(n: int = None):
    """Get SQL training examples"""
    examples = SQL_TRAINING_EXAMPLES
    return examples[:n] if n else examples


def get_discovery_examples(n: int = None):
    """Get data discovery training examples"""
    examples = DATA_DISCOVERY_EXAMPLES
    return examples[:n] if n else examples


def get_decomposition_examples(n: int = None):
    """Get task decomposition training examples"""
    examples = TASK_DECOMPOSITION_EXAMPLES
    return examples[:n] if n else examples
