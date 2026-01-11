"""
Database Schema Definitions
Used by agents to understand table structures
"""

RETAIL_SCHEMA = {
    "catalog": "retail",
    "schema": "analytics",
    "tables": {
        "stores_locations": {
            "description": "Store locations and attributes for all retail stores",
            "columns": {
                "store_id": {"type": "INTEGER", "description": "Primary key", "pk": True},
                "store_name": {"type": "TEXT", "description": "Store display name"},
                "region": {"type": "TEXT", "description": "Geographic region: Northeast, Southeast, Midwest, West"},
                "state": {"type": "TEXT", "description": "US state abbreviation"},
                "city": {"type": "TEXT", "description": "City name"},
                "address": {"type": "TEXT", "description": "Street address"},
                "store_type": {"type": "TEXT", "description": "Type: Standard, Premium, Outlet"},
                "square_footage": {"type": "INTEGER", "description": "Store size in sq ft"},
                "opened_date": {"type": "DATE", "description": "Store opening date"},
                "manager_name": {"type": "TEXT", "description": "Current store manager"}
            },
            "row_count": "~50",
            "update_frequency": "monthly"
        },
        "products_catalog": {
            "description": "Product master data with pricing and categorization",
            "columns": {
                "product_id": {"type": "INTEGER", "description": "Primary key", "pk": True},
                "product_name": {"type": "TEXT", "description": "Product display name"},
                "product_line": {"type": "TEXT", "description": "Line: Electronics, Apparel, Home, Outdoor, Food"},
                "category": {"type": "TEXT", "description": "Product category within line"},
                "subcategory": {"type": "TEXT", "description": "Subcategory"},
                "price_tier": {"type": "TEXT", "description": "Pricing tier: Budget, Standard, Premium"},
                "unit_price": {"type": "REAL", "description": "Selling price"},
                "unit_cost": {"type": "REAL", "description": "Cost to company"},
                "brand": {"type": "TEXT", "description": "Brand name"},
                "supplier_id": {"type": "INTEGER", "description": "Supplier reference"},
                "is_active": {"type": "BOOLEAN", "description": "Active product flag"}
            },
            "row_count": "~750",
            "update_frequency": "weekly"
        },
        "customers_profiles": {
            "description": "Customer information and loyalty status",
            "columns": {
                "customer_id": {"type": "INTEGER", "description": "Primary key", "pk": True},
                "first_name": {"type": "TEXT", "description": "Customer first name"},
                "last_name": {"type": "TEXT", "description": "Customer last name"},
                "email": {"type": "TEXT", "description": "Email address"},
                "loyalty_tier": {"type": "TEXT", "description": "Tier: Bronze, Silver, Gold, Platinum"},
                "signup_date": {"type": "DATE", "description": "Account creation date"},
                "lifetime_value": {"type": "REAL", "description": "Total customer spend"},
                "preferred_store_id": {"type": "INTEGER", "description": "Primary store"},
                "region": {"type": "TEXT", "description": "Customer region"}
            },
            "row_count": "~10,000",
            "update_frequency": "daily"
        },
        "sales_transactions": {
            "description": "Individual sales transactions with line item detail",
            "columns": {
                "transaction_id": {"type": "INTEGER", "description": "Primary key", "pk": True},
                "transaction_date": {"type": "DATE", "description": "Sale date"},
                "transaction_time": {"type": "TEXT", "description": "Sale time HH:MM:SS"},
                "store_id": {"type": "INTEGER", "description": "FK to stores_locations"},
                "customer_id": {"type": "INTEGER", "description": "FK to customers_profiles (nullable)"},
                "product_id": {"type": "INTEGER", "description": "FK to products_catalog"},
                "quantity": {"type": "INTEGER", "description": "Units sold"},
                "unit_price": {"type": "REAL", "description": "Price per unit"},
                "discount_amount": {"type": "REAL", "description": "Discount applied"},
                "revenue": {"type": "REAL", "description": "Total line revenue"},
                "payment_method": {"type": "TEXT", "description": "Payment: Credit, Debit, Cash, Mobile"}
            },
            "row_count": "~100,000",
            "update_frequency": "real-time",
            "partitioned_by": "transaction_date"
        },
        "inventory_levels": {
            "description": "Current inventory by store and product",
            "columns": {
                "inventory_id": {"type": "INTEGER", "description": "Primary key", "pk": True},
                "store_id": {"type": "INTEGER", "description": "FK to stores_locations"},
                "product_id": {"type": "INTEGER", "description": "FK to products_catalog"},
                "quantity_on_hand": {"type": "INTEGER", "description": "Available inventory"},
                "quantity_reserved": {"type": "INTEGER", "description": "Reserved for orders"},
                "reorder_point": {"type": "INTEGER", "description": "Reorder trigger level"},
                "last_updated": {"type": "TIMESTAMP", "description": "Last update time"}
            },
            "row_count": "~37,500",
            "update_frequency": "real-time"
        },
        "marketing_campaigns": {
            "description": "Marketing campaign tracking and performance",
            "columns": {
                "campaign_id": {"type": "INTEGER", "description": "Primary key", "pk": True},
                "campaign_name": {"type": "TEXT", "description": "Campaign name"},
                "campaign_type": {"type": "TEXT", "description": "Type: Discount, Promotion, Multi-channel"},
                "start_date": {"type": "DATE", "description": "Campaign start"},
                "end_date": {"type": "DATE", "description": "Campaign end (nullable if ongoing)"},
                "target_region": {"type": "TEXT", "description": "Target region or 'All'"},
                "target_product_line": {"type": "TEXT", "description": "Target product line (nullable)"},
                "budget": {"type": "REAL", "description": "Campaign budget"},
                "actual_spend": {"type": "REAL", "description": "Actual spend to date"},
                "impressions": {"type": "INTEGER", "description": "Ad impressions"},
                "conversions": {"type": "INTEGER", "description": "Conversions attributed"},
                "status": {"type": "TEXT", "description": "Status: active, completed, paused"}
            },
            "row_count": "~10",
            "update_frequency": "daily"
        },
        "weather_data": {
            "description": "Daily weather conditions by region",
            "columns": {
                "weather_id": {"type": "INTEGER", "description": "Primary key", "pk": True},
                "date": {"type": "DATE", "description": "Weather date"},
                "region": {"type": "TEXT", "description": "Geographic region"},
                "avg_temperature": {"type": "REAL", "description": "Average temp (F)"},
                "precipitation": {"type": "REAL", "description": "Precipitation (inches)"},
                "weather_condition": {"type": "TEXT", "description": "Condition: Sunny, Cloudy, Rainy, Snowy, Windy"},
                "is_extreme": {"type": "BOOLEAN", "description": "Extreme weather flag"}
            },
            "row_count": "~360",
            "update_frequency": "daily"
        },
        "competitors_pricing": {
            "description": "Competitor pricing intelligence",
            "columns": {
                "pricing_id": {"type": "INTEGER", "description": "Primary key", "pk": True},
                "date": {"type": "DATE", "description": "Observation date"},
                "competitor_name": {"type": "TEXT", "description": "Competitor name"},
                "product_category": {"type": "TEXT", "description": "Product category"},
                "avg_price": {"type": "REAL", "description": "Average competitor price"},
                "discount_percentage": {"type": "REAL", "description": "Active discount %"},
                "promotion_active": {"type": "BOOLEAN", "description": "Promotion running flag"}
            },
            "row_count": "~1,000",
            "update_frequency": "weekly"
        }
    },
    "relationships": [
        {"from": "sales_transactions.store_id", "to": "stores_locations.store_id", "type": "many-to-one"},
        {"from": "sales_transactions.product_id", "to": "products_catalog.product_id", "type": "many-to-one"},
        {"from": "sales_transactions.customer_id", "to": "customers_profiles.customer_id", "type": "many-to-one"},
        {"from": "inventory_levels.store_id", "to": "stores_locations.store_id", "type": "many-to-one"},
        {"from": "inventory_levels.product_id", "to": "products_catalog.product_id", "type": "many-to-one"},
        {"from": "customers_profiles.preferred_store_id", "to": "stores_locations.store_id", "type": "many-to-one"}
    ],
    "business_rules": {
        "revenue_calculation": "revenue = quantity * (unit_price - discount_amount)",
        "margin_calculation": "margin = (unit_price - unit_cost) / unit_price",
        "regions": ["Northeast", "Southeast", "Midwest", "West"],
        "product_lines": ["Electronics", "Apparel", "Home", "Outdoor", "Food"],
        "price_tiers": ["Budget", "Standard", "Premium"],
        "loyalty_tiers": ["Bronze", "Silver", "Gold", "Platinum"]
    }
}


def get_schema_prompt() -> str:
    """Generate schema description for LLM prompts"""
    lines = ["# Retail Analytics Database Schema\n"]
    
    for table_name, table_info in RETAIL_SCHEMA["tables"].items():
        lines.append(f"## {table_name}")
        lines.append(f"{table_info['description']}\n")
        lines.append("Columns:")
        
        for col_name, col_info in table_info["columns"].items():
            pk = " (PK)" if col_info.get("pk") else ""
            lines.append(f"  - {col_name}: {col_info['type']}{pk} - {col_info['description']}")
        
        lines.append("")
    
    lines.append("## Relationships")
    for rel in RETAIL_SCHEMA["relationships"]:
        lines.append(f"  - {rel['from']} -> {rel['to']} ({rel['type']})")
    
    lines.append("\n## Business Rules")
    for rule_name, rule_value in RETAIL_SCHEMA["business_rules"].items():
        lines.append(f"  - {rule_name}: {rule_value}")
    
    return "\n".join(lines)


def get_table_ddl(table_name: str) -> str:
    """Generate DDL-style schema for a specific table"""
    if table_name not in RETAIL_SCHEMA["tables"]:
        return f"-- Table {table_name} not found"
    
    table = RETAIL_SCHEMA["tables"][table_name]
    columns = []
    
    for col_name, col_info in table["columns"].items():
        col_def = f"  {col_name} {col_info['type']}"
        if col_info.get("pk"):
            col_def += " PRIMARY KEY"
        columns.append(col_def)
    
    ddl = f"-- {table['description']}\n"
    ddl += f"CREATE TABLE {table_name} (\n"
    ddl += ",\n".join(columns)
    ddl += "\n);"
    
    return ddl
