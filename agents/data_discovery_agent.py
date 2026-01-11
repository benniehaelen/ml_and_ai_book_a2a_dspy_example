"""
Data Discovery Agent
Finds relevant tables in the data catalog based on user questions
Uses DSPy for intelligent table matching
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import dspy
import logging
from typing import Dict, Any, List, Optional

from utils.a2a_server import A2AServer, AgentSkill, Artifact
from config.schemas import RETAIL_SCHEMA, get_schema_prompt

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =============================================================================
# DSPy Signatures and Modules
# =============================================================================

class TableDiscovery(dspy.Signature):
    """Identify relevant database tables for a given question"""
    
    question = dspy.InputField(desc="User's natural language question")
    available_tables = dspy.InputField(desc="List of available tables with descriptions")
    
    relevant_tables = dspy.OutputField(desc="Comma-separated list of relevant table names")
    explanation = dspy.OutputField(desc="Brief explanation of why these tables are needed")
    join_hints = dspy.OutputField(desc="Suggested join keys between tables")


class DataDiscoveryProgram(dspy.Module):
    """DSPy program for discovering relevant tables"""
    
    def __init__(self):
        super().__init__()
        self.discover = dspy.ChainOfThought(TableDiscovery)
    
    def forward(self, question: str, available_tables: str) -> dspy.Prediction:
        return self.discover(
            question=question,
            available_tables=available_tables
        )


# =============================================================================
# Data Discovery Agent
# =============================================================================

class DataDiscoveryAgent(A2AServer):
    """
    Agent that discovers relevant tables for analytical questions.
    Exposes A2A-compliant endpoints for table discovery.
    """
    
    def __init__(self, port: int = 8001):
        # Define skills
        skills = [
            AgentSkill(
                id="discover-tables",
                name="Discover Relevant Tables",
                description="Find tables relevant to a user's analytical question",
                input_schema={
                    "question": "string - The user's question",
                    "context": "object - Optional additional context"
                },
                output_schema={
                    "tables": "array - List of relevant table names",
                    "explanation": "string - Why these tables are relevant",
                    "join_hints": "string - Suggested join keys",
                    "schema_details": "object - Detailed schema for each table"
                },
                examples=[
                    "What tables contain sales data?",
                    "Where can I find customer information?",
                    "Which tables have regional data?"
                ]
            ),
            AgentSkill(
                id="get-table-schema",
                name="Get Table Schema",
                description="Get detailed schema information for specific tables",
                input_schema={
                    "tables": "array - List of table names"
                },
                output_schema={
                    "schemas": "object - Schema details for each table"
                },
                examples=[
                    "Get schema for sales_transactions",
                    "Show columns in products_catalog"
                ]
            )
        ]
        
        super().__init__(
            name="Data Discovery Agent",
            version="1.0.0",
            description="Discovers relevant tables in the retail data lakehouse",
            skills=skills,
            port=port
        )
        
        # Initialize DSPy
        self._init_dspy()
        
        # Initialize DSPy program
        self.discovery_program = DataDiscoveryProgram()
        
        # Register handlers
        self.register_handler("discover-tables", self._handle_discover_tables)
        self.register_handler("get-table-schema", self._handle_get_schema)
        
        # Prepare table descriptions
        self.table_descriptions = self._prepare_table_descriptions()
    
    def _init_dspy(self):
        """Initialize DSPy with LLM"""
        try:
            from dotenv import load_dotenv
            load_dotenv()
            
            api_key = os.getenv('OPENAI_API_KEY')
            if api_key:
                lm = dspy.LM('openai/gpt-4o-mini', api_key=api_key)
                dspy.configure(lm=lm)
                logger.info("✓ Data Discovery Agent: DSPy configured with OpenAI")
            else:
                logger.warning("⚠ OPENAI_API_KEY not found, using mock mode")
        except Exception as e:
            logger.warning(f"⚠ DSPy configuration warning: {e}")
    
    def _prepare_table_descriptions(self) -> str:
        """Prepare table descriptions for DSPy prompt"""
        descriptions = []
        
        for table_name, table_info in RETAIL_SCHEMA["tables"].items():
            cols = ", ".join(table_info["columns"].keys())
            descriptions.append(
                f"- {table_name}: {table_info['description']} "
                f"(Columns: {cols})"
            )
        
        return "\n".join(descriptions)
    
    async def _handle_discover_tables(
        self,
        parameters: Dict[str, Any],
        context: Optional[Dict[str, Any]]
    ) -> List[Artifact]:
        """Handle table discovery request"""
        question = parameters.get("question", "")
        
        if not question:
            raise ValueError("Question is required")
        
        logger.info(f"Discovering tables for: {question}")
        
        try:
            # Use DSPy program
            result = self.discovery_program(
                question=question,
                available_tables=self.table_descriptions
            )
            
            # Parse table names
            table_names = [
                t.strip() 
                for t in result.relevant_tables.split(",")
                if t.strip() in RETAIL_SCHEMA["tables"]
            ]
            
            # Get schema details for relevant tables
            schema_details = {}
            for table_name in table_names:
                if table_name in RETAIL_SCHEMA["tables"]:
                    schema_details[table_name] = RETAIL_SCHEMA["tables"][table_name]
            
            response = {
                "tables": table_names,
                "explanation": result.explanation,
                "join_hints": result.join_hints,
                "schema_details": schema_details
            }
            
            return [Artifact(type="json", content=response)]
            
        except Exception as e:
            logger.error(f"Discovery failed: {e}")
            # Fallback to keyword matching
            return await self._fallback_discovery(question)
    
    async def _fallback_discovery(self, question: str) -> List[Artifact]:
        """Fallback discovery using keyword matching"""
        question_lower = question.lower()
        relevant_tables = []
        
        # Simple keyword matching
        keywords_to_tables = {
            ("sale", "revenue", "transaction"): ["sales_transactions", "stores_locations", "products_catalog"],
            ("customer", "loyalty", "member"): ["customers_profiles"],
            ("product", "item", "catalog"): ["products_catalog"],
            ("store", "location", "region"): ["stores_locations"],
            ("inventory", "stock"): ["inventory_levels"],
            ("weather", "temperature"): ["weather_data"],
            ("campaign", "marketing", "promotion"): ["marketing_campaigns"],
            ("competitor", "pricing"): ["competitors_pricing"],
            ("outdoor",): ["products_catalog", "sales_transactions", "stores_locations"],
        }
        
        for keywords, tables in keywords_to_tables.items():
            if any(kw in question_lower for kw in keywords):
                relevant_tables.extend(tables)
        
        # Deduplicate
        relevant_tables = list(dict.fromkeys(relevant_tables))
        
        if not relevant_tables:
            relevant_tables = ["sales_transactions", "products_catalog", "stores_locations"]
        
        schema_details = {
            t: RETAIL_SCHEMA["tables"][t] 
            for t in relevant_tables 
            if t in RETAIL_SCHEMA["tables"]
        }
        
        response = {
            "tables": relevant_tables,
            "explanation": "Tables identified based on keyword matching",
            "join_hints": "Join on store_id, product_id as appropriate",
            "schema_details": schema_details
        }
        
        return [Artifact(type="json", content=response)]
    
    async def _handle_get_schema(
        self,
        parameters: Dict[str, Any],
        context: Optional[Dict[str, Any]]
    ) -> List[Artifact]:
        """Handle schema request"""
        tables = parameters.get("tables", [])
        
        if isinstance(tables, str):
            tables = [tables]
        
        schemas = {}
        for table_name in tables:
            if table_name in RETAIL_SCHEMA["tables"]:
                schemas[table_name] = RETAIL_SCHEMA["tables"][table_name]
            else:
                schemas[table_name] = {"error": f"Table {table_name} not found"}
        
        return [Artifact(type="json", content={"schemas": schemas})]


# =============================================================================
# Main Entry Point
# =============================================================================

if __name__ == "__main__":
    agent = DataDiscoveryAgent(port=8001)
    agent.start()
