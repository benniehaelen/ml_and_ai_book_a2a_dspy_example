"""
SQL Generation Agent
Converts natural language requirements to SQL queries
Uses DSPy for optimized text-to-SQL conversion
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import dspy
import logging
from typing import Dict, Any, List, Optional

from utils.a2a_server import A2AServer, AgentSkill, Artifact
from utils.database import get_connector
from config.schemas import RETAIL_SCHEMA, get_schema_prompt

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =============================================================================
# DSPy Signatures and Modules
# =============================================================================

class TextToSQL(dspy.Signature):
    """Convert natural language requirements to SQL queries"""
    
    requirement = dspy.InputField(desc="Natural language description of query")
    db_schema = dspy.InputField(desc="Database schema information")
    business_rules = dspy.InputField(desc="Business logic and calculation rules")
    
    sql_query = dspy.OutputField(desc="Complete, executable SQL query")
    explanation = dspy.OutputField(desc="Explanation of the query logic")


class SQLGenerationProgram(dspy.Module):
    """DSPy program for SQL generation"""
    
    def __init__(self):
        super().__init__()
        self.generate = dspy.ChainOfThought(TextToSQL)
    
    def forward(
        self, 
        requirement: str, 
        db_schema: str, 
        business_rules: str = ""
    ) -> dspy.Prediction:
        return self.generate(
            requirement=requirement,
            db_schema=db_schema,
            business_rules=business_rules
        )


# =============================================================================
# SQL Generation Agent
# =============================================================================

class SQLGenerationAgent(A2AServer):
    """
    Agent that generates SQL queries from natural language.
    Optimized with DSPy for high accuracy.
    """
    
    def __init__(self, port: int = 8002):
        # Define skills
        skills = [
            AgentSkill(
                id="text-to-sql",
                name="Text to SQL Conversion",
                description="Convert natural language requirements into optimized SQL queries",
                input_schema={
                    "requirement": "string - Natural language query requirement",
                    "tables": "array - List of relevant tables (optional)",
                    "schema": "object - Schema information (optional)",
                    "execute": "boolean - Whether to execute the query (default: false)"
                },
                output_schema={
                    "sql": "string - Generated SQL query",
                    "explanation": "string - Query explanation",
                    "results": "array - Query results if executed"
                },
                examples=[
                    "Find total revenue by region for the last 30 days",
                    "Show top 10 products by sales",
                    "Compare this month's sales to last month"
                ]
            ),
            AgentSkill(
                id="validate-sql",
                name="Validate SQL Query",
                description="Validate SQL syntax and check for errors",
                input_schema={
                    "sql": "string - SQL query to validate"
                },
                output_schema={
                    "valid": "boolean - Whether query is valid",
                    "error": "string - Error message if invalid"
                },
                examples=[
                    "SELECT * FROM sales",
                    "SELECT revenue FROM invalid_table"
                ]
            )
        ]
        
        super().__init__(
            name="SQL Generation Agent",
            version="1.0.0",
            description="Generates optimized SQL queries from natural language",
            skills=skills,
            port=port
        )
        
        # Initialize DSPy
        self._init_dspy()
        
        # Initialize DSPy program
        self.sql_program = SQLGenerationProgram()
        
        # Database connector (lazy initialization)
        self._db = None
        
        # Register handlers
        self.register_handler("text-to-sql", self._handle_text_to_sql)
        self.register_handler("validate-sql", self._handle_validate_sql)
    
    def _init_dspy(self):
        """Initialize DSPy with LLM"""
        try:
            from dotenv import load_dotenv
            load_dotenv()
            
            api_key = os.getenv('OPENAI_API_KEY')
            if api_key:
                lm = dspy.LM('openai/gpt-4o-mini', api_key=api_key)
                dspy.configure(lm=lm)
                logger.info("✓ SQL Generation Agent: DSPy configured with OpenAI")
            else:
                logger.warning("⚠ OPENAI_API_KEY not found, using template mode")
        except Exception as e:
            logger.warning(f"⚠ DSPy configuration warning: {e}")
    
    @property
    def db(self):
        """Lazy database connector initialization"""
        if self._db is None:
            try:
                self._db = get_connector()
            except Exception as e:
                logger.warning(f"Database not available: {e}")
        return self._db
    
    def _build_schema_prompt(self, tables: List[str] = None) -> str:
        """Build schema prompt for specified tables"""
        if not tables:
            return get_schema_prompt()
        
        lines = ["# Database Schema\n"]
        
        for table_name in tables:
            if table_name in RETAIL_SCHEMA["tables"]:
                table = RETAIL_SCHEMA["tables"][table_name]
                lines.append(f"## {table_name}")
                lines.append(f"{table['description']}\n")
                lines.append("Columns:")
                
                for col_name, col_info in table["columns"].items():
                    pk = " (PK)" if col_info.get("pk") else ""
                    lines.append(f"  - {col_name}: {col_info['type']}{pk}")
                
                lines.append("")
        
        # Add relevant relationships
        lines.append("## Relationships")
        for rel in RETAIL_SCHEMA["relationships"]:
            from_table = rel["from"].split(".")[0]
            to_table = rel["to"].split(".")[0]
            if from_table in tables or to_table in tables:
                lines.append(f"  - {rel['from']} -> {rel['to']}")
        
        return "\n".join(lines)
    
    def _get_business_rules(self) -> str:
        """Get business rules for SQL generation"""
        rules = RETAIL_SCHEMA.get("business_rules", {})
        return "\n".join([f"- {k}: {v}" for k, v in rules.items()])
    
    async def _handle_text_to_sql(
        self,
        parameters: Dict[str, Any],
        context: Optional[Dict[str, Any]]
    ) -> List[Artifact]:
        """Handle text-to-SQL conversion"""
        requirement = parameters.get("requirement", "")
        tables = parameters.get("tables", [])
        schema_override = parameters.get("schema")
        execute = parameters.get("execute", False)
        
        if not requirement:
            raise ValueError("Requirement is required")
        
        logger.info(f"Generating SQL for: {requirement[:100]}...")
        
        # Build schema prompt
        if schema_override:
            schema_prompt = str(schema_override)
        else:
            schema_prompt = self._build_schema_prompt(tables if tables else None)
        
        business_rules = self._get_business_rules()
        
        try:
            # Use DSPy program
            result = self.sql_program(
                requirement=requirement,
                db_schema=schema_prompt,
                business_rules=business_rules
            )
            
            sql_query = result.sql_query.strip()
            explanation = result.explanation
            
            # Clean up SQL (remove markdown code blocks if present)
            if sql_query.startswith("```"):
                sql_query = sql_query.split("```")[1]
                if sql_query.startswith("sql"):
                    sql_query = sql_query[3:]
                sql_query = sql_query.strip()
            
            response = {
                "sql": sql_query,
                "explanation": explanation,
                "results": None
            }
            
            # Execute if requested
            if execute and self.db:
                try:
                    df = self.db.execute_query(sql_query)
                    response["results"] = df.to_dict(orient="records")[:100]  # Limit results
                    response["row_count"] = len(df)
                except Exception as e:
                    response["execution_error"] = str(e)
            
            return [Artifact(type="json", content=response)]
            
        except Exception as e:
            logger.error(f"SQL generation failed: {e}")
            raise
    
    async def _handle_validate_sql(
        self,
        parameters: Dict[str, Any],
        context: Optional[Dict[str, Any]]
    ) -> List[Artifact]:
        """Handle SQL validation"""
        sql = parameters.get("sql", "")
        
        if not sql:
            raise ValueError("SQL query is required")
        
        # Try to validate by explaining the query
        if self.db:
            try:
                # SQLite doesn't have EXPLAIN ANALYZE, but we can prepare
                self.db.execute_query(f"EXPLAIN QUERY PLAN {sql}")
                return [Artifact(type="json", content={
                    "valid": True,
                    "error": None
                })]
            except Exception as e:
                return [Artifact(type="json", content={
                    "valid": False,
                    "error": str(e)
                })]
        
        # Without DB, do basic syntax check
        sql_upper = sql.upper()
        if not any(sql_upper.startswith(kw) for kw in ["SELECT", "WITH"]):
            return [Artifact(type="json", content={
                "valid": False,
                "error": "Query must start with SELECT or WITH"
            })]
        
        return [Artifact(type="json", content={
            "valid": True,
            "error": None,
            "note": "Syntax check only - database validation not available"
        })]


# =============================================================================
# Main Entry Point
# =============================================================================

if __name__ == "__main__":
    agent = SQLGenerationAgent(port=8002)
    agent.start()
