"""
Orchestrator Agent
Coordinates multiple specialized agents to answer complex queries
Uses DSPy for task decomposition and planning
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import dspy
import asyncio
import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

from utils.a2a_server import A2AServer, AgentSkill, Artifact
from utils.a2a_client import A2AClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =============================================================================
# DSPy Signatures and Modules
# =============================================================================

class QueryDecomposition(dspy.Signature):
    """Break down complex queries into subtasks"""
    
    user_question = dspy.InputField(desc="The user's natural language question")
    business_context = dspy.InputField(desc="Context about the business domain")
    
    task_plan = dspy.OutputField(desc="Numbered list of subtasks to execute")
    required_skills = dspy.OutputField(desc="Comma-separated list: discover-tables, text-to-sql, analysis, synthesis")


class ResultSynthesis(dspy.Signature):
    """Synthesize results from multiple agents into a coherent response"""
    
    original_question = dspy.InputField(desc="The user's original question")
    agent_results = dspy.InputField(desc="Results from various agents")
    
    answer = dspy.OutputField(desc="Comprehensive answer to the user's question")
    recommendations = dspy.OutputField(desc="Actionable recommendations based on findings")
    confidence = dspy.OutputField(desc="Confidence level: high, medium, or low")


class OrchestratorProgram(dspy.Module):
    """DSPy program for query orchestration"""
    
    def __init__(self):
        super().__init__()
        self.decompose = dspy.ChainOfThought(QueryDecomposition)
        self.synthesize = dspy.ChainOfThought(ResultSynthesis)
    
    def plan(self, user_question: str, business_context: str) -> dspy.Prediction:
        return self.decompose(
            user_question=user_question,
            business_context=business_context
        )
    
    def synthesize_results(
        self, 
        original_question: str, 
        agent_results: str
    ) -> dspy.Prediction:
        return self.synthesize(
            original_question=original_question,
            agent_results=agent_results
        )


# =============================================================================
# Agent Registry
# =============================================================================

@dataclass
class AgentEndpoint:
    """Registered agent endpoint"""
    name: str
    url: str
    skills: List[str]


# =============================================================================
# Orchestrator Agent
# =============================================================================

class OrchestratorAgent(A2AServer):
    """
    Main orchestrator that coordinates other agents.
    Decomposes complex queries and aggregates results.
    """
    
    # Business context for RetailCo
    BUSINESS_CONTEXT = """
    RetailCo is a national retail chain with:
    - 500 stores across 4 regions (Northeast, Southeast, Midwest, West)
    - 5 product lines: Electronics, Apparel, Home, Outdoor, Food
    - 3 price tiers: Budget, Standard, Premium
    - 10M active customers with 4 loyalty tiers (Bronze, Silver, Gold, Platinum)
    - Data lakehouse with real-time sales and inventory data
    - Marketing campaigns tracked by region and product line
    - External data: weather, competitor pricing
    """
    
    def __init__(self, port: int = 8000):
        # Define skills
        skills = [
            AgentSkill(
                id="answer-question",
                name="Answer Business Question",
                description="Answer complex business questions by coordinating multiple agents",
                input_schema={
                    "question": "string - The business question to answer",
                    "context": "object - Additional context (optional)"
                },
                output_schema={
                    "answer": "string - Comprehensive answer",
                    "recommendations": "array - Actionable recommendations",
                    "supporting_data": "object - Data supporting the answer",
                    "confidence": "string - Confidence level"
                },
                examples=[
                    "Why are sales declining for outdoor products in the Northeast?",
                    "What are our best-selling products this quarter?",
                    "Which stores are underperforming?"
                ]
            ),
            AgentSkill(
                id="plan-analysis",
                name="Plan Analysis",
                description="Create an analysis plan without executing it",
                input_schema={
                    "question": "string - The question to plan for"
                },
                output_schema={
                    "plan": "string - Analysis plan",
                    "required_agents": "array - Agents needed"
                },
                examples=[
                    "Plan analysis for sales trends",
                    "How would you investigate customer churn?"
                ]
            )
        ]
        
        super().__init__(
            name="Orchestrator Agent",
            version="1.0.0",
            description="Coordinates analytics agents to answer complex business questions",
            skills=skills,
            port=port
        )
        
        # Initialize DSPy
        self._init_dspy()
        
        # Initialize DSPy program
        self.orchestrator_program = OrchestratorProgram()
        
        # A2A client for calling other agents
        self.a2a_client = A2AClient()
        
        # Registered agents
        self.agents: Dict[str, AgentEndpoint] = {}
        
        # Register handlers
        self.register_handler("answer-question", self._handle_answer_question)
        self.register_handler("plan-analysis", self._handle_plan_analysis)
    
    def _init_dspy(self):
        """Initialize DSPy with LLM"""
        try:
            from dotenv import load_dotenv
            load_dotenv()
            
            api_key = os.getenv('OPENAI_API_KEY')
            if api_key:
                lm = dspy.LM('openai/gpt-4o-mini', api_key=api_key)
                dspy.configure(lm=lm)
                logger.info("✓ Orchestrator Agent: DSPy configured with OpenAI")
            else:
                logger.warning("⚠ OPENAI_API_KEY not found")
        except Exception as e:
            logger.warning(f"⚠ DSPy configuration warning: {e}")
    
    def register_agent(self, name: str, url: str, skills: List[str]):
        """Register an agent endpoint"""
        self.agents[name] = AgentEndpoint(name=name, url=url, skills=skills)
        logger.info(f"Registered agent: {name} at {url}")
    
    async def _call_agent(
        self, 
        agent_name: str, 
        skill_id: str, 
        parameters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Call another agent via A2A protocol"""
        if agent_name not in self.agents:
            raise ValueError(f"Unknown agent: {agent_name}")
        
        agent = self.agents[agent_name]
        
        try:
            result = await self.a2a_client.send_task(
                agent_url=agent.url,
                skill_id=skill_id,
                parameters=parameters,
                wait_for_completion=True
            )
            
            if result.get("status") == "completed":
                artifacts = result.get("artifacts", [])
                if artifacts:
                    return artifacts[0].get("content", {})
            
            return {"error": result.get("error", "Unknown error")}
            
        except Exception as e:
            logger.error(f"Failed to call {agent_name}: {e}")
            return {"error": str(e)}
    
    async def _handle_answer_question(
        self,
        parameters: Dict[str, Any],
        context: Optional[Dict[str, Any]]
    ) -> List[Artifact]:
        """Handle complex question answering"""
        question = parameters.get("question", "")
        additional_context = parameters.get("context", {})
        
        if not question:
            raise ValueError("Question is required")
        
        logger.info(f"Processing question: {question[:100]}...")
        
        results = {
            "question": question,
            "steps": [],
            "agent_results": {}
        }
        
        try:
            # Step 1: Decompose the question
            plan_result = self.orchestrator_program.plan(
                user_question=question,
                business_context=self.BUSINESS_CONTEXT
            )
            
            results["plan"] = plan_result.task_plan
            required_skills = [
                s.strip() 
                for s in plan_result.required_skills.split(",")
            ]
            
            logger.info(f"Plan created, required skills: {required_skills}")
            results["steps"].append({
                "step": "planning",
                "result": "Created analysis plan"
            })
            
            # Step 2: Discover relevant tables
            if "discover-tables" in required_skills and "data_discovery" in self.agents:
                discovery_result = await self._call_agent(
                    "data_discovery",
                    "discover-tables",
                    {"question": question}
                )
                results["agent_results"]["data_discovery"] = discovery_result
                results["steps"].append({
                    "step": "data_discovery",
                    "tables": discovery_result.get("tables", [])
                })
            
            # Step 3: Generate and execute SQL
            if "text-to-sql" in required_skills and "sql_generation" in self.agents:
                tables = results["agent_results"].get("data_discovery", {}).get("tables", [])
                
                sql_result = await self._call_agent(
                    "sql_generation",
                    "text-to-sql",
                    {
                        "requirement": question,
                        "tables": tables,
                        "execute": True
                    }
                )
                results["agent_results"]["sql_generation"] = sql_result
                results["steps"].append({
                    "step": "sql_generation",
                    "sql": sql_result.get("sql", ""),
                    "row_count": sql_result.get("row_count", 0)
                })
            
            # Step 4: Synthesize results
            agent_results_str = str(results["agent_results"])
            
            synthesis = self.orchestrator_program.synthesize_results(
                original_question=question,
                agent_results=agent_results_str
            )
            
            results["answer"] = synthesis.answer
            results["recommendations"] = synthesis.recommendations
            results["confidence"] = synthesis.confidence
            
            return [Artifact(type="json", content=results)]
            
        except Exception as e:
            logger.error(f"Question processing failed: {e}")
            results["error"] = str(e)
            return [Artifact(type="json", content=results)]
    
    async def _handle_plan_analysis(
        self,
        parameters: Dict[str, Any],
        context: Optional[Dict[str, Any]]
    ) -> List[Artifact]:
        """Create analysis plan without executing"""
        question = parameters.get("question", "")
        
        if not question:
            raise ValueError("Question is required")
        
        try:
            plan_result = self.orchestrator_program.plan(
                user_question=question,
                business_context=self.BUSINESS_CONTEXT
            )
            
            required_skills = [
                s.strip() 
                for s in plan_result.required_skills.split(",")
            ]
            
            return [Artifact(type="json", content={
                "plan": plan_result.task_plan,
                "required_agents": required_skills
            })]
            
        except Exception as e:
            logger.error(f"Planning failed: {e}")
            raise


# =============================================================================
# Standalone Orchestrator (without A2A)
# =============================================================================

class StandaloneOrchestrator:
    """
    Simplified orchestrator for demo purposes.
    Directly imports and uses agent classes.
    """
    
    BUSINESS_CONTEXT = OrchestratorAgent.BUSINESS_CONTEXT
    
    def __init__(self):
        from dotenv import load_dotenv
        load_dotenv()
        
        # Initialize DSPy
        api_key = os.getenv('OPENAI_API_KEY')
        if api_key:
            lm = dspy.LM('openai/gpt-4o-mini', api_key=api_key)
            dspy.configure(lm=lm)
            logger.info("✓ Standalone Orchestrator: DSPy configured")
        
        self.program = OrchestratorProgram()
        
        # Import agent classes
        from agents.data_discovery_agent import DataDiscoveryProgram
        from agents.sql_generation_agent import SQLGenerationProgram
        
        self.discovery_program = DataDiscoveryProgram()
        self.sql_program = SQLGenerationProgram()
        
        # Database
        from utils.database import get_connector
        try:
            self.db = get_connector()
        except:
            self.db = None
    
    async def process_query(self, question: str) -> Dict[str, Any]:
        """Process a user query end-to-end"""
        results = {"question": question, "steps": []}
        
        # Step 1: Plan
        logger.info("Step 1: Planning...")
        plan = self.program.plan(
            user_question=question,
            business_context=self.BUSINESS_CONTEXT
        )
        results["plan"] = plan.task_plan
        results["steps"].append({"step": "planning", "status": "complete"})
        
        # Step 2: Discover tables
        logger.info("Step 2: Discovering tables...")
        from config.schemas import RETAIL_SCHEMA
        
        table_descriptions = "\n".join([
            f"- {name}: {info['description']}"
            for name, info in RETAIL_SCHEMA["tables"].items()
        ])
        
        discovery = self.discovery_program(
            question=question,
            available_tables=table_descriptions
        )
        
        tables = [t.strip() for t in discovery.relevant_tables.split(",")]
        results["tables"] = tables
        results["steps"].append({
            "step": "discovery", 
            "tables": tables,
            "status": "complete"
        })
        
        # Step 3: Generate SQL
        logger.info("Step 3: Generating SQL...")
        from config.schemas import get_schema_prompt
        
        sql_result = self.sql_program(
            requirement=question,
            schema=get_schema_prompt(),
            business_rules=""
        )
        
        sql_query = sql_result.sql_query.strip()
        if sql_query.startswith("```"):
            sql_query = sql_query.split("```")[1]
            if sql_query.startswith("sql"):
                sql_query = sql_query[3:]
            sql_query = sql_query.strip()
        
        results["sql"] = sql_query
        results["sql_explanation"] = sql_result.explanation
        results["steps"].append({
            "step": "sql_generation",
            "sql": sql_query[:100] + "...",
            "status": "complete"
        })
        
        # Step 4: Execute SQL
        if self.db:
            logger.info("Step 4: Executing SQL...")
            try:
                df = self.db.execute_query(sql_query)
                results["data"] = df.to_dict(orient="records")[:20]
                results["row_count"] = len(df)
                results["steps"].append({
                    "step": "execution",
                    "row_count": len(df),
                    "status": "complete"
                })
            except Exception as e:
                results["execution_error"] = str(e)
                results["steps"].append({
                    "step": "execution",
                    "error": str(e),
                    "status": "failed"
                })
        
        # Step 5: Synthesize
        logger.info("Step 5: Synthesizing results...")
        synthesis = self.program.synthesize_results(
            original_question=question,
            agent_results=str(results)
        )
        
        results["answer"] = synthesis.answer
        results["recommendations"] = synthesis.recommendations
        results["confidence"] = synthesis.confidence
        results["steps"].append({"step": "synthesis", "status": "complete"})
        
        return results


# =============================================================================
# Main Entry Point
# =============================================================================

if __name__ == "__main__":
    # Run as A2A server
    agent = OrchestratorAgent(port=8000)
    
    # Register other agents (adjust ports as needed)
    agent.register_agent("data_discovery", "http://localhost:8001", ["discover-tables"])
    agent.register_agent("sql_generation", "http://localhost:8002", ["text-to-sql"])
    
    agent.start()
