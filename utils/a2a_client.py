"""
A2A Protocol Client Implementation
Used by agents to communicate with other agents
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class AgentInfo:
    """Information about a discovered agent"""
    name: str
    url: str
    version: str
    description: str
    skills: List[Dict[str, Any]]


class A2AClient:
    """
    Client for A2A protocol communication.
    Handles discovery, task submission, and result polling.
    """
    
    def __init__(
        self,
        registry_url: Optional[str] = None,
        timeout: float = 30.0,
        poll_interval: float = 0.5,
        max_poll_attempts: int = 120
    ):
        self.registry_url = registry_url
        self.timeout = timeout
        self.poll_interval = poll_interval
        self.max_poll_attempts = max_poll_attempts
        
        # Cache discovered agents
        self.agents: Dict[str, AgentInfo] = {}
        
        # HTTP client
        self.client = httpx.AsyncClient(timeout=timeout)
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def discover_agent(self, agent_url: str) -> AgentInfo:
        """
        Discover an agent by fetching its agent card.
        
        Args:
            agent_url: Base URL of the agent
            
        Returns:
            AgentInfo with agent capabilities
        """
        discovery_url = f"{agent_url.rstrip('/')}/.well-known/agent.json"
        
        try:
            response = await self.client.get(discovery_url)
            response.raise_for_status()
            data = response.json()
            
            agent = AgentInfo(
                name=data["name"],
                url=agent_url,
                version=data.get("version", "1.0.0"),
                description=data.get("description", ""),
                skills=data.get("skills", [])
            )
            
            self.agents[agent.name] = agent
            logger.info(f"Discovered agent: {agent.name} at {agent_url}")
            
            return agent
            
        except Exception as e:
            logger.error(f"Failed to discover agent at {agent_url}: {e}")
            raise
    
    async def get_agent_skills(self, agent_url: str) -> List[Dict[str, Any]]:
        """Get list of skills for an agent"""
        agent = await self.discover_agent(agent_url)
        return agent.skills
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def send_task(
        self,
        agent_url: str,
        skill_id: str,
        parameters: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
        wait_for_completion: bool = True
    ) -> Dict[str, Any]:
        """
        Send a task to an agent.
        
        Args:
            agent_url: Base URL of the target agent
            skill_id: ID of the skill to invoke
            parameters: Parameters for the skill
            context: Optional context information
            wait_for_completion: Whether to poll until task completes
            
        Returns:
            Task response with status and artifacts
        """
        task_url = f"{agent_url.rstrip('/')}/tasks"
        
        payload = {
            "skill_id": skill_id,
            "parameters": parameters,
            "context": context
        }
        
        try:
            # Submit task
            if wait_for_completion:
                # Use sync endpoint
                response = await self.client.post(
                    f"{task_url}/sync",
                    json=payload,
                    timeout=120.0  # Longer timeout for sync
                )
            else:
                response = await self.client.post(task_url, json=payload)
            
            response.raise_for_status()
            result = response.json()
            
            if not wait_for_completion:
                # Poll for completion
                result = await self._poll_task(
                    agent_url, 
                    result["task_id"]
                )
            
            return result
            
        except httpx.TimeoutException:
            logger.error(f"Timeout sending task to {agent_url}")
            raise
        except Exception as e:
            logger.error(f"Failed to send task to {agent_url}: {e}")
            raise
    
    async def _poll_task(
        self,
        agent_url: str,
        task_id: str
    ) -> Dict[str, Any]:
        """Poll for task completion"""
        status_url = f"{agent_url.rstrip('/')}/tasks/{task_id}"
        
        for _ in range(self.max_poll_attempts):
            try:
                response = await self.client.get(status_url)
                response.raise_for_status()
                result = response.json()
                
                status = result.get("status")
                if status in ["completed", "failed", "cancelled"]:
                    return result
                
                await asyncio.sleep(self.poll_interval)
                
            except Exception as e:
                logger.warning(f"Poll attempt failed: {e}")
                await asyncio.sleep(self.poll_interval)
        
        raise TimeoutError(f"Task {task_id} did not complete in time")
    
    async def get_task_status(
        self,
        agent_url: str,
        task_id: str
    ) -> Dict[str, Any]:
        """Get current status of a task"""
        status_url = f"{agent_url.rstrip('/')}/tasks/{task_id}"
        response = await self.client.get(status_url)
        response.raise_for_status()
        return response.json()
    
    async def cancel_task(
        self,
        agent_url: str,
        task_id: str
    ) -> Dict[str, Any]:
        """Cancel a running task"""
        cancel_url = f"{agent_url.rstrip('/')}/tasks/{task_id}/cancel"
        response = await self.client.post(cancel_url)
        response.raise_for_status()
        return response.json()
    
    async def health_check(self, agent_url: str) -> bool:
        """Check if an agent is healthy"""
        try:
            response = await self.client.get(
                f"{agent_url.rstrip('/')}/health",
                timeout=5.0
            )
            return response.status_code == 200
        except Exception:
            return False


class AgentRegistry:
    """
    Simple in-memory agent registry for development.
    In production, use a distributed registry (e.g., Consul, etcd).
    """
    
    def __init__(self):
        self.agents: Dict[str, str] = {}  # name -> url
    
    def register(self, name: str, url: str):
        """Register an agent"""
        self.agents[name] = url
        logger.info(f"Registered agent: {name} at {url}")
    
    def unregister(self, name: str):
        """Unregister an agent"""
        if name in self.agents:
            del self.agents[name]
            logger.info(f"Unregistered agent: {name}")
    
    def get_url(self, name: str) -> Optional[str]:
        """Get URL for an agent by name"""
        return self.agents.get(name)
    
    def list_agents(self) -> Dict[str, str]:
        """List all registered agents"""
        return self.agents.copy()
