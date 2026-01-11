"""
A2A Protocol Server Implementation
Base class for all agents following the Agent2Agent protocol
"""

import asyncio
import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional, Callable
from enum import Enum
from dataclasses import dataclass, field, asdict
import json
import logging

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TaskStatus(str, Enum):
    SUBMITTED = "submitted"
    WORKING = "working"
    COMPLETED = "completed"
    FAILED = "failed"
    INPUT_REQUIRED = "input-required"
    CANCELLED = "cancelled"


@dataclass
class AgentSkill:
    """Defines a capability that an agent can perform"""
    id: str
    name: str
    description: str
    input_schema: Dict[str, Any]
    output_schema: Dict[str, Any]
    examples: List[str] = field(default_factory=list)


@dataclass
class AgentCard:
    """A2A Agent Card - describes agent capabilities"""
    name: str
    version: str
    description: str
    skills: List[AgentSkill]
    url: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "url": self.url,
            "skills": [asdict(s) for s in self.skills]
        }


@dataclass 
class Artifact:
    """Output artifact from a task"""
    type: str  # "text", "json", "sql", "code", "image"
    content: Any
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TaskResponse:
    """Response object for A2A tasks"""
    task_id: str
    status: TaskStatus
    artifacts: List[Artifact] = field(default_factory=list)
    error: Optional[str] = None
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "status": self.status.value,
            "artifacts": [asdict(a) for a in self.artifacts],
            "error": self.error,
            "created_at": self.created_at,
            "updated_at": self.updated_at
        }


class TaskRequest(BaseModel):
    """Incoming task request"""
    skill_id: str
    parameters: Dict[str, Any]
    context: Optional[Dict[str, Any]] = None


class A2AServer:
    """
    Base class for A2A-compliant agents.
    Subclass this and implement skill handlers.
    """
    
    def __init__(
        self,
        name: str,
        version: str,
        description: str,
        skills: List[AgentSkill],
        port: int = 8000,
        host: str = "0.0.0.0"
    ):
        self.name = name
        self.version = version
        self.description = description
        self.skills = {s.id: s for s in skills}
        self.port = port
        self.host = host
        
        # Task storage
        self.tasks: Dict[str, TaskResponse] = {}
        
        # Skill handlers
        self.handlers: Dict[str, Callable] = {}
        
        # FastAPI app
        self.app = FastAPI(title=name, version=version)
        self._setup_routes()
        
        # Agent card
        self.agent_card = AgentCard(
            name=name,
            version=version,
            description=description,
            skills=skills,
            url=f"http://{host}:{port}"
        )
    
    def _setup_routes(self):
        """Setup A2A protocol routes"""
        
        @self.app.get("/.well-known/agent.json")
        async def get_agent_card():
            """Return agent card (A2A discovery endpoint)"""
            return self.agent_card.to_dict()
        
        @self.app.get("/health")
        async def health_check():
            """Health check endpoint"""
            return {"status": "healthy", "agent": self.name}
        
        @self.app.post("/tasks")
        async def create_task(request: TaskRequest, background_tasks: BackgroundTasks):
            """Create a new task (A2A task submission)"""
            if request.skill_id not in self.skills:
                raise HTTPException(
                    status_code=400,
                    detail=f"Unknown skill: {request.skill_id}. Available: {list(self.skills.keys())}"
                )
            
            task_id = str(uuid.uuid4())
            task = TaskResponse(
                task_id=task_id,
                status=TaskStatus.SUBMITTED
            )
            self.tasks[task_id] = task
            
            # Execute task in background
            background_tasks.add_task(
                self._execute_task,
                task_id,
                request.skill_id,
                request.parameters,
                request.context
            )
            
            return task.to_dict()
        
        @self.app.get("/tasks/{task_id}")
        async def get_task_status(task_id: str):
            """Get task status (A2A task polling)"""
            if task_id not in self.tasks:
                raise HTTPException(status_code=404, detail="Task not found")
            return self.tasks[task_id].to_dict()
        
        @self.app.post("/tasks/{task_id}/cancel")
        async def cancel_task(task_id: str):
            """Cancel a task"""
            if task_id not in self.tasks:
                raise HTTPException(status_code=404, detail="Task not found")
            
            task = self.tasks[task_id]
            if task.status in [TaskStatus.SUBMITTED, TaskStatus.WORKING]:
                task.status = TaskStatus.CANCELLED
                task.updated_at = datetime.utcnow().isoformat()
            
            return task.to_dict()
        
        @self.app.post("/tasks/sync")
        async def create_task_sync(request: TaskRequest):
            """Synchronous task execution (waits for completion)"""
            if request.skill_id not in self.skills:
                raise HTTPException(
                    status_code=400,
                    detail=f"Unknown skill: {request.skill_id}"
                )
            
            task_id = str(uuid.uuid4())
            task = TaskResponse(
                task_id=task_id,
                status=TaskStatus.SUBMITTED
            )
            self.tasks[task_id] = task
            
            # Execute synchronously
            await self._execute_task(
                task_id,
                request.skill_id,
                request.parameters,
                request.context
            )
            
            return self.tasks[task_id].to_dict()
    
    async def _execute_task(
        self,
        task_id: str,
        skill_id: str,
        parameters: Dict[str, Any],
        context: Optional[Dict[str, Any]]
    ):
        """Execute a task using the registered handler"""
        task = self.tasks[task_id]
        task.status = TaskStatus.WORKING
        task.updated_at = datetime.utcnow().isoformat()
        
        try:
            if skill_id not in self.handlers:
                raise ValueError(f"No handler registered for skill: {skill_id}")
            
            handler = self.handlers[skill_id]
            result = await handler(parameters, context)
            
            # Convert result to artifacts
            if isinstance(result, list):
                task.artifacts = result
            elif isinstance(result, Artifact):
                task.artifacts = [result]
            elif isinstance(result, dict):
                task.artifacts = [Artifact(type="json", content=result)]
            elif isinstance(result, str):
                task.artifacts = [Artifact(type="text", content=result)]
            else:
                task.artifacts = [Artifact(type="json", content=str(result))]
            
            task.status = TaskStatus.COMPLETED
            
        except Exception as e:
            logger.error(f"Task {task_id} failed: {e}", exc_info=True)
            task.status = TaskStatus.FAILED
            task.error = str(e)
        
        task.updated_at = datetime.utcnow().isoformat()
    
    def register_handler(self, skill_id: str, handler: Callable):
        """Register a handler function for a skill"""
        if skill_id not in self.skills:
            raise ValueError(f"Unknown skill: {skill_id}")
        self.handlers[skill_id] = handler
        logger.info(f"Registered handler for skill: {skill_id}")
    
    def start(self):
        """Start the agent server"""
        logger.info(f"Starting {self.name} on {self.host}:{self.port}")
        uvicorn.run(self.app, host=self.host, port=self.port, log_level="info")
    
    async def start_async(self):
        """Start the agent server asynchronously"""
        config = uvicorn.Config(
            self.app, 
            host=self.host, 
            port=self.port, 
            log_level="info"
        )
        server = uvicorn.Server(config)
        await server.serve()
