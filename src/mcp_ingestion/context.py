"""
Local file system MCP context storage
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict
import structlog
from typing import Any, Dict, List

logger = structlog.get_logger()


class LocalMCPContext:
    """Local file system context storage"""
    
    def __init__(self, context_dir: str = "./mcp_context"):
        self.context_dir = Path(context_dir)
        self.context_dir.mkdir(parents=True, exist_ok=True)
        
        # Create .gitignore to avoid committing context data
        gitignore_path = self.context_dir / ".gitignore"
        if not gitignore_path.exists():
            with open(gitignore_path, 'w') as f:
                f.write("# MCP Context Data - Don't commit\n*\n!.gitignore\n")
        
        logger.info(f"MCP context storage initialized at: {self.context_dir}")
    
    def _get_context_file(self, agent_id: str, source_id: str) -> Path:
        """Get context file path for agent-source combination"""
        agent_dir = self.context_dir / agent_id
        agent_dir.mkdir(parents=True, exist_ok=True)
        return agent_dir / f"{source_id}.json"
    
    def get_context(self, agent_id: str, source_id: str) -> Dict[str, Any]:
        """Get agent context"""
        try:
            context_file = self._get_context_file(agent_id, source_id)
            
            if context_file.exists():
                with open(context_file, 'r') as f:
                    return json.load(f)
            else:
                return {}
                
        except Exception as e:
            logger.error("Failed to get context", agent_id=agent_id, source_id=source_id, error=str(e))
            return {}
    
    def update_context(self, agent_id: str, source_id: str, updates: Dict[str, Any]):
        """Update agent context"""
        try:
            context_file = self._get_context_file(agent_id, source_id)
            
            # Add metadata
            context_data = {
                **updates,
                "_metadata": {
                    "updated_at": datetime.now().isoformat(),
                    "agent_id": agent_id,
                    "source_id": source_id
                }
            }
            
            with open(context_file, 'w') as f:
                json.dump(context_data, f, indent=2)
            
            logger.debug("Updated context", agent_id=agent_id, source_id=source_id)
            
        except Exception as e:
            logger.error("Failed to update context", agent_id=agent_id, source_id=source_id, error=str(e))
    
    def list_contexts(self) -> Dict[str, List[str]]:
        """List all available contexts"""
        contexts = {}
        
        for agent_dir in self.context_dir.iterdir():
            if agent_dir.is_dir() and not agent_dir.name.startswith('.'):
                agent_id = agent_dir.name
                source_ids = []
                
                for context_file in agent_dir.glob("*.json"):
                    source_ids.append(context_file.stem)
                
                contexts[agent_id] = source_ids
        
        return contexts