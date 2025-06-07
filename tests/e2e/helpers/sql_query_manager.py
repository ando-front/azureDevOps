"""SQL Query Manager for E2E Tests"""

import os
import re
import logging
from typing import Dict, Optional, List
from pathlib import Path

logger = logging.getLogger(__name__)


class E2ESQLQueryManager:
    """E2E SQL Query Manager Class for managing external SQL queries"""
    
    def __init__(self, filename: Optional[str] = None):
        """Initialize the SQL Query Manager
        
        Args:
            filename: Optional SQL file name to load initially
        """
        # Path to SQL files from project root
        current_dir = Path(__file__).parent.parent.parent.parent
        self.base_path = current_dir / "sql" / "e2e_queries"
        
        self._query_cache: Dict[str, Dict[str, str]] = {}
        self._file_cache: Dict[str, str] = {}
        self._current_filename = filename
        
        # Load queries if filename is provided
        if filename:
            self.load_queries_from_file(filename)
        
        logger.info(f"E2ESQLQueryManager initialized with base_path: {self.base_path}")
    
    def load_queries_from_file(self, filename: str) -> Dict[str, str]:
        """Load queries from SQL file
        
        Args:
            filename: Name of the SQL file to load
            
        Returns:
            Dictionary of query_name: query_sql
        """
        file_path = self.base_path / filename
        
        if not file_path.exists():
            raise FileNotFoundError(f"SQL file not found: {file_path}")
        
        # Check file cache
        if filename in self._file_cache:
            logger.debug(f"Using cached content for {filename}")
            return self._query_cache[filename]
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Cache file content
            self._file_cache[filename] = content
            
            # Parse queries
            queries = self._parse_queries(content)
            self._query_cache[filename] = queries
            
            logger.info(f"Loaded {len(queries)} queries from {filename}")
            return queries
            
        except Exception as e:
            logger.error(f"Failed to load SQL file {filename}: {e}")
            raise
    
    def get_query(self, query_name: str, filename: str = None, **params) -> str:
        """Get SQL query by name
        
        Args:
            query_name: Name of the query to retrieve
            filename: Optional filename if not using default
            **params: Parameters to substitute in the query
            
        Returns:
            SQL query string with parameters substituted
        """
        # Use first cached file if filename not specified
        if filename is None:
            if not self._query_cache:
                raise ValueError("No SQL files loaded. Please specify filename or load a file first.")
            filename = next(iter(self._query_cache.keys()))
        
        # Load file if not cached
        if filename not in self._query_cache:
            self.load_queries_from_file(filename)
        
        # Get query
        if query_name not in self._query_cache[filename]:
            available_queries = list(self._query_cache[filename].keys())
            raise KeyError(f"Query '{query_name}' not found in {filename}. Available queries: {available_queries}")
        
        query = self._query_cache[filename][query_name]
        
        # Parameter substitution
        if params:
            query = self._substitute_parameters(query, params)
        
        return query
    
    def _parse_queries(self, content: str) -> Dict[str, str]:
        """Parse queries from SQL file content"""
        queries = {}
        current_query_name = None
        current_query_lines = []
        
        lines = content.split('\n')
        
        for line in lines:
            line = line.strip()
            
            # Detect @name annotation
            if line.startswith('-- @name:'):
                # Save previous query
                if current_query_name and current_query_lines:
                    query_sql = '\n'.join(current_query_lines).strip()
                    if query_sql:
                        queries[current_query_name] = self._format_query(query_sql)
                
                # Start new query
                current_query_name = line.replace('-- @name:', '').strip()
                current_query_lines = []
                
            elif current_query_name:
                # Skip comment lines (except @name annotations)
                if not line.startswith('--') or line.startswith('-- @'):
                    if line.startswith('-- @description:'):
                        # Skip description
                        continue
                    current_query_lines.append(line)
        
        # Save last query
        if current_query_name and current_query_lines:
            query_sql = '\n'.join(current_query_lines).strip()
            if query_sql:
                queries[current_query_name] = self._format_query(query_sql)
        
        return queries
    
    def _format_query(self, query: str) -> str:
        """Format SQL query string"""
        # Remove empty lines
        lines = [line for line in query.split('\n') if line.strip()]
        return '\n'.join(lines)
    
    def _substitute_parameters(self, query: str, params: Dict[str, any]) -> str:
        """Substitute parameters in SQL query"""
        result = query
        for key, value in params.items():
            placeholder = f"{{{key}}}"
            result = result.replace(placeholder, str(value))
        
        return result
    
    def list_files(self) -> List[str]:
        """Get list of available SQL files"""
        if not self.base_path.exists():
            return []
        
        return [f.name for f in self.base_path.glob("*.sql")]
    
    def list_queries(self, filename: str) -> List[str]:
        """Get list of query names in specified file"""
        if filename not in self._query_cache:
            self.load_queries_from_file(filename)
        
        return list(self._query_cache[filename].keys())
    
    def clear_cache(self):
        """Clear all caches"""
        self._query_cache.clear()
        self._file_cache.clear()
        logger.info("Query cache cleared")
    
    def file_exists(self, filename: str = None) -> bool:
        """Check if SQL file exists
        
        Args:
            filename: Name of the SQL file to check, or use the loaded file
            
        Returns:
            True if file exists, False otherwise
        """
        if filename is None:
            # Use the first loaded filename if available
            if hasattr(self, '_current_filename'):
                filename = self._current_filename
            else:
                return False
        
        file_path = self.base_path / filename
        return file_path.exists()
    
    def get_sql_content(self, filename: str = None) -> str:
        """Get raw SQL file content
        
        Args:
            filename: Name of the SQL file to get content from
            
        Returns:
            Raw SQL file content as string
        """
        if filename is None:
            # Use the first loaded filename if available
            if hasattr(self, '_current_filename'):
                filename = self._current_filename
            else:
                raise ValueError("No filename specified and no file currently loaded")
        
        # Check cache first
        if filename in self._file_cache:
            return self._file_cache[filename]
        
        # Load file
        file_path = self.base_path / filename
        if not file_path.exists():
            raise FileNotFoundError(f"SQL file not found: {file_path}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Cache content
            self._file_cache[filename] = content
            return content
            
        except Exception as e:
            logger.error(f"Failed to read SQL file {filename}: {e}")
            raise
    
    def get_sql_content_with_params(self, params: Dict[str, any], filename: str = None) -> str:
        """Get SQL content with parameters substituted
        
        Args:
            params: Parameters to substitute
            filename: Name of the SQL file to get content from
            
        Returns:
            SQL content with parameters substituted
        """
        content = self.get_sql_content(filename)
        
        if params:
            content = self._substitute_parameters(content, params)
        
        return content
