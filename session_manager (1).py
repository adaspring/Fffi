import os
import secrets
import shutil
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, List
from config import SESSION_ROOT, SESSION_TIMEOUT


class SessionManager:
    def __init__(self, storage_path: Optional[str] = None):
        """Initialize session manager with storage configuration"""
        self.storage_path = Path(storage_path or SESSION_ROOT)
        self.storage_path.mkdir(exist_ok=True, parents=True)
        self.logger = logging.getLogger('session_manager')
        self.session_timeout = SESSION_TIMEOUT


    @staticmethod
    def validate_session_id(session_id: str) -> bool:
        """Static method to validate session ID format consistently"""
        if not session_id:
            return False
            
        # Must start with 'session-' followed by at least 12 alphanumeric chars
        import re
        pattern = r'^session-[a-zA-Z0-9_-]{12,}$'
        return bool(re.match(pattern, session_id))

    def create_session(self) -> Path:
        """Create a new session with unique ID and directory structure"""
        max_attempts = 10
        
        for _ in range(max_attempts):
            session_id = self._generate_session_id()
            if not self.validate_session_id(session_id):
                continue
                
            session_path = self.storage_path / session_id
            
            if not session_path.exists():
                try:
                    session_path.mkdir(parents=True)
                    
                    subdirs = ['uploads', 'extracted', 'translated', 'refined', 'final', 'results']
                    for subdir in subdirs:
                        (session_path / subdir).mkdir()
                    
                    metadata_file = session_path / '.session_metadata'
                    metadata = {
                        'created': datetime.now().isoformat(),
                        'session_id': session_id,
                        'status': 'active'
                    }
                    import json
                    with open(metadata_file, 'w') as f:
                        json.dump(metadata, f)
                    
                    self.logger.info(f"Created new session: {session_id}")
                    return session_path
                    
                except OSError as e:
                    self.logger.error(f"Failed to create session {session_id}: {e}")
                    if session_path.exists():
                        shutil.rmtree(session_path, ignore_errors=True)
                    continue
        
        raise RuntimeError("Failed to create unique session after multiple attempts")

    def get_session_path(self, session_id: str) -> Path:
        """Get path for existing session with validation"""
        if not self.validate_session_id(session_id):
            raise ValueError(f"Invalid session ID format: {session_id}")
        
        session_path = self.storage_path / session_id
        
        if not session_path.exists():
            raise FileNotFoundError(f"Session {session_id} not found")
        
        if self._is_session_expired(session_path):
            self.logger.warning(f"Attempted to access expired session: {session_id}")
            raise ValueError(f"Session {session_id} has expired")
        
        return session_path

    def delete_session(self, session_id: str) -> bool:
        """Delete a session and all its files"""
        try:
            session_path = self.get_session_path(session_id)
            shutil.rmtree(session_path)
            self.logger.info(f"Deleted session: {session_id}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to delete session {session_id}: {e}")
            return False

    def list_active_sessions(self) -> List[Dict[str, any]]:
        """List all active (non-expired) sessions"""
        sessions = []
        
        for session_dir in self.storage_path.iterdir():
            if session_dir.is_dir() and not session_dir.name.startswith('.'):
                try:
                    metadata_file = session_dir / '.session_metadata'
                    if metadata_file.exists():
                        import json
                        with open(metadata_file, 'r') as f:
                            metadata = json.load(f)
                        
                        if not self._is_session_expired(session_dir):
                            metadata['file_count'] = len(list((session_dir / 'uploads').glob('*.html')))
                            metadata['has_results'] = (session_dir / 'results' / 'batch-output.zip').exists()
                            sessions.append(metadata)
                            
                except Exception as e:
                    self.logger.warning(f"Error reading session {session_dir.name}: {e}")
                    continue
        
        return sorted(sessions, key=lambda x: x.get('created', ''), reverse=True)

    def _generate_session_id(self) -> str:
        """Generate a secure random session ID"""
        # Generate longer IDs for better uniqueness
        return f"session-{secrets.token_urlsafe(16)}"

    def _is_session_expired(self, session_path: Path) -> bool:
        """Check if a session has expired based on creation time"""
        try:
            metadata_file = session_path / '.session_metadata'
            if metadata_file.exists():
                import json
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                
                created_str = metadata.get('created')
                if created_str:
                    created_time = datetime.fromisoformat(created_str)
                    age = datetime.now() - created_time
                    return age.total_seconds() > self.session_timeout
            
            stat = session_path.stat()
            age = datetime.now() - datetime.fromtimestamp(stat.st_mtime)
            return age.total_seconds() > self.session_timeout
            
        except Exception as e:
            self.logger.warning(f"Error checking session expiry: {e}")
            return True

    def _cleanup_stale_sessions(self):
        """Remove expired sessions to free up disk space"""
        try:
            cleaned = 0
            errors = 0
            
            for session_dir in self.storage_path.iterdir():
                if session_dir.is_dir() and not session_dir.name.startswith('.'):
                    if self._is_session_expired(session_dir):
                        try:
                            shutil.rmtree(session_dir)
                            cleaned += 1
                            self.logger.info(f"Cleaned up expired session: {session_dir.name}")
                        except Exception as e:
                            errors += 1
                            self.logger.error(f"Failed to clean up session {session_dir.name}: {e}")
            
            if cleaned > 0:
                self.logger.info(f"Session cleanup completed: {cleaned} sessions removed, {errors} errors")
                
        except Exception as e:
            self.logger.error(f"Session cleanup failed: {e}")

    def get_session_info(self, session_id: str) -> Optional[Dict[str, any]]:
        """Get detailed information about a specific session"""
        try:
            session_path = self.get_session_path(session_id)
            
            metadata_file = session_path / '.session_metadata'
            metadata = {}
            
            if metadata_file.exists():
                import json
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
            
            metadata['session_id'] = session_id
            metadata['path'] = str(session_path)
            
            metadata['files'] = {
                'uploads': len(list((session_path / 'uploads').glob('*.html'))),
                'extracted': len(list((session_path / 'extracted').iterdir())) if (session_path / 'extracted').exists() else 0,
                'translated': len(list((session_path / 'translated').iterdir())) if (session_path / 'translated').exists() else 0,
                'refined': len(list((session_path / 'refined').iterdir())) if (session_path / 'refined').exists() else 0,
                'final': len(list((session_path / 'final').iterdir())) if (session_path / 'final').exists() else 0,
            }
            
            results_dir = session_path / 'results'
            if results_dir.exists():
                metadata['results'] = [f.name for f in results_dir.glob('*.zip')]
            else:
                metadata['results'] = []
            
            if 'created' in metadata:
                created_time = datetime.fromisoformat(metadata['created'])
                age = datetime.now() - created_time
                metadata['age_minutes'] = int(age.total_seconds() / 60)
                metadata['expires_in_minutes'] = max(0, int((self.session_timeout - age.total_seconds()) / 60))
            
            return metadata
            
        except Exception as e:
            self.logger.error(f"Error getting session info for {session_id}: {e}")
            return None

    def extend_session(self, session_id: str, additional_time: int = 3600) -> bool:
        """Extend a session's expiration time"""
        try:
            if not self.validate_session_id(session_id):
                return False
                
            session_path = self.get_session_path(session_id)
            metadata_file = session_path / '.session_metadata'
            
            if metadata_file.exists():
                import json
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                
                metadata['created'] = datetime.now().isoformat()
                metadata['extended'] = True
                metadata['extended_at'] = datetime.now().isoformat()
                
                with open(metadata_file, 'w') as f:
                    json.dump(metadata, f)
                
                self.logger.info(f"Extended session {session_id} by {additional_time} seconds")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to extend session {session_id}: {e}")
            return False

    def get_storage_stats(self) -> Dict[str, any]:
        """Get storage statistics for all sessions"""
        try:
            total_size = 0
            session_count = 0
            expired_count = 0
            
            for session_dir in self.storage_path.iterdir():
                if session_dir.is_dir() and not session_dir.name.startswith('.'):
                    session_count += 1
                    
                    size = sum(f.stat().st_size for f in session_dir.rglob('*') if f.is_file())
                    total_size += size
                    
                    if self._is_session_expired(session_dir):
                        expired_count += 1
            
            return {
                'total_sessions': session_count,
                'active_sessions': session_count - expired_count,
                'expired_sessions': expired_count,
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'storage_path': str(self.storage_path)
            }
            
        except Exception as e:
            self.logger.error(f"Error getting storage stats: {e}")
            return {
                'error': str(e),
                'storage_path': str(self.storage_path)
            }