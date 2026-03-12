"""
Firebase client for Chimera Ingestor Service
Handles all Firebase operations with error recovery and retry logic
"""
import json
import time
from typing import Dict, Any, Optional, List
import logging
from datetime import datetime
import firebase_admin
from firebase_admin import credentials, firestore, db
from firebase_admin.exceptions import FirebaseError

from config import settings, get_logger


logger = get_logger(__name__)


class FirebaseClient:
    """Firebase client with connection management and error handling"""
    
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(FirebaseClient, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self._initialized = True
            self.app = None
            self.firestore_client = None
            self.realtime_db = None
            self._initialize_firebase()
    
    def _initialize_firebase(self) -> None:
        """Initialize Firebase connection with retry logic"""
        max_attempts = 3
        attempt = 0
        
        while attempt < max_attempts:
            try:
                if not firebase_admin._apps:
                    cred = credentials.Certificate(settings.firebase_credentials_path)
                    self.app = firebase_admin.initialize_app(
                        cred,
                        {
                            'projectId': settings.firebase_project_id,
                            'databaseURL': f"https://{settings.firebase_project_id}.firebaseio.com"
                        }
                    )
                    logger.info(f"Firebase initialized for project: {settings.firebase_project_id}")
                
                self.firestore_client = firestore.client()
                self.realtime_db = db.reference(settings.realtime_db_root)
                
                # Test connection
                self._test_connection()
                logger.info("Firebase connection test successful")
                break
                
            except Exception as e:
                attempt += 1
                logger.error(f"Firebase initialization attempt {attempt} failed: {str(e)}")
                if attempt == max_attempts:
                    logger.critical("Failed to initialize Firebase after maximum attempts")
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff
    
    def _test_connection(self) -> None:
        """Test Firebase connection by writing a test document"""
        test_ref = self.firestore_client.collection("connection_tests").document("test")
        test_ref.set({
            "timestamp": datetime.utcnow().isoformat(),
            "service": settings.service_name,
            "status": "connected"
        }, merge=True)
        test_ref.delete()  # Clean up
    
    def store_tick(self, tick_data: Dict[str, Any]) -> bool:
        """
        Store a market tick in Firestore with error handling
        
        Args:
            tick_data: Dictionary containing tick information
        
        Returns:
            bool: True if successful, False otherwise
        """
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Validate required fields
                required_fields = ['exchange', 'symbol', 'bid', 'ask']
                for field in required_fields:
                    if field not in tick_data:
                        logger.error(f"Missing required field in tick data: {field}")
                        return False
                
                # Generate document ID with timestamp to ensure uniqueness
                doc_id = f"{tick_data['exchange']}_{tick_data['symbol']}_{int(time.time() * 1000)}_{tick_data.get('sequence_id', '0')}"
                
                # Add metadata
                tick_data['_stored_at'] = datetime.utcnow().isoformat()
                tick_data['_ingestion_version'] = "1.0"
                
                # Store in Firestore
                doc_ref = self.firestore_client.collection(
                    settings.firestore_raw_ticks_collection
                ).document(doc_id)
                
                doc_ref.set(tick_data)
                
                # Also update realtime DB for live monitoring
                realtime_path = f"{tick_data['exchange']}/{tick_data['symbol'].replace('/', '_')}"
                self.realtime_db.child(realtime_path).set({
                    'latest': tick_data,
                    'updated_at': tick_data['_stored_at']
                })
                
                logger.debug(f"Stored tick: {doc_id}")
                return True
                
            except FirebaseError as e:
                retry_count += 1
                logger.error(f"Firebase error storing tick (attempt {retry_count}): {str(e)}")
                if