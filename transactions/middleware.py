import logging
import time
import uuid
from django.utils.deprecation import MiddlewareMixin
import threading
from django.conf import settings
from django.http import HttpResponse

logger = logging.getLogger('banking_transactions')
_active_requests = 0
_request_lock = threading.Lock()

class TransactionLoggingMiddleware(MiddlewareMixin):
    """
    Middleware that logs every transaction request with:
    - Request ID
    - Client IP
    - Timestamp
    - Latency
    - Action, Account, Result, Balance (passed from views)
    """
    
    def process_request(self, request):
        global _active_requests
        
        # --- Concurrent Client Limiting (Section 2.1) ---
        with _request_lock:

            if _active_requests >= settings.MAX_CONCURRENT_CLIENTS:
                # Server is at capacity, reject the request
                request._counter_incremented = False  # Mark that we didn't increment
                return HttpResponse(
                    "Server at maximum capacity. Please try again later.",
                    status=503
                )
            _active_requests += 1
            request._counter_incremented = True  # Mark that we did increment

        # Start timer for latency calculation
        request.start_time = time.time()
        
        # Generate or extract Request ID
        request.request_id = request.headers.get('X-Request-ID', str(uuid.uuid4())[:8])
        
        # Store client IP
        request.client_ip = self.get_client_ip(request)
        
        # Initialize transaction data (will be populated by views)
        request.transaction_data = {
            'action': 'unknown',
            'account': 'unknown',
            'result': 'unknown',
            'balance': 'N/A'
        }
    
    def process_response(self, request, response):
        global _active_requests

        # Identify the serving replica to clients and to the gateway. Also
        # echo the request ID so a client can correlate its request with logs.
        if hasattr(request, 'request_id'):
            response['X-Request-ID'] = request.request_id
        response['X-Served-By'] = settings.REPLICA_ID
        response['X-Shard'] = settings.SHARD_ID

        # --- Decrement Active Request Counter ---
        # Only decrement if we actually incremented in process_request
        if getattr(request, '_counter_incremented', False):
            with _request_lock:
                _active_requests -= 1
        
        # Only log API endpoints (not admin, static files, etc.)
        if not request.path.startswith('/api/'):
            return response
        
        # Handle requests that were rejected early (don't have start_time)
        if not hasattr(request, 'start_time'):
            # This request was rejected before we could set up logging data
            return response
        
        # Calculate latency
        latency_ms = round((time.time() - request.start_time) * 1000, 2)
        
        # Extract transaction data from request
        tx_data = getattr(request, 'transaction_data', {})
        
        # Determine result from HTTP status code
        if response.status_code == 200:
            result = 'success'
        elif response.status_code == 404:
            result = 'error:unknown_account'
        elif response.status_code == 422:
            result = 'error:insufficient_funds'
        elif response.status_code == 400:
            result = 'error:invalid_input'
        elif response.status_code == 503:
            result = 'error:server_capacity'
        else:
            result = f'error:{response.status_code}'
        
        # Write to log file
        logger.info(
            "Transaction logged",
            extra={
                'shard': settings.SHARD_ID,
                'replica': settings.REPLICA_ID,
                'request_id': getattr(request, 'request_id', 'unknown'),
                'ip': getattr(request, 'client_ip', 'unknown'),
                'action': tx_data.get('action', 'unknown'),
                'account': tx_data.get('account', 'unknown'),
                'result': result,
                'balance': tx_data.get('balance', 'N/A'),
                'latency': latency_ms
            }
        )
        
        return response
    
    
    def get_client_ip(self, request):
        """Extract client IP address from request"""
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        if x_forwarded_for:
            return x_forwarded_for.split(',')[0].strip()
        return request.META.get('REMOTE_ADDR')