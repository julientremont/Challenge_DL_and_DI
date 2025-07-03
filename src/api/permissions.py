from rest_framework.permissions import BasePermission


class IsAdminOrReadOnlyForUsers(BasePermission):
    """
    Custom permission class:
    - Admin users: full access to all endpoints
    - Regular users: only access to list and detail endpoints (GET only)
    - Anonymous users: no access
    """
    
    def has_permission(self, request, view):
        # Require authentication
        if not request.user or not request.user.is_authenticated:
            return False
        
        # Admin users have full access
        if request.user.is_staff or request.user.is_superuser:
            return True
        
        # Regular users restrictions
        if request.method != 'GET':
            return False
            
        # For class-based views, check the action
        if hasattr(view, 'action'):
            allowed_actions = ['list', 'retrieve']
            return view.action in allowed_actions
        
        # For function-based views, check URL path for analytics endpoints
        path = request.path_info.lower()
        restricted_paths = [
            '/summary/', '/stats/', '/analysis/', '/analytics/', 
            '/aggregated/', '/time-series/', '/top-', '/by-',
            '/salary-', '/company-', '/industry-', '/location-',
            '/technology-', '/activity-', '/popular/', '/developer-types/',
            '/technologies/', '/salary-analysis/', '/skills-analysis/',
            '/salary-trends/', '/salary-rating-correlation/', '/company-insights/'
        ]
        
        # If path contains restricted patterns, deny access for regular users
        if any(restricted in path for restricted in restricted_paths):
            return False
        
        # Allow basic list and detail GET requests only
        return True


class AdminOnlyPermission(BasePermission):
    """
    Permission class that only allows admin users access.
    Used for analytics and administrative endpoints.
    """
    
    def has_permission(self, request, view):
        return (
            request.user and 
            request.user.is_authenticated and 
            (request.user.is_staff or request.user.is_superuser)
        )


class AuthenticatedReadOnlyPermission(BasePermission):
    """
    Permission class that allows authenticated users read-only access.
    Used for basic list and detail endpoints.
    """
    
    def has_permission(self, request, view):
        return (
            request.user and 
            request.user.is_authenticated and 
            request.method in ['GET', 'HEAD', 'OPTIONS']
        )