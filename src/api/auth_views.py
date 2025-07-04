from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.models import User
from rest_framework import status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework_simplejwt.tokens import RefreshToken
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiResponse
from drf_spectacular.openapi import OpenApiTypes, OpenApiExample


@extend_schema(
    tags=['Authentication'],
    request={
        'application/json': {
            'type': 'object',
            'properties': {
                'username': {'type': 'string', 'description': 'Username'},
                'password': {'type': 'string', 'format': 'password', 'description': 'Password'}
            },
            'required': ['username', 'password']
        }
    },
    responses={
        200: {
            'type': 'object',
            'properties': {
                'message': {'type': 'string'},
                'user': {
                    'type': 'object',
                    'properties': {
                        'username': {'type': 'string'},
                        'role': {'type': 'string'},
                        'access_level': {'type': 'string'}
                    }
                }
            }
        },
        400: {'description': 'Bad request - missing credentials'},
        401: {'description': 'Unauthorized - invalid credentials'}
    },
    description='Login to the API using session authentication.\n\nTest credentials:\n- **Admin**: username=`admin`, password=`admin123`\n- **User**: username=`user`, password=`user123`'
)
@api_view(['POST'])
@permission_classes([AllowAny])
def api_login(request):
    """
    Login endpoint for session authentication.
    
    Test credentials:
    - Admin: admin / admin123 (full access)
    - User: user / user123 (read-only access to list/detail endpoints)
    """
    # Get credentials from request data (DRF automatically parses JSON/form data)
    username = request.data.get('username')
    password = request.data.get('password')
    
    # Debug logging
    print(f'Login attempt - Username: {username}, Password present: {bool(password)}')
    print(f'Request content type: {request.content_type}')
    print(f'Request method: {request.method}')
    print(f'Request data: {request.data}')
    print(f'Request data type: {type(request.data)}')
    
    if not username or not password:
        return Response({
            'error': 'Both username and password are required',
            'debug_info': {
                'content_type': request.content_type,
                'method': request.method,
                'data_received': request.data,
                'data_keys': list(request.data.keys()) if request.data else []
            },
            'test_credentials': {
                'admin': {'username': 'admin', 'password': 'admin123', 'access': 'full'},
                'user': {'username': 'user', 'password': 'user123', 'access': 'read-only list/detail'}
            }
        }, status=status.HTTP_400_BAD_REQUEST)
    
    user = authenticate(username=username, password=password)
    
    if user is not None:
        login(request, user)
        
        # Determine user role
        role = 'admin' if user.is_staff or user.is_superuser else 'user'
        access_level = 'Full access to all endpoints' if role == 'admin' else 'Read-only access to list/detail endpoints only'
        
        return Response({
            'message': 'Login successful',
            'user': {
                'username': user.username,
                'role': role,
                'access_level': access_level,
                'is_staff': user.is_staff,
                'is_superuser': user.is_superuser
            },
            'next_steps': 'You can now access protected endpoints. Your session will persist in Swagger UI.'
        })
    else:
        return Response({
            'error': 'Invalid credentials',
            'test_credentials': {
                'admin': {'username': 'admin', 'password': 'admin123'},
                'user': {'username': 'user', 'password': 'user123',}
            }
        }, status=status.HTTP_401_UNAUTHORIZED)


@extend_schema(
    operation_id='auth_jwt_login',
    tags=['Authentication'],
    summary='JWT Login - Get JWT tokens',
    description='Authenticate user and return JWT access and refresh tokens',
    request={
        'application/json': {
            'type': 'object',
            'properties': {
                'username': {'type': 'string'},
                'password': {'type': 'string'}
            },
            'required': ['username', 'password']
        }
    },
    responses={
        200: OpenApiResponse(
            response={
                'type': 'object',
                'properties': {
                    'access': {'type': 'string', 'description': 'JWT access token'},
                    'refresh': {'type': 'string', 'description': 'JWT refresh token'},
                    'user': {
                        'type': 'object',
                        'properties': {
                            'id': {'type': 'integer'},
                            'username': {'type': 'string'},
                            'email': {'type': 'string'},
                            'is_staff': {'type': 'boolean'},
                            'is_superuser': {'type': 'boolean'}
                        }
                    }
                }
            },
            description='JWT tokens generated successfully'
        ),
        401: OpenApiResponse(description='Invalid credentials'),
    },
    examples=[
        OpenApiExample(
            'Admin Login',
            summary='Admin user login',
            description='Login as admin user',
            value={
                'username': 'admin',
                'password': 'admin123'
            },
            request_only=True,
        ),
        OpenApiExample(
            'Regular User Login',
            summary='Regular user login',
            description='Login as regular user',
            value={
                'username': 'user',
                'password': 'user123'
            },
            request_only=True,
        ),
    ]
)
@api_view(['POST'])
@permission_classes([AllowAny])
def jwt_login(request):
    """
    JWT Login endpoint that returns access and refresh tokens.
    
    Test credentials:
    - Admin: admin / admin123 (full access)
    - User: user / user123 (read-only access)
    
    Use the access token in Authorization header: Bearer <access_token>
    """
    # Get credentials from request data (DRF automatically parses JSON/form data)
    username = request.data.get('username')
    password = request.data.get('password')
    
    # Debug logging
    print(f'JWT Login attempt - Username: {username}, Password present: {bool(password)}')
    print(f'Request content type: {request.content_type}')
    print(f'Request data: {request.data}')
    
    if not username or not password:
        return Response(
            {
                'error': 'Username and password are required',
                'debug_info': {
                    'content_type': request.content_type,
                    'method': request.method,
                    'data_received': request.data,
                    'data_keys': list(request.data.keys()) if request.data else []
                },
                'test_credentials': {
                    'admin': {'username': 'admin', 'password': 'admin123'},
                    'user': {'username': 'user', 'password': 'user123'}
                }
            }, 
            status=status.HTTP_400_BAD_REQUEST
        )
    
    user = authenticate(username=username, password=password)
    
    if user is not None:
        refresh = RefreshToken.for_user(user)
        return Response({
            'access': str(refresh.access_token),
            'refresh': str(refresh),
            'user': {
                'id': user.id,
                'username': user.username,
                'email': user.email,
                'is_staff': user.is_staff,
                'is_superuser': user.is_superuser
            },
            'instructions': 'Use the access token in Authorization header: Bearer <access_token>'
        })
    else:
        return Response(
            {
                'error': 'Invalid credentials',
                'test_credentials': {
                    'admin': {'username': 'admin', 'password': 'admin123'},
                    'user': {'username': 'user', 'password': 'user123'}
                }
            }, 
            status=status.HTTP_401_UNAUTHORIZED
        )


@extend_schema(
    operation_id='auth_jwt_refresh',
    tags=['Authentication'],
    summary='JWT Refresh - Get new access token',
    description='Refresh JWT access token using refresh token',
    request={
        'application/json': {
            'type': 'object',
            'properties': {
                'refresh': {'type': 'string', 'description': 'Refresh token from login response'}
            },
            'required': ['refresh']
        }
    },
    responses={
        200: OpenApiResponse(
            response={
                'type': 'object',
                'properties': {
                    'access': {'type': 'string', 'description': 'New JWT access token'}
                }
            },
            description='Token refreshed successfully'
        ),
        401: OpenApiResponse(description='Invalid refresh token'),
    }
)
@api_view(['POST'])
@permission_classes([AllowAny])
def jwt_refresh(request):
    """
    Refresh JWT access token using refresh token
    """
    refresh_token = request.data.get('refresh')
    
    if not refresh_token:
        return Response(
            {'error': 'Refresh token is required'}, 
            status=status.HTTP_400_BAD_REQUEST
        )
    
    try:
        refresh = RefreshToken(refresh_token)
        return Response({
            'access': str(refresh.access_token),
            'instructions': 'Use the new access token in Authorization header: Bearer <access_token>'
        })
    except Exception as e:
        return Response(
            {'error': 'Invalid refresh token'}, 
            status=status.HTTP_401_UNAUTHORIZED
        )


@extend_schema(
    tags=['Authentication'],
    description='Logout from the current session'
)
@api_view(['POST'])
def api_logout(request):
    """Logout from the current session"""
    if request.user.is_authenticated:
        username = request.user.username
        logout(request)
        return Response({
            'message': f'User {username} logged out successfully'
        })
    else:
        return Response({
            'message': 'No active session to logout'
        })



@extend_schema(
    tags=['Authentication'],
    description='Get current user information and access level'
)
@api_view(['GET'])
@permission_classes([AllowAny])
def current_user(request):
    """Get information about the current authenticated user"""
    if request.user.is_authenticated:
        role = 'admin' if request.user.is_staff or request.user.is_superuser else 'user'
        access_level = 'Full access to all endpoints' if role == 'admin' else 'Read-only access to list/detail endpoints only'
        
        return Response({
            'authenticated': True,
            'user': {
                'username': request.user.username,
                'email': request.user.email,
                'role': role,
                'access_level': access_level,
                'is_staff': request.user.is_staff,
                'is_superuser': request.user.is_superuser,
                'date_joined': request.user.date_joined
            }
        })
    else:
        return Response({
            'authenticated': False,
            'message': 'Not authenticated',
            'login_instructions': 'Use the /api/auth/login/ endpoint or Basic Authentication',
            'test_credentials': {
                'admin': {'username': 'admin', 'password': 'admin123', 'access': 'full'},
                'user': {'username': 'user', 'password': 'user123', 'access': 'read-only'}
            }
        })

