from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from django.contrib.auth.models import User, Group


# Customize the User admin to show role information
class CustomUserAdmin(UserAdmin):
    list_display = ('username', 'email', 'first_name', 'last_name', 'is_staff', 'is_active', 'date_joined')
    list_filter = ('is_staff', 'is_superuser', 'is_active', 'groups')
    search_fields = ('username', 'first_name', 'last_name', 'email')
    ordering = ('username',)
    
    fieldsets = UserAdmin.fieldsets + (
        ('API Permissions', {
            'fields': ('groups',),
            'description': 'Admin users (is_staff=True) have full API access. Regular users have read-only access to list/detail endpoints only.',
        }),
    )


# Re-register User with custom admin
admin.site.unregister(User)
admin.site.register(User, CustomUserAdmin)


# Customize the admin site headers
admin.site.site_header = "Challenge DL & DI API Administration"
admin.site.site_title = "API Admin"
admin.site.index_title = "Welcome to the Challenge DL & DI API Administration"


# Admin actions for user management
@admin.action(description='Make selected users admin (staff)')
def make_admin(modeladmin, request, queryset):
    queryset.update(is_staff=True)


@admin.action(description='Remove admin privileges from selected users')
def remove_admin(modeladmin, request, queryset):
    queryset.update(is_staff=False)


# Register the actions
admin.site.add_action(make_admin)
admin.site.add_action(remove_admin)