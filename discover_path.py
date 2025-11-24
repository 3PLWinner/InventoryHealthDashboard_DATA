import os
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential
from dotenv import load_dotenv

load_dotenv()

SHAREPOINT_URL = os.getenv("SHAREPOINT_URL")
SHAREPOINT_CLIENT_ID = os.getenv("SHAREPOINT_CLIENT_ID")
SHAREPOINT_CLIENT_SECRET = os.getenv("SHAREPOINT_CLIENT_SECRET")

def test_permissions():
    """Test what permissions the app actually has"""
    print("="*60)
    print("SharePoint Permissions Test")
    print("="*60)
    
    credentials = ClientCredential(SHAREPOINT_CLIENT_ID, SHAREPOINT_CLIENT_SECRET)
    ctx = ClientContext(SHAREPOINT_URL).with_credentials(credentials)
    
    # Test 1: Can we read the root site?
    print("\n1. Testing root site access...")
    try:
        ctx.load(ctx.web)
        ctx.execute_query()
        print(f"   ✓ Can access site: {ctx.web.properties['Title']}")
    except Exception as e:
        print(f"   ✗ Cannot access site: {e}")
        return
    
    # Test 2: Can we list document libraries?
    print("\n2. Testing document library enumeration...")
    try:
        lists = ctx.web.lists
        ctx.load(lists)
        ctx.execute_query()
        print(f"   ✓ Can list libraries: {len([l for l in lists])} found")
    except Exception as e:
        print(f"   ✗ Cannot list libraries: {e}")
    
    # Test 3: Can we access the Documents library?
    print("\n3. Testing Documents library access...")
    try:
        doc_lib = ctx.web.lists.get_by_title("Documents")
        ctx.load(doc_lib)
        ctx.load(doc_lib.root_folder)
        ctx.execute_query()
        print(f"   ✓ Can access Documents library")
        print(f"      Item Count: {doc_lib.properties.get('ItemCount')}")
    except Exception as e:
        print(f"   ✗ Cannot access Documents: {e}")
    
    # Test 4: Can we list files in root Shared Documents?
    print("\n4. Testing root Shared Documents file listing...")
    try:
        root_folder = ctx.web.get_folder_by_server_relative_url("/Shared Documents")
        ctx.load(root_folder)
        ctx.load(root_folder.files)
        ctx.execute_query()
        print(f"   ✓ Can list root folder files: {len(root_folder.files)} files")
    except Exception as e:
        print(f"   ✗ Cannot list root files: {e}")
    
    # Test 5: Can we access the InventoryHealthDashboard folder?
    print("\n5. Testing InventoryHealthDashboard folder access...")
    try:
        target_folder = ctx.web.get_folder_by_server_relative_url("/Shared Documents/InventoryHealthDashboard")
        ctx.load(target_folder)
        ctx.execute_query()
        print(f"   ✓ Can access folder")
        print(f"      Exists: {target_folder.exists}")
        print(f"      Server Relative URL: {target_folder.serverRelativeUrl}")
    except Exception as e:
        print(f"   ✗ Cannot access folder: {e}")
        return
    
    # Test 6: Can we list files in the target folder?
    print("\n6. Testing file listing in InventoryHealthDashboard...")
    try:
        ctx.load(target_folder.files)
        ctx.execute_query()
        print(f"   File collection loaded: {type(target_folder.files)}")
        print(f"   Files count: {len(target_folder.files)}")
        
        if len(target_folder.files) == 0:
            print(f"   ⚠ WARNING: Folder accessible but no files visible!")
            print(f"   This indicates a permissions issue.")
    except Exception as e:
        print(f"   ✗ Cannot list files: {e}")
    
    # Test 7: Try to get items from the library directly
    print("\n7. Testing direct library item query...")
    try:
        doc_lib = ctx.web.lists.get_by_title("Documents")
        
        # Try to get all items (limited)
        items = doc_lib.items.top(5000)
        ctx.load(items)
        ctx.execute_query()
        
        print(f"   ✓ Retrieved {len(items)} items from library")
        
        # Filter for items in our folder
        folder_items = [
            item for item in items 
            if '/InventoryHealthDashboard' in str(item.properties.get('FileRef', ''))
        ]
        
        print(f"   Items in InventoryHealthDashboard: {len(folder_items)}")
        
        if len(folder_items) > 0:
            print(f"\n   📄 Files found via library query:")
            for item in folder_items[:10]:  # Show first 10
                print(f"      - {item.properties.get('FileLeafRef')} at {item.properties.get('FileRef')}")
        
    except Exception as e:
        print(f"   ✗ Library query failed: {e}")
    
    # Test 8: Check effective permissions
    print("\n8. Checking effective permissions on folder...")
    try:
        target_folder = ctx.web.get_folder_by_server_relative_url("/Shared Documents/InventoryHealthDashboard")
        ctx.load(target_folder)
        ctx.load(target_folder.list_item_all_fields)
        ctx.execute_query()
        
        # Try to get role assignments
        list_item = target_folder.list_item_all_fields
        ctx.load(list_item)
        ctx.load(list_item.role_assignments)
        ctx.execute_query()
        
        print(f"   Role assignments: {len(list_item.role_assignments)}")
        
    except Exception as e:
        print(f"   Cannot check permissions: {e}")
    
    print("\n" + "="*60)
    print("DIAGNOSIS:")
    print("="*60)
    print("""
If you see:
- ✓ Folder accessible but 0 files
- Library has 11,917+ items

Then the app has Sites.FullControl.All but it's not being applied
correctly to this specific folder. This happens when:

1. The folder has unique permissions (broken inheritance)
2. Sites.Selected is being used instead of Sites.FullControl.All
3. The app registration needs explicit site access

SOLUTION:
Run this PowerShell command to grant explicit access:

Install-Module PnP.PowerShell -Scope CurrentUser
Connect-PnPOnline -Url "https://3plwinner.sharepoint.com" -Interactive
Grant-PnPAzureADAppSitePermission -AppId "YOUR_CLIENT_ID" \\
    -DisplayName "Data Pipeline App" \\
    -Permissions FullControl

Then wait 10 minutes and try again.
    """)

if __name__ == "__main__":
    test_permissions()