#!/usr/bin/env python3
"""
Script to apply proxy fixes to test files
"""
import os
import re
import glob

def apply_proxy_fix(file_path):
    """Apply proxy fix to a test file"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Skip if already has setup_class
    if '@classmethod' in content and 'def setup_class(cls):' in content:
        print(f"SKIP: {file_path} already has proxy fix")
        return False
    
    # Skip if not a test class
    if 'class Test' not in content:
        print(f"SKIP: {file_path} doesn't contain test classes")
        return False
    
    # Find imports section
    import_match = re.search(r'(import.*?\n)+', content, re.MULTILINE)
    if not import_match:
        print(f"SKIP: {file_path} - couldn't find imports")
        return False
    
    # Add os and requests imports if not present
    imports_to_add = []
    if 'import os' not in content:
        imports_to_add.append('import os')
    if 'import requests' not in content:
        imports_to_add.append('import requests')
    
    if imports_to_add:
        import_end = import_match.end()
        new_imports = '\n'.join(imports_to_add) + '\n'
        content = content[:import_end] + new_imports + content[import_end:]
    
    # Find test classes and add setup_class method
    class_pattern = r'class (Test\w+).*?:'
    classes = re.finditer(class_pattern, content)
    
    modifications_made = False
    for class_match in classes:
        class_name = class_match.group(1)
        class_start = class_match.end()
        
        # Find the next line after class declaration
        lines = content[class_start:].split('\n')
        indent = ''
        for i, line in enumerate(lines):
            if line.strip():
                indent = re.match(r'(\s*)', line).group(1)
                break
        
        # Create the setup_class method
        setup_method = f'''
{indent}@classmethod
{indent}def setup_class(cls):
{indent}    """Disable proxy settings for tests"""
{indent}    # Store and clear proxy environment variables
{indent}    for var in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY']:
{indent}        if var in os.environ:
{indent}            del os.environ[var]

{indent}def _get_no_proxy_session(self):
{indent}    """Get a requests session with proxy disabled"""
{indent}    session = requests.Session()
{indent}    session.proxies = {{'http': None, 'https': None}}
{indent}    return session
'''
        
        # Insert the setup method after class declaration
        insertion_point = class_start + len(lines[0]) + 1  # After first line
        content = content[:insertion_point] + setup_method + content[insertion_point:]
        modifications_made = True
    
    if modifications_made:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"FIXED: {file_path}")
        return True
    else:
        print(f"SKIP: {file_path} - no test classes found")
        return False

def main():
    test_files = glob.glob("/Users/andokenji/Documents/書類 - 安藤賢二のMac mini/GitHub/azureDevOps/tests/**/test_*.py", recursive=True)
    
    # Skip already fixed files
    skip_files = [
        'test_final_integration.py',
        'test_e2e_working.py',
        'test_database_schema.py',
        'test_basic_connections.py'
    ]
    
    fixed_count = 0
    skipped_count = 0
    
    for file_path in test_files:
        filename = os.path.basename(file_path)
        if any(skip in filename for skip in skip_files):
            print(f"SKIP: {file_path} - already manually fixed")
            skipped_count += 1
            continue
            
        try:
            if apply_proxy_fix(file_path):
                fixed_count += 1
            else:
                skipped_count += 1
        except Exception as e:
            print(f"ERROR: {file_path} - {e}")
    
    print(f"\nSummary: Fixed {fixed_count} files, Skipped {skipped_count} files")

if __name__ == "__main__":
    main()
