import os
import re

# Check CREATE TABLE detection in each file
path = 'temp/Water-Quality-DW-on-Oracle-Database-main'

files_to_check = [
    'Setup. PLSQL code to create the staging table.sql',
    'SQL queries and PLSQL codes/2. LOAD PHASE !!!!! Create dimension and fact tables and load them with PLSQL Cursors.sql'
]

CREATE_TABLE_RE = re.compile(r'\bcreate\s+table\s+([a-zA-Z0-9_\.\$#\"]+)', re.IGNORECASE)

for file_rel in files_to_check:
    file_path = os.path.join(path, file_rel)
    print(f'=== {os.path.basename(file_path)} ===')
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        tables = CREATE_TABLE_RE.findall(content)
        print(f'CREATE TABLE statements found: {tables}')
        print(f'Total CREATE TABLE count: {len(tables)}')
        
        # Test if our enhanced extraction would work for the first table
        if tables:
            test_table = tables[0]
            create_start_pattern = rf'create\s+table\s+{re.escape(test_table)}\s*\('
            start_match = re.search(create_start_pattern, content, re.IGNORECASE)
            
            if start_match:
                print(f'✓ Enhanced extraction would work for {test_table}')
            else:
                print(f'✗ Enhanced extraction failed for {test_table}')
        
    except Exception as e:
        print(f'Error: {e}')
    
    print()
