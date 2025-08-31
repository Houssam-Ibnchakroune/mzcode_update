import re
from metazcode.sdk.ingestion.plsql.type_mapping import detect_column_types_from_sql

# Test with simple CREATE TABLE
test_sql = """
CREATE TABLE dimLocationTable (
    locationID INTEGER GENERATED ALWAYS AS IDENTITY,
    locationMetadata VARCHAR2(255),
    locationNotation VARCHAR2(255),
    measurementLocation VARCHAR2(255),
    locationEasting INTEGER, 
    locationNorthing INTEGER,
    CONSTRAINT pk_locationID PRIMARY KEY (locationID)
    );
"""

print("=== Testing CREATE TABLE Detection ===")

# Test CREATE TABLE regex
CREATE_TABLE_RE = re.compile(r'\bcreate\s+table\s+([a-zA-Z0-9_\.\$#\"]+)', re.IGNORECASE | re.DOTALL)
matches = CREATE_TABLE_RE.findall(test_sql)

print(f'Tables found by regex: {matches}')

if matches:
    table_name = matches[0]
    # Use a more robust approach to extract the full CREATE TABLE statement
    create_start_pattern = rf'create\s+table\s+{re.escape(table_name)}\s*\('
    start_match = re.search(create_start_pattern, test_sql, re.IGNORECASE)
    
    if start_match:
        start_pos = start_match.start()
        # Find the matching closing parenthesis
        paren_count = 0
        pos = start_match.end() - 1  # Start at the opening parenthesis
        
        for i in range(pos, len(test_sql)):
            if test_sql[i] == '(':
                paren_count += 1
            elif test_sql[i] == ')':
                paren_count -= 1
                if paren_count == 0:
                    # Found the matching closing parenthesis
                    end_pos = i + 1
                    # Look for optional semicolon
                    if end_pos < len(test_sql) and test_sql[end_pos:end_pos+1].strip() == ';':
                        end_pos += 1
                    create_statement = test_sql[start_pos:end_pos].strip()
                    break
        else:
            create_statement = None
    else:
        create_statement = None
    if create_statement:
        print(f'\nFound CREATE statement:')
        print(create_statement)
        print()
        
        try:
            column_types = detect_column_types_from_sql(create_statement)
            print(f'Column types detected: {len(column_types)} columns')
            for col in column_types:
                print(f'  {col["column_name"]}: {col["oracle_native_type"]} -> {col["canonical_type"]}')
        except Exception as e:
            print(f'Error detecting column types: {e}')
            import traceback
            traceback.print_exc()
    else:
        print(f'No CREATE statement found for {table_name}')
