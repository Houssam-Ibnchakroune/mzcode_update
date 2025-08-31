#!/usr/bin/env python3

import sys
sys.path.append('.')

from metazcode.sdk.ingestion.plsql.plsql_parser import CanonicalPlsqlParser

def test_file_parsing():
    parser = CanonicalPlsqlParser()
    
    file_path = "temp/Water-Quality-DW-on-Oracle-Database-main/SQL queries and PLSQL codes/3. Query the data warehouse to get insights and answers to project questions.sql"
    
    print("Testing file parsing...")
    print(f"File: {file_path}")
    print("="*80)
    
    # Read the file content
    with open(file_path, 'r') as f:
        content = f.read()
    
    print("File content length:", len(content))
    print("First 200 chars:", content[:200])
    print("\n" + "="*80)
    
    # Parse the file
    result = parser.parse_file(file_path)
    
    print(f"Parser result type: {type(result)}")
    if hasattr(result, '__dict__'):
        print(f"Result attributes: {list(result.__dict__.keys())}")
    
    if hasattr(result, 'sql_statements'):
        print(f"SQL statements found: {len(result.sql_statements)}")
        for i, stmt in enumerate(result.sql_statements):
            print(f"\nStatement {i+1}:")
            print(f"  Type: {type(stmt)}")
            if hasattr(stmt, 'content'):
                print(f"  Content length: {len(stmt.content)}")
                print(f"  First 100 chars: {stmt.content[:100]}...")
            if hasattr(stmt, 'semantics'):
                print(f"  Semantics: {stmt.semantics}")

if __name__ == "__main__":
    test_file_parsing()
