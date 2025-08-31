#!/usr/bin/env python3

import sqlglot
from sqlglot import exp

def test_inline_view_detection():
    sql = "SELECT s.measurementMonth FROM (SELECT t.measurementMonth FROM dimTimeTable t) s"
    parsed = sqlglot.parse(sql, dialect='oracle')[0]
    
    print('Full AST:')
    print(parsed)
    print('\n' + '='*50)
    
    print('FROM clause:')
    from_clause = parsed.find(exp.From)
    print(from_clause)
    print('\nMain expr type:', type(from_clause.this))
    print('Is subquery:', isinstance(from_clause.this, exp.Subquery))
    print('Has alias:', hasattr(from_clause.this, 'alias'))
    if hasattr(from_clause.this, 'alias'):
        print('Alias:', from_clause.this.alias)
    
    print('\n' + '='*50)
    
    # Test table extraction
    print('All table nodes:')
    for table in parsed.find_all(exp.Table):
        print(f"  Table: {table.this}, Alias: {getattr(table, 'alias', None)}")
        parent = table.parent
        parents = []
        while parent:
            parents.append(type(parent).__name__)
            parent = parent.parent
        print(f"    Parent chain: {' -> '.join(parents)}")
    
    print('\n' + '='*50)
    
    # Test subquery detection
    print('All subqueries:')
    for subq in parsed.find_all(exp.Subquery):
        print(f"  Subquery alias: {getattr(subq, 'alias', None)}")
        print(f"  Contains tables: {[t.this for t in subq.find_all(exp.Table)]}")

if __name__ == "__main__":
    test_inline_view_detection()
