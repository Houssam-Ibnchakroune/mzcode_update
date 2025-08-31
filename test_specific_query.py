#!/usr/bin/env python3

import sys
sys.path.append('.')

from metazcode.sdk.ingestion.plsql.enhanced_sql_parser import EnhancedPlsqlParser

def test_specific_query():
    parser = EnhancedPlsqlParser()
    
    sql = """SELECT s.measurementMonth, s.sensorType, s.numberOfSensors FROM ( SELECT t.measurementMonth, EXTRACT(MONTH FROM TO_DATE(measurementMonth, 'MM')) monthNumber, s.sensorType, COUNT(measurement) numberOfSensors FROM dimSensorTable s INNER JOIN factMeasurementsTable f ON s.sensorID = f.sensorID INNER JOIN dimTimeTable t ON f.timeID = t.timeID GROUP BY s.sensorType, t.measurementMonth ) s ORDER BY s.monthNumber, s.sensorType"""
    
    print("Testing SQL:")
    print(sql)
    print("\n" + "="*80)
    
    semantics = parser.parse_sql_semantics(sql)
    
    print("Results:")
    print(f"Tables count: {len(semantics.tables)}")
    for i, table in enumerate(semantics.tables):
        print(f"  {i+1}. {table.name} (alias: {table.alias})")
    
    print(f"\nJoins count: {len(semantics.joins)}")
    for i, join in enumerate(semantics.joins):
        print(f"  {i+1}. {join.left_table.name} {join.join_type.value} {join.right_table.name}")
    
    print(f"\nInline views count: {len(semantics.inline_views)}")
    for i, iv in enumerate(semantics.inline_views):
        print(f"  {i+1}. Alias: {iv.alias}, Base tables: {iv.base_tables}")

if __name__ == "__main__":
    test_specific_query()
