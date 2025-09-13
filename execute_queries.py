#!/usr/bin/env python3
"""
Execute all SQL queries against olist.db and generate JSON results for frontend
"""
import sqlite3
import json
import os
from pathlib import Path

def execute_query(db_path, query_file):
    """Execute a SQL query and return results as list of dictionaries"""
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row  # This enables column access by name
        cursor = conn.cursor()
        
        with open(query_file, 'r') as f:
            query = f.read()
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to list of dictionaries
        return [dict(row) for row in results]

def main():
    """Execute all queries and save results as JSON"""
    db_path = "olist.db"
    queries_dir = Path("queries")
    results_dir = Path("frontend/data")
    results_dir.mkdir(parents=True, exist_ok=True)
    
    # Define query files and their output names
    queries = {
        "revenue_by_month_year.sql": "revenue_by_month_year.json",
        "revenue_per_state.sql": "revenue_per_state.json", 
        "top_10_revenue_categories.sql": "top_10_revenue_categories.json",
        "top_10_least_revenue_categories.sql": "top_10_least_revenue_categories.json",
        "delivery_date_difference.sql": "delivery_date_difference.json",
        "real_vs_estimated_delivered_time.sql": "real_vs_estimated_delivered_time.json",
        "global_ammount_order_status.sql": "global_ammount_order_status.json"
    }
    
    results_summary = {}
    
    for query_file, output_file in queries.items():
        query_path = queries_dir / query_file
        output_path = results_dir / output_file
        
        print(f"Executing {query_file}...")
        try:
            results = execute_query(db_path, query_path)
            
            # Save results to JSON
            with open(output_path, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            
            results_summary[query_file] = {
                "status": "success",
                "rows": len(results),
                "output_file": str(output_path)
            }
            print(f"  ✓ {len(results)} rows saved to {output_path}")
            
        except Exception as e:
            results_summary[query_file] = {
                "status": "error",
                "error": str(e)
            }
            print(f"  ✗ Error: {e}")
    
    # Save summary
    with open(results_dir / "execution_summary.json", 'w') as f:
        json.dump(results_summary, f, indent=2)
    
    print(f"\nExecution complete. Results saved in {results_dir}")
    return results_summary

if __name__ == "__main__":
    main()
