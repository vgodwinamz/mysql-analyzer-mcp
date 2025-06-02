"""
Functions for detecting MySQL query patterns and anti-patterns
"""
import re
from typing import List, Dict, Any, Tuple, Optional

def detect_query_patterns(plan_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Detect query patterns from execution plan
    
    Args:
        plan_json: Execution plan JSON
        
    Returns:
        List of detected patterns
    """
    patterns = []
    
    # Check for table scans
    if "query_block" in plan_json and "table" in plan_json["query_block"]:
        tables = plan_json["query_block"]["table"]
        if isinstance(tables, dict):
            tables = [tables]
        
        for table in tables:
            access_type = table.get("access_type", "")
            if access_type == "ALL":
                patterns.append({
                    "pattern": "Full Table Scan",
                    "description": f"The query performs a full table scan on table '{table.get('table_name', 'Unknown')}'.",
                    "recommendation": "Consider adding an index to the columns used in WHERE clauses."
                })
    
    # Check for temporary tables
    if "query_block" in plan_json and "temporary_table" in plan_json["query_block"]:
        patterns.append({
            "pattern": "Temporary Table",
            "description": "The query creates a temporary table, which can be memory-intensive.",
            "recommendation": "Consider simplifying the query or adding appropriate indexes."
        })
    
    # Check for filesorts
    if "query_block" in plan_json and "ordering_operation" in plan_json["query_block"]:
        patterns.append({
            "pattern": "Filesort",
            "description": "The query uses a filesort operation, which can be slow for large datasets.",
            "recommendation": "Consider adding an index that matches your ORDER BY clause."
        })
    
    # Check for joins without indexes
    if "query_block" in plan_json and "nested_loop" in plan_json["query_block"]:
        nested_loops = plan_json["query_block"]["nested_loop"]
        if isinstance(nested_loops, list):
            for loop in nested_loops:
                if "table" in loop and loop["table"].get("access_type") == "ALL":
                    patterns.append({
                        "pattern": "Join Without Index",
                        "description": f"The query joins with table '{loop['table'].get('table_name', 'Unknown')}' without using an index.",
                        "recommendation": "Add an index to the join columns in this table."
                    })
    
    return patterns

def detect_query_anti_patterns(query: str) -> List[Dict[str, Any]]:
    """
    Detect common MySQL query anti-patterns
    
    Args:
        query: SQL query
        
    Returns:
        List of detected anti-patterns
    """
    query_lower = query.lower()
    anti_patterns = []
    
    # Check for SELECT *
    if re.search(r'select\s+\*\s+from', query_lower):
        anti_patterns.append({
            "issue": "SELECT *",
            "description": "Using SELECT * retrieves all columns, which can be inefficient when you only need specific columns.",
            "suggestion": "Explicitly list only the columns you need.",
            "example": "SELECT id, name, email FROM users WHERE active = 1"
        })
    
    # Check for LIKE with leading wildcard
    if re.search(r'like\s+[\'"]%', query_lower):
        anti_patterns.append({
            "issue": "LIKE with Leading Wildcard",
            "description": "Using LIKE with a leading wildcard (%) prevents the use of indexes.",
            "suggestion": "Avoid using LIKE with leading wildcards, or consider using a full-text index.",
            "example": "SELECT * FROM products WHERE name LIKE 'apple%' -- Good\nSELECT * FROM products WHERE name LIKE '%apple' -- Bad"
        })
    
    # Check for functions on indexed columns
    function_patterns = [
        r'where\s+\w+$[^)]+$',
        r'on\s+\w+$[^)]+$'
    ]
    
    for pattern in function_patterns:
        if re.search(pattern, query_lower):
            anti_patterns.append({
                "issue": "Function on Indexed Column",
                "description": "Using functions on columns in WHERE or JOIN conditions prevents the use of indexes.",
                "suggestion": "Avoid using functions on columns in WHERE or JOIN conditions.",
                "example": "SELECT * FROM users WHERE YEAR(created_at) = 2023 -- Bad\nSELECT * FROM users WHERE created_at BETWEEN '2023-01-01' AND '2023-12-31' -- Good"
            })
    
    # Check for OR conditions
    if re.search(r'where.*?\s+or\s+', query_lower):
        anti_patterns.append({
            "issue": "OR Conditions",
            "description": "OR conditions can prevent the use of indexes in some cases.",
            "suggestion": "Consider using UNION ALL instead of OR, or ensure both sides of the OR have indexes.",
            "example": "SELECT * FROM users WHERE last_name = 'Smith' OR first_name = 'John' -- May not use indexes efficiently\n\nSELECT * FROM users WHERE last_name = 'Smith' UNION ALL SELECT * FROM users WHERE first_name = 'John' -- May be more efficient"
        })
    
    # Check for implicit conversions
    if re.search(r'where\s+\w+\s*=\s*[\'"][0-9]+[\'"]', query_lower):
        anti_patterns.append({
            "issue": "Implicit Type Conversion",
            "description": "Comparing a numeric column to a string value causes implicit type conversion and prevents index usage.",
            "suggestion": "Ensure the data types in comparisons match the column types.",
            "example": "SELECT * FROM users WHERE id = '123' -- Bad (string comparison with numeric column)\nSELECT * FROM users WHERE id = 123 -- Good"
        })
    
    # Check for NOT IN or NOT EXISTS
    if re.search(r'not\s+in\s*$', query_lower) or re.search(r'not\s+exists', query_lower):
        anti_patterns.append({
            "issue": "NOT IN or NOT EXISTS",
            "description": "NOT IN and NOT EXISTS can be inefficient, especially with large subqueries.",
            "suggestion": "Consider using LEFT JOIN with IS NULL instead.",
            "example": "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders) -- Less efficient\n\nSELECT u.* FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE o.user_id IS NULL -- More efficient"
        })
    
    # Check for HAVING without GROUP BY
    if re.search(r'having', query_lower) and not re.search(r'group\s+by', query_lower):
        anti_patterns.append({
            "issue": "HAVING without GROUP BY",
            "description": "Using HAVING without GROUP BY treats the entire result set as one group, which may not be intended.",
            "suggestion": "Add an appropriate GROUP BY clause or use WHERE instead if grouping is not needed.",
            "example": "SELECT user_id, COUNT(*) FROM orders HAVING COUNT(*) > 5 -- Missing GROUP BY\n\nSELECT user_id, COUNT(*) FROM orders GROUP BY user_id HAVING COUNT(*) > 5 -- Correct"
        })
    
    # Check for ORDER BY RAND()
    if re.search(r'order\s+by\s+rand$$', query_lower):
        anti_patterns.append({
            "issue": "ORDER BY RAND()",
            "description": "ORDER BY RAND() is extremely inefficient as it requires sorting the entire result set.",
            "suggestion": "Use application code to randomize results, or consider other techniques like ORDER BY RAND() LIMIT for small result sets.",
            "example": "-- Instead of:\nSELECT * FROM products ORDER BY RAND() LIMIT 5\n\n-- Consider:\nSELECT * FROM products WHERE id >= (SELECT FLOOR(RAND() * (SELECT MAX(id) FROM products))) ORDER BY id LIMIT 5"
        })
    
    return anti_patterns

def validate_read_only_query(query: str) -> Tuple[bool, Optional[str]]:
    """
    Validate that a query is read-only
    
    Args:
        query: SQL query
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    query_lower = query.lower().strip()
    
    # List of allowed query types
    allowed_prefixes = [
        'select', 'show', 'explain', 'desc', 'describe'
    ]
    
    # Check if query starts with allowed prefix
    if not any(query_lower.startswith(prefix) for prefix in allowed_prefixes):
        return False, "Only SELECT, SHOW, EXPLAIN, and DESCRIBE queries are allowed in read-only mode."
    
    # Check for potentially dangerous operations
    dangerous_operations = [
        'insert', 'update', 'delete', 'drop', 'alter', 'create', 'truncate', 
        'grant', 'revoke', 'reset', 'load', 'optimize', 'repair', 'flush'
    ]
    
    # Look for dangerous operations in the query
    for op in dangerous_operations:
        pattern = r'\b' + op + r'\b'
        if re.search(pattern, query_lower):
            return False, f"The query contains a potentially dangerous operation: {op.upper()}"
    
    # Check for multi-statement queries
    if ';' in query_lower[:-1]:  # Allow semicolon at the end
        return False, "Multi-statement queries are not allowed."
    
    return True, None