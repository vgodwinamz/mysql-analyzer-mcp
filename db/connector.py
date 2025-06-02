import json
import mysql.connector
import boto3
import base64
from typing import List, Dict, Any, Optional

class MySQLConnector:
    def __init__(self, secret_name=None, region_name=None, host=None, port=None, 
                 database=None, user=None, password=None):
        self.secret_name = secret_name
        self.region_name = region_name
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.conn = None
        self.read_only = True  # Default to read-only mode
        
    def connect(self):
        """Connect to MySQL database using either AWS Secrets or direct credentials"""
        try:
            if self.secret_name and self.region_name:
                # Get credentials from AWS Secrets Manager
                session = boto3.session.Session()
                client = session.client(
                    service_name='secretsmanager',
                    region_name=self.region_name
                )
                
                get_secret_value_response = client.get_secret_value(
                    SecretId=self.secret_name
                )
                
                if 'SecretString' in get_secret_value_response:
                    secret = json.loads(get_secret_value_response['SecretString'])
                    self.host = secret.get('host')
                    self.port = secret.get('port', 3306)
                    self.database = secret.get('dbname')
                    self.user = secret.get('username')
                    self.password = secret.get('password')
                else:
                    decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
                    secret = json.loads(decoded_binary_secret)
                    self.host = secret.get('host')
                    self.port = secret.get('port', 3306)
                    self.database = secret.get('dbname')
                    self.user = secret.get('username')
                    self.password = secret.get('password')
            elif not all([self.host, self.database, self.user, self.password]):
                # If direct credentials are not provided and no secret name, we can't connect
                print("Error: Either AWS Secrets Manager details or direct database credentials must be provided")
                return False
            
            # Connect to the database
            self.conn = mysql.connector.connect(
                host=self.host,
                port=self.port or 3306,
                database=self.database,
                user=self.user,
                password=self.password
            )
            
            # Set session to read-only mode for safety
            if self.read_only:
                with self.conn.cursor() as cursor:
                    cursor.execute("SET SESSION TRANSACTION READ ONLY")
                    cursor.execute("SET SESSION MAX_EXECUTION_TIME=30000")  # 30-second timeout
            
            print(f"Connected to MySQL database: {self.database} at {self.host}")
            return True
        except Exception as e:
            print(f"Error connecting to database: {str(e)}")
            return False
    
    def disconnect(self):
        """Close the database connection"""
        if self.conn:
            try:
                self.conn.close()
                self.conn = None
                print("Database connection closed")
            except Exception as e:
                print(f"Error closing database connection: {str(e)}")
    
    def execute_query(self, query, params=None):
        """Execute a query and return results as a list of dictionaries"""
        if not self.conn:
            print("No database connection. Call connect() first.")
            return []
        
        try:
            with self.conn.cursor(dictionary=True) as cursor:
                # For safety, check if this is a potentially dangerous operation
                if self.read_only:
                    query_lower = query.lower().strip()
                    dangerous_operations = [
                        'insert', 'update', 'delete', 'drop', 'alter', 'create', 'truncate', 
                        'grant', 'revoke', 'reset', 'load', 'optimize', 'repair', 'flush'
                    ]
                    
                    # Check if query starts with any dangerous operation
                    for op in dangerous_operations:
                        if query_lower.startswith(op):
                            print(f"Error: Write operation '{op}' attempted in read-only mode")
                            return []
                
                cursor.execute(query, params or ())
                
                # For SELECT queries, return results
                if cursor.description:
                    results = cursor.fetchall()
                    return list(results)  # Convert to list for consistency
                
                # For non-SELECT queries, commit and return empty list
                self.conn.commit()
                return []
        except Exception as e:
            self.conn.rollback()
            print(f"Error executing query: {str(e)}")
            return []

    def analyze_query_complexity(self, query):
        """
        Analyze query complexity and potential resource impact
        
        Args:
            query (str): SQL query to analyze
        
        Returns:
            dict: Complexity metrics
        """
        query_lower = query.lower()
        complexity_score = 0
        warnings = []
        
        # Check for joins
        join_count = sum(1 for join_type in ['join', 'inner join', 'left join', 'right join', 'full join'] 
                        if join_type in query_lower)
        complexity_score += join_count * 2
        if join_count > 3:
            warnings.append(f"Query contains {join_count} joins - consider simplifying")
        
        # Check for subqueries
        subquery_count = query_lower.count('(select')
        complexity_score += subquery_count * 3
        if subquery_count > 2:
            warnings.append(f"Query contains {subquery_count} subqueries - consider restructuring")
        
        # Check for aggregations
        agg_functions = ['count(', 'sum(', 'avg(', 'max(', 'min(']
        agg_count = sum(query_lower.count(func) for func in agg_functions)
        complexity_score += agg_count
        
        # Check for MySQL-specific features
        if 'force index' in query_lower:
            complexity_score += 2
            warnings.append("Query uses FORCE INDEX - consider if this is necessary")
        
        # Check for complex WHERE conditions
        where_pos = query_lower.find('where')
        if where_pos != -1:
            where_clause = query_lower[where_pos:]
            and_count = where_clause.count(' and ')
            or_count = where_clause.count(' or ')
            complexity_score += (and_count + or_count)
            if (and_count + or_count) > 5:
                warnings.append(f"Complex WHERE clause with {and_count + or_count} conditions")
        
        # Check for ORDER BY with multiple columns
        order_by_pos = query_lower.find('order by')
        if order_by_pos != -1:
            order_clause = query_lower[order_by_pos:]
            comma_count = order_clause.count(',')
            complexity_score += comma_count
            if comma_count > 2:
                warnings.append(f"ORDER BY with {comma_count + 1} columns may impact performance")
        
        return {
            'complexity_score': complexity_score,
            'warnings': warnings,
            'join_count': join_count,
            'subquery_count': subquery_count,
            'aggregation_count': agg_count
            }