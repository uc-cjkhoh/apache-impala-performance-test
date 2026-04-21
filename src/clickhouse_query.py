from dataclasses import dataclass, field


@dataclass
class ClickHouseQueryList:
    """Container for ClickHouse queries with their configurations."""
    queries: list = field(default_factory=list)
    descriptions: list = field(default_factory=list)
    
    def add(self, query: str, description: str = ""):
        """
        Add a query to the test list.
        
        Args:
            query: SQL query string
            description: Optional description of what the query tests
        """
        self.queries.append(query)
        self.descriptions.append(description)


def get_clickhouse_queries():
    """
    Get predefined ClickHouse queries for performance testing without cache.
    
    These queries are designed to test various ClickHouse features:
    - Aggregation functions
    - Joins
    - Complex filters
    - GROUP BY operations
    - Subqueries
    
    Returns:
        List of ClickHouseQueryList objects containing test queries
    """
    ql = ClickHouseQueryList()
    
    # ============================================================================
    # 1. BASIC AGGREGATION - Testing COUNT and GROUP BY performance
    # ============================================================================
    
    # 1a: Simple COUNT - baseline aggregation
    ql.add(
        '''
        SELECT COUNT(*) as total_rows
        FROM system.query_log
        ''',
        description="Simple COUNT aggregation"
    )
    
    # 1b: COUNT with WHERE - filter before aggregation
    ql.add(
        '''
        SELECT COUNT(*) as query_count
        FROM system.query_log
        WHERE type = 'QueryStart'
        ''',
        description="COUNT with WHERE filter"
    )
    
    # 1c: GROUP BY aggregation - multiple groups
    ql.add(
        '''
        SELECT type, COUNT(*) as count
        FROM system.query_log
        GROUP BY type
        ORDER BY count DESC
        ''',
        description="GROUP BY with COUNT"
    )
    
    
    # ============================================================================
    # 2. MULTI-COLUMN AGGREGATION - Testing complex GROUP BY
    # ============================================================================
    
    # 2a: Multiple GROUP BY columns
    ql.add(
        '''
        SELECT 
            type, 
            query_duration_ms >= 1000 as slow_query,
            COUNT(*) as count
        FROM system.query_log
        WHERE event_date >= today() - 7
        GROUP BY type, slow_query
        ORDER BY count DESC
        ''',
        description="Multi-column GROUP BY"
    )
    
    # 2b: GROUP BY with multiple aggregation functions
    ql.add(
        '''
        SELECT 
            type,
            COUNT(*) as cnt,
            AVG(query_duration_ms) as avg_duration,
            MIN(query_duration_ms) as min_duration,
            MAX(query_duration_ms) as max_duration
        FROM system.query_log
        WHERE event_date >= today() - 30
        GROUP BY type
        ''',
        description="GROUP BY with multiple aggregate functions"
    )
    
    
    # ============================================================================
    # 3. FILTERING & PREDICATES - Testing filter optimization
    # ============================================================================
    
    # 3a: Complex WHERE clause with multiple conditions
    ql.add(
        '''
        SELECT COUNT(*) as result_count
        FROM system.query_log
        WHERE type IN ('QueryStart', 'QueryFinish')
          AND query_duration_ms > 100
          AND user != 'default'
        ''',
        description="Complex WHERE clause"
    )
    
    # 3b: Range queries - testing numeric range filtering
    ql.add(
        '''
        SELECT 
            query_duration_ms,
            COUNT(*) as count
        FROM system.query_log
        WHERE event_date >= today() - 1
          AND query_duration_ms BETWEEN 100 AND 5000
        GROUP BY query_duration_ms
        ORDER BY query_duration_ms
        ''',
        description="Range query filtering"
    )
    
    
    # ============================================================================
    # 4. JOINS - Testing join performance
    # ============================================================================
    
    # 4a: INNER JOIN on dictionaries
    ql.add(
        '''
        SELECT 
            ql.type,
            ql.user,
            COUNT(*) as query_count
        FROM system.query_log ql
        INNER JOIN system.users u ON ql.user = u.name
        WHERE ql.event_date >= today() - 1
        GROUP BY ql.type, ql.user
        LIMIT 100
        ''',
        description="INNER JOIN performance"
    )
    
    # 4b: LEFT JOIN with aggregation
    ql.add(
        '''
        SELECT 
            d.database,
            COUNT(ql.query_id) as query_count
        FROM system.databases d
        LEFT JOIN system.query_log ql ON d.database = ql.database
        GROUP BY d.database
        ORDER BY query_count DESC
        LIMIT 50
        ''',
        description="LEFT JOIN with GROUP BY"
    )
    
    
    # ============================================================================
    # 5. SUBQUERIES - Testing subquery performance
    # ============================================================================
    
    # 5a: Subquery in WHERE clause
    ql.add(
        '''
        SELECT COUNT(*) as high_duration_count
        FROM system.query_log
        WHERE query_duration_ms > (
            SELECT AVG(query_duration_ms) * 2
            FROM system.query_log
            WHERE event_date >= today() - 1
        )
        ''',
        description="Subquery in WHERE clause"
    )
    
    # 5b: Subquery with IN operator
    ql.add(
        '''
        SELECT 
            query_id,
            user,
            query_duration_ms
        FROM system.query_log
        WHERE user IN (
            SELECT name FROM system.users LIMIT 10
        )
        LIMIT 100
        ''',
        description="Subquery with IN operator"
    )
    
    
    # ============================================================================
    # 6. STRING OPERATIONS - Testing string function performance
    # ============================================================================
    
    # 6a: String matching
    ql.add(
        '''
        SELECT COUNT(*) as matching_count
        FROM system.query_log
        WHERE query LIKE '%SELECT%'
          AND query NOT LIKE '%system%'
        ''',
        description="String LIKE pattern matching"
    )
    
    # 6b: String functions in aggregation
    ql.add(
        '''
        SELECT 
            substring(query, 1, 20) as query_prefix,
            COUNT(*) as count
        FROM system.query_log
        WHERE length(query) > 10
        GROUP BY substring(query, 1, 20)
        ORDER BY count DESC
        LIMIT 50
        ''',
        description="String functions with GROUP BY"
    )
    
    
    # ============================================================================
    # 7. ADVANCED FEATURES - Testing advanced ClickHouse features
    # ============================================================================
    
    # 7a: Array functions
    ql.add(
        '''
        SELECT 
            arrayMap(x -> x * 2, arrayRange(1, 10)) as doubled_array,
            COUNT(*) as count
        FROM system.query_log
        WHERE event_date >= today()
        GROUP BY arrayMap(x -> x * 2, arrayRange(1, 10))
        LIMIT 100
        ''',
        description="Array functions performance"
    )
    
    # 7b: Case expression with aggregation
    ql.add(
        '''
        SELECT 
            CASE 
                WHEN query_duration_ms < 100 THEN 'Fast'
                WHEN query_duration_ms < 1000 THEN 'Medium'
                ELSE 'Slow'
            END as performance_level,
            COUNT(*) as count
        FROM system.query_log
        WHERE event_date >= today() - 1
        GROUP BY performance_level
        ORDER BY count DESC
        ''',
        description="CASE expression with GROUP BY"
    )
    
    # 7c: Window functions (if available)
    ql.add(
        '''
        SELECT 
            type,
            query_duration_ms,
            ROW_NUMBER() OVER (PARTITION BY type ORDER BY query_duration_ms DESC) as rank
        FROM system.query_log
        WHERE event_date >= today() - 1
        LIMIT 1000
        ''',
        description="Window functions - ROW_NUMBER"
    )
    
    
    # ============================================================================
    # 8. UNION & SET OPERATIONS - Testing set operations
    # ============================================================================
    
    # 8a: UNION queries
    ql.add(
        '''
        SELECT type, COUNT(*) as count FROM system.query_log WHERE event_date >= today() - 1 GROUP BY type
        UNION ALL
        SELECT 'total' as type, COUNT(*) as count FROM system.query_log WHERE event_date >= today() - 1
        ORDER BY type
        ''',
        description="UNION ALL operation"
    )
    
    
    # ============================================================================
    # 9. DEDUPLICATION - Testing DISTINCT
    # ============================================================================
    
    # 9a: DISTINCT with multiple columns
    ql.add(
        '''
        SELECT DISTINCT 
            user,
            client_hostname
        FROM system.query_log
        WHERE event_date >= today() - 1
        ORDER BY user
        ''',
        description="DISTINCT on multiple columns"
    )
    
    
    # ============================================================================
    # 10. SORTING & LIMITS - Testing ORDER BY performance
    # ============================================================================
    
    # 10a: ORDER BY with LIMIT
    ql.add(
        '''
        SELECT 
            query_id,
            user,
            query_duration_ms
        FROM system.query_log
        WHERE query_duration_ms > 0
        ORDER BY query_duration_ms DESC
        LIMIT 100
        ''',
        description="ORDER BY with LIMIT"
    )
    
    # 10b: Multiple ORDER BY columns
    ql.add(
        '''
        SELECT 
            type,
            user,
            query_duration_ms
        FROM system.query_log
        WHERE event_date >= today()
        ORDER BY type, query_duration_ms DESC
        LIMIT 1000
        ''',
        description="Multi-column ORDER BY"
    )
    
    return [ql]
