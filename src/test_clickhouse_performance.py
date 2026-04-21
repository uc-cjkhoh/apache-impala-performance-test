import pandas as pd

from tqdm import tqdm
from time import perf_counter
from clickhouse_driver import Client


class TestClickHousePerformance:
    """
    ClickHouse query performance testing without query cache.
    
    This class measures query execution times by:
    1. Connecting to a ClickHouse server
    2. Clearing the query result cache before each execution
    3. Measuring execution time with high precision
    4. Recording results for statistical analysis
    
    The query cache is disabled to get true query performance
    without the benefit of cached results from previous runs.
    """
    
    def __init__(self, host: str, port: int, iteration: int, queries: list[str], 
                 database: str = 'default', user: str = 'default', password: str = ''):
        """
        Initialize ClickHouse performance tester.
        
        Args:
            host: ClickHouse server hostname/IP
            port: ClickHouse port (default: 9000)
            iteration: Number of times to run each query
            queries: List of SQL queries to test
            database: Database to use (default: 'default')
            user: Database user (default: 'default')
            password: Database password (default: '')
        """
        self.host = host
        self.port = port
        self.iteration = iteration
        self.queries = queries
        self.database = database
        self.user = user
        self.password = password
        
    
    def get_clickhouse_version(self, client: Client) -> str:
        """
        Get ClickHouse server version.
        
        Args:
            client: ClickHouse client connection
            
        Returns:
            Version string formatted as clickhouse_version_X_X_X
        """
        result = client.execute('SELECT version()')
        version = result[0][0]
        # Format: e.g., "23.8.1.1" -> "clickhouse_version_23_8_1"
        version_parts = version.split('.')[:3]
        return 'clickhouse_' + '_'.join(version_parts).replace('.', '_')
    
    
    def clear_query_cache(self, client: Client):
        """
        Clear ClickHouse query result cache.
        
        This ensures each query runs without cached results.
        """
        try:
            client.execute('SYSTEM DROP QUERY CACHE')
        except Exception as e:
            # Query result cache might not be available in older versions
            print(f"Warning: Could not clear query cache: {e}")
    
    
    def get_performance(self):
        """
        Execute queries and measure performance.
        
        Returns:
            Tuple of (clickhouse_version, performance_dataframe)
        """
        try:
            client = Client(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                settings={'log_queries': 1, 'final': 0}
            )
            
            all_query_performance = {}
            
            # Get ClickHouse version
            clickhouse_version = self.get_clickhouse_version(client)
              
            # Actual performance test
            for query in tqdm(self.queries, desc="Testing queries"):
                query_performance = []
                
                # Single execution warm-up (with cache cleared)
                # self.clear_query_cache(client)
                try:
                    client.execute(query)
                except Exception as e:
                    print(f"Error during warm-up: {e}")
                    raise
                
                # Run the query multiple times, measuring each
                for i in tqdm(range(self.iteration), desc="Iterations", leave=False):
                    # Clear query cache before each execution
                    self.clear_query_cache(client)
                    
                    # Get starting time
                    start_time = perf_counter()
                    
                    # Run query
                    try:
                        client.execute(query)
                    except Exception as e:
                        print(f"Error during query execution: {e}")
                        raise
                    
                    # Get ending time
                    end_time = perf_counter()
                    
                    # Calculate run time
                    run_time = end_time - start_time
                    
                    # Save run time
                    query_performance.append(run_time)
                
                all_query_performance[query.strip()] = query_performance
            
            client.disconnect()
            return clickhouse_version, pd.DataFrame(all_query_performance)
            
        except Exception as e:
            raise
    
    
    def save_result(self, clickhouse_version: str, result: pd.DataFrame):
        """
        Save performance test results to CSV file.
        
        Args:
            clickhouse_version: ClickHouse version string
            result: DataFrame containing performance data
        """
        return result.to_csv(
            f'clickhouse_{clickhouse_version}_no_cache_performance.csv', 
            index=False
        )
