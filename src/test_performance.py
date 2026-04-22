import pandas as pd

from tqdm import tqdm
from time import perf_counter
from impala.dbapi import connect


class TestPerformance:
    def __init__(self, host: str, port: int, iteration: int, queries: list[str], mt_dops: list[int]):
        self.host = host
        self.port = port
        self.iteration = iteration
        self.queries = queries
        self.mt_dops = mt_dops
        
    
    def get_performance(self): 
        try:
            conn = connect(host=self.host, port=self.port)
            cursor = conn.cursor()
            all_query_performance = {}
            
            # get impala version
            cursor.execute('SELECT VERSION()')
            impala_version = cursor.fetchall()[0][0].split('-')[0].replace(' ', '_')
            
            # sql validation
            for query in self.queries:
                cursor.execute(f'EXPLAIN {query}')
            
            # actual performance test
            for (mt_dop, query) in tqdm(zip(self.mt_dops, self.queries)):
                query_performance = []
                
                # cursor.execute(f"SET MT_DOP={mt_dop}")
                cursor.execute(f"SET MT_DOP=8")
                
                # single execution warm-up
                # cursor.execute(query)
                
                for i in tqdm(range(self.iteration)):
                    # clear metadata cache
                    # cursor.execute('INVALIDATE METADATA')
                    
                    # get starting time
                    start_time = perf_counter()
                    
                    # run query
                    cursor.execute(query)
            
                    # get ending time
                    end_time = perf_counter()
                    
                    # calculate run time
                    run_time = end_time - start_time
                    
                    # save run time
                    query_performance.append(run_time)
                    
                all_query_performance[f'MT_DOP={mt_dop};\n{query.strip()}'] = query_performance
            
            return impala_version, pd.DataFrame(all_query_performance)
            
        except Exception as e:
            raise
         
    
    def save_result(self, impala_version: str, result: pd.DataFrame):
        return result.to_csv(
            f'test_result/apache_{impala_version}_performance.csv', 
            index=False
        )
        