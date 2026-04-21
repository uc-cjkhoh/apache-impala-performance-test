import os
import argparse

from src.test_query import get_queries
from src.test_performance import TestPerformance
from src.clickhouse_query import get_clickhouse_queries
from src.test_clickhouse_performance import TestClickHousePerformance
from src.compare_performance import ComparePerformance
from src.plot_charts import PerformanceReport


def main(args):
    if args.mode == 'test': 
        # Test Impala (default behavior)
        for query_list in get_queries():
            tester = TestPerformance(
                host=args.host, 
                port=args.port, 
                iteration=args.iteration,
                queries=query_list.queries,
                mt_dops=query_list.mt_dop
            )
            
            impala_version, result = tester.get_performance() 
            tester.save_result(impala_version, result)
    
    elif args.mode == 'test_clickhouse':
        # Test ClickHouse without query cache
        for query_list in get_clickhouse_queries():
            tester = TestClickHousePerformance(
                host=args.ch_host,
                port=args.ch_port,
                iteration=args.iteration,
                queries=query_list.queries,
                database=args.ch_database,
                user=args.ch_user,
                password=args.ch_password
            )
            
            clickhouse_version, result = tester.get_performance()
            tester.save_result(clickhouse_version, result)
        
    elif args.mode == 'compare':
        data_folder = os.path.abspath('test_result/')
        cp = ComparePerformance(data_folder)
        result, raw_data_a, raw_data_b, sample_a_name, sample_b_name = cp.perform_paired_ttest()
         
        report_gen = PerformanceReport(result, raw_data_a, raw_data_b, sample_a_name, sample_b_name)
        report_gen.generate_pdf_report(output_file='performance_comparison_report.pdf')
        

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Performance testing tool for Apache Impala and ClickHouse'
    )
    parser.add_argument(
        '-M', '--mode', 
        type=str, 
        choices=['test', 'test_clickhouse', 'compare'],
        help="Mode: 'test' for Impala, 'test_clickhouse' for ClickHouse, 'compare' for comparison"
    )
    parser.add_argument(
        '-I', '--iteration', 
        type=int, 
        default=10, 
        help='Number of iteration(s) for each query'
    )
    
    # Impala connection arguments
    parser.add_argument(
        '-H', '--host', 
        type=str, 
        default='10.168.49.12', 
        help='Impala host'
    )
    parser.add_argument(
        '-P', '--port', 
        type=int, 
        default=21050, 
        help='Impala port'
    )
    
    # ClickHouse connection arguments
    parser.add_argument(
        '--ch-host', 
        type=str, 
        default='localhost',
        help='ClickHouse host'
    )
    parser.add_argument(
        '--ch-port', 
        type=int, 
        default=9000,
        help='ClickHouse port'
    )
    parser.add_argument(
        '--ch-database',
        type=str,
        default='default',
        help='ClickHouse database'
    )
    parser.add_argument(
        '--ch-user',
        type=str,
        default='default',
        help='ClickHouse user'
    )
    parser.add_argument(
        '--ch-password',
        type=str,
        default='',
        help='ClickHouse password'
    )
    
    args = parser.parse_args()
    main(args)