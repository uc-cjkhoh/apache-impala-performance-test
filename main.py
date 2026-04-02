import argparse 
import pandas as pd

from src.test_query import get_queries
from src.test_performance import TestPerformance
from src.compare_performance import ComparePerformance


def main(args):
    if args.mode == 'test': 
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
        
    elif args.mode == 'compare':
        report_dirs = 'test_result/'
        cp = ComparePerformance(report_dirs)
        
        metrics_to_compare = []
        for file in cp.files:
            perf_history = pd.DataFrame(file)
            metrics = cp.get_metrics(perf_history)
            metrics_to_compare.append(metrics)
        
        comparison_report = cp.get_comparison(metrics_to_compare)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-M', '--mode', type=str, help="['test', 'compare'] for test performancec or compare performance")
    parser.add_argument('-I', '--iteration', type=int, default=10, help='Number of iteration(s) for each query')
    parser.add_argument('-H', '--host', type=str, default='10.168.49.12', help='Impala host number')
    parser.add_argument('-P', '--port', type=int, default=21050, help='Impala port number')
    
    args = parser.parse_args()
    main(args)