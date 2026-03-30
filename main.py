import argparse 
 
from src.test_query import get_queries
from src.test_performance import TestPerformance
from src.compare_performance import ComparePerformance


def main(args):
    if args.mode == 'test': 
        tester = TestPerformance(
            host=args.host, 
            port=args.port, 
            iteration=args.iteration,
            queries=get_queries()
        )
         
        impala_version, result = tester.get_performance() 
        tester.save_result(impala_version, result)
        
    elif args.mode == 'compare':
        comparison = ComparePerformance()
        
        # result = comparison.get_comparison()
        # comparison.save_result(result)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-M', '--mode', type=str, help="['test', 'compare'] for test performancec or compare performance")
    parser.add_argument('-I', '--iteration', type=int, default=30, help='Number of iteration(s) for each query')
    parser.add_argument('-H', '--host', type=str, default='10.168.49.12', help='Impala host number')
    parser.add_argument('-P', '--port', type=int, default=21050, help='Impala port number')
    
    args = parser.parse_args()
    main(args)