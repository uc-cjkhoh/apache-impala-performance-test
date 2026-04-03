import os
import argparse 
import pandas as pd

from src.test_query import get_queries
from src.test_performance import TestPerformance
from src.compare_performance import ComparePerformance
from src.plot_charts import PerformanceReport


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
        data_folder = os.path.abspath('test_result/')
        cp = ComparePerformance(data_folder)
    
        result, raw_data_a, raw_data_b, sample_a_name, sample_b_name = cp.perform_paired_ttest()
        
        # Generate comprehensive report
        report_gen = PerformanceReport(result, raw_data_a, raw_data_b, sample_a_name, sample_b_name)
        report_gen.generate_pdf_report(output_file='performance_comparison_report.pdf')
        print("Performance comparison report generated: performance_comparison_report.pdf")
        print("\nReport Summary:")
        print(f"- Total queries: {len(result)}")
        print(f"- Significant differences: {result['is_reject_H0'].sum()}")
        print(f"- No significant differences: {len(result) - result['is_reject_H0'].sum()}")
        

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-M', '--mode', type=str, help="['test', 'compare'] for test performancec or compare performance")
    parser.add_argument('-I', '--iteration', type=int, default=10, help='Number of iteration(s) for each query')
    parser.add_argument('-H', '--host', type=str, default='10.168.49.12', help='Impala host number')
    parser.add_argument('-P', '--port', type=int, default=21050, help='Impala port number')
    
    args = parser.parse_args()
    main(args)