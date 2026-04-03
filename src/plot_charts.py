import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Image, Table, TableStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
import io
import os
import argparse

from src.compare_performance import ComparePerformance

class PerformanceReport:
    """
    A class to generate a comprehensive PDF report for A/B testing results
    comparing Impala performance between two versions, including visual charts.
    """

    def __init__(self, df: pd.DataFrame, raw_data_a: pd.DataFrame, raw_data_b: pd.DataFrame, sample_a_name: str, sample_b_name: str):
        """
        Initialize the report generator with the A/B testing results DataFrame and raw data.

        Args:
            df (pd.DataFrame): DataFrame containing statistical results with columns:
                - sample_a: Name of sample A file
                - sample_b: Name of sample B file
                - query: The SQL query tested
                - t_value: t-statistic from paired t-test
                - p_value: p-value from paired t-test
                - is_reject_H0: 1 if null hypothesis rejected, 0 otherwise
                - description: Human-readable description of the result
            raw_data_a (pd.DataFrame): Raw performance data for sample A
            raw_data_b (pd.DataFrame): Raw performance data for sample B
            sample_a_name (str): filename or version label for sample A
            sample_b_name (str): filename or version label for sample B
        """
        self.df = df.copy()
        self.raw_data_a = raw_data_a.copy()
        self.raw_data_b = raw_data_b.copy()
        self.sample_a_name = sample_a_name
        self.sample_b_name = sample_b_name

    def _create_boxplot_chart(self, query_idx: int, query_name: str) -> str:
        """
        Create a box plot for a specific query showing performance distributions.

        Args:
            query_idx (int): Index of the query in the DataFrame
            query_name (str): Shortened name for the query

        Returns:
            str: Path to the saved chart image
        """
        fig, ax = plt.subplots(figsize=(10, 6))

        # Get data for this query
        data_a = self.raw_data_a.iloc[:, query_idx]
        data_b = self.raw_data_b.iloc[:, query_idx]

        # Create box plot
        bp = ax.boxplot([data_a, data_b], tick_labels=[self.sample_a_name, self.sample_b_name],
                       patch_artist=True, medianprops=dict(color='black', linewidth=2))

        # Color the boxes
        colors_list = ['lightblue', 'lightgreen']
        for patch, color in zip(bp['boxes'], colors_list):
            patch.set_facecolor(color)
            patch.set_alpha(0.7)

        ax.set_ylabel('Execution Time (seconds)')
        ax.set_title(f'Performance Distribution - Query {query_idx + 1}')
        ax.grid(True, alpha=0.3)

        # Add statistical annotations
        t_val = self.df.iloc[query_idx]['t_value']
        p_val = self.df.iloc[query_idx]['p_value']
        significance = "Significant" if self.df.iloc[query_idx]['is_reject_H0'] else "Not Significant"

        ax.text(0.02, 0.98, f't-value: {t_val:.3f}\np-value: {p_val:.4f}\n{significance}',
                transform=ax.transAxes, verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))

        plt.tight_layout()

        # Save to buffer
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
        buf.seek(0)
        plt.close()

        return buf

    def _create_summary_charts(self) -> list:
        """
        Create summary charts showing overall statistics.

        Returns:
            list: List of chart buffers
        """
        charts = []

        # Chart 1: Bar chart of t-values
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

        queries = [f'Q{i+1}' for i in range(len(self.df))]
        t_values = self.df['t_value']
        p_values = self.df['p_value']
        colors_t = ['red' if x > 0 else 'blue' for x in t_values]

        # T-values bar chart
        bars = ax1.bar(queries, t_values, color=colors_t, alpha=0.7)
        ax1.axhline(y=0, color='black', linestyle='-', alpha=0.3)
        ax1.set_ylabel('t-value')
        ax1.set_title('t-Values for All Queries')
        ax1.set_xlabel('Query')
        ax1.grid(True, alpha=0.3)

        # P-values scatter plot
        scatter = ax2.scatter(range(len(p_values)), p_values, c=['red' if x < 0.05 else 'blue' for x in p_values], s=50)
        ax2.axhline(y=0.05, color='red', linestyle='--', alpha=0.7, label='Significance Threshold (α=0.05)')
        ax2.set_yscale('log')
        ax2.set_ylabel('p-value (log scale)')
        ax2.set_xlabel('Query Index')
        ax2.set_title('p-Values Distribution')
        ax2.legend()
        ax2.grid(True, alpha=0.3)

        plt.tight_layout()
        buf1 = io.BytesIO()
        plt.savefig(buf1, format='png', dpi=150, bbox_inches='tight')
        buf1.seek(0)
        plt.close()
        charts.append(buf1)

        # Chart 2: Performance comparison summary
        fig, ax = plt.subplots(figsize=(10, 6))

        significant_queries = self.df[self.df['is_reject_H0'] == 1]
        better_a = sum(significant_queries['t_value'] < 0)
        better_b = sum(significant_queries['t_value'] > 0)
        no_diff = len(self.df) - len(significant_queries)

        categories = [f'{self.sample_a_name} Better', f'{self.sample_b_name} Better', 'No Difference']
        values = [better_a, better_b, no_diff]
        colors_pie = ['lightcoral', 'lightgreen', 'lightgray']

        wedges, texts, autotexts = ax.pie(values, labels=categories, colors=colors_pie, autopct='%1.1f%%',
                                          startangle=90, wedgeprops=dict(width=0.6))
        ax.set_title('Performance Comparison Summary')

        # Add legend with counts
        ax.legend([f'{cat}: {val}' for cat, val in zip(categories, values)], loc='lower right')

        plt.tight_layout()
        buf2 = io.BytesIO()
        plt.savefig(buf2, format='png', dpi=150, bbox_inches='tight')
        buf2.seek(0)
        plt.close()
        charts.append(buf2)

        return charts

    def generate_pdf_report(self, output_file: str = 'performance_report.pdf'):
        """
        Generate a comprehensive PDF report with charts and explanations.

        Args:
            output_file (str): Path to save the PDF file
        """
        doc = SimpleDocTemplate(output_file, pagesize=A4)
        styles = getSampleStyleSheet()

        # Custom styles
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            spaceAfter=30,
            alignment=TA_CENTER,
        )

        heading_style = ParagraphStyle(
            'CustomHeading',
            parent=styles['Heading2'],
            fontSize=16,
            spaceAfter=20,
        )

        subheading_style = ParagraphStyle(
            'CustomSubheading',
            parent=styles['Heading3'],
            fontSize=14,
            spaceAfter=15,
        )

        normal_style = styles['Normal']
        normal_style.fontSize = 10
        normal_style.leading = 14

        story = []

        # Title Page
        story.append(Paragraph("A/B Testing Performance Report", title_style))
        story.append(Paragraph("Impala Version Comparison", title_style))
        story.append(Spacer(1, 0.5*inch))
        story.append(Paragraph("Comparing Apache Impala 4.3.0 vs 4.5.0", styles['Heading3']))
        story.append(Spacer(1, 1*inch))
        story.append(Paragraph(f"Report Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}", normal_style))
        story.append(PageBreak())

        # Introduction
        story.append(Paragraph("Introduction", heading_style))
        intro_text = f"""
        This report presents a comprehensive analysis of A/B testing results comparing the performance of two Apache Impala versions:
        <br/><br/>
        • <b>Sample A</b>: {self.sample_a_name}
        <br/>
        • <b>Sample B</b>: {self.sample_b_name}
        <br/><br/>
        A/B testing is a statistical method used to compare two variants to determine which one performs better. In this context, we're measuring query execution performance to identify if {self.sample_b_name} offers performance differences compared to {self.sample_a_name}.
        """
        story.append(Paragraph(intro_text, normal_style))
        story.append(Spacer(1, 0.3*inch))

        # Methodology
        story.append(Paragraph("Methodology", heading_style))
        methodology_text = """
        <b>Statistical Test Used:</b> Two-sample paired t-test
        <br/>
        <b>Purpose:</b> Compare means of two related groups (paired samples from the same queries)
        <br/><br/>
        <b>Hypotheses:</b>
        <br/>
        • <b>Null Hypothesis (H₀):</b> There is no significant difference in query execution times between Impala 4.3.0 and 4.5.0
        <br/>
        • <b>Alternative Hypothesis (H₁):</b> There is a significant difference in query execution times
        <br/><br/>
        <b>Significance Level:</b> α (alpha) = 0.05 (5% significance level)
        <br/>
        • If p-value < 0.05, we reject H₀ and conclude there's a significant difference
        <br/>
        • If p-value ≥ 0.05, we fail to reject H₀ (no significant difference detected)
        <br/><br/>
        <b>Interpretation of Results:</b>
        <br/>
        • <b>t-value:</b> Measures the difference between group means relative to the variation within groups
        <br/>
        • Positive t-value: Sample A (4.3.0) is slower than Sample B (4.5.0)
        <br/>
        • Negative t-value: Sample A (4.3.0) is faster than Sample B (4.5.0)
        <br/>
        • <b>p-value:</b> Probability of observing the data assuming H₀ is true
        """
        story.append(Paragraph(methodology_text, normal_style))
        story.append(Spacer(1, 0.3*inch))

        # Summary Statistics
        story.append(Paragraph("Summary Statistics", heading_style))

        total_queries = len(self.df)
        significant_differences = self.df['is_reject_H0'].sum()
        no_difference = total_queries - significant_differences
        sample_a_better = self.df[self.df['description'].str.contains('Sample A is faster', na=False)].shape[0]
        sample_b_better = self.df[self.df['description'].str.contains('Sample A is slower', na=False)].shape[0]

        summary_data = [
            ['Metric', 'Value'],
            ['Total queries tested', str(total_queries)],
            ['Queries with significant performance difference', f"{significant_differences} ({significant_differences/total_queries*100:.1f}%)"],
            ['Queries with no significant difference', f"{no_difference} ({no_difference/total_queries*100:.1f}%)"],
        ]

        if significant_differences > 0:
            summary_data.extend([
                ['Queries where 4.3.0 performs better', str(sample_a_better)],
                ['Queries where 4.5.0 performs better', str(sample_b_better)],
            ])

        summary_table = Table(summary_data)
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))

        story.append(summary_table)
        story.append(Spacer(1, 0.3*inch))

        # Summary Charts
        story.append(Paragraph("Overall Performance Analysis", heading_style))

        summary_charts = self._create_summary_charts()
        for i, chart_buf in enumerate(summary_charts):
            img = Image(chart_buf)
            img.drawHeight = 4*inch
            img.drawWidth = 6*inch
            story.append(img)
            story.append(Spacer(1, 0.2*inch))

        story.append(PageBreak())

        # Detailed Results
        story.append(Paragraph("Detailed Results by Query", heading_style))

        for idx, row in self.df.iterrows():
            story.append(Paragraph(f"Query {idx + 1}", subheading_style))

            # Query text (full text included)
            query_text = row['query'].strip()
            story.append(Paragraph(f"<b>Query:</b> {query_text.replace(chr(10), '<br/>')}", normal_style))
            story.append(Spacer(1, 0.1*inch))

            # Box plot
            chart_buf = self._create_boxplot_chart(idx, f"Query {idx + 1}")
            img = Image(chart_buf)
            img.drawHeight = 3*inch
            img.drawWidth = 5*inch
            story.append(img)
            story.append(Spacer(1, 0.1*inch))

            # Statistical results
            story.append(Paragraph("Statistical Results:", styles['Heading4']))
            stats_data = [
                ['Metric', 'Value'],
                ['t-value', f"{row['t_value']:.6f}"],
                ['p-value', f"{row['p_value']:.6f}"],
                ['Reject H₀', 'Yes' if row['is_reject_H0'] else 'No'],
                ['Conclusion', row['description']],
            ]

            stats_table = Table(stats_data)
            stats_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('FONTSIZE', (0, 0), (-1, -1), 9),
            ]))

            story.append(stats_table)
            story.append(Spacer(1, 0.2*inch))

            # Recorded performance stats for the query
            a_series = self.raw_data_a.iloc[:, idx]
            b_series = self.raw_data_b.iloc[:, idx]
            perf_summary = [
                ['', self.sample_a_name, self.sample_b_name],
                ['mean', f"{a_series.mean():.6f}", f"{b_series.mean():.6f}"],
                ['median', f"{a_series.median():.6f}", f"{b_series.median():.6f}"],
                ['std', f"{a_series.std():.6f}", f"{b_series.std():.6f}"],
                ['min', f"{a_series.min():.6f}", f"{b_series.min():.6f}"],
                ['max', f"{a_series.max():.6f}", f"{b_series.max():.6f}"],
            ]

            story.append(Paragraph("Recorded performance (execution times) for this query:", normal_style))
            perf_summary_table = Table(perf_summary)
            perf_summary_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.lightblue),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('FONTSIZE', (0, 0), (-1, -1), 8),
            ]))
            story.append(perf_summary_table)
            story.append(Spacer(1, 0.2*inch))

            # Detailed analysis
            story.append(Paragraph("Detailed Analysis:", styles['Heading4']))
            if row['is_reject_H0']:
                analysis = f"""
                <b>Significant difference detected</b> (p-value = {row['p_value']:.6f} < 0.05)
                <br/>
                <b>Performance comparison:</b> {'Impala 4.3.0 executes this query significantly slower than Impala 4.5.0' if row['t_value'] > 0 else 'Impala 4.3.0 executes this query significantly faster than Impala 4.5.0'}
                <br/>
                <b>Implication:</b> {'Version 4.5.0 shows performance improvement for this query' if row['t_value'] > 0 else 'Version 4.3.0 performs better for this specific query'}
                """
            else:
                analysis = f"""
                <b>No significant difference</b> (p-value = {row['p_value']:.6f} ≥ 0.05)
                <br/>
                <b>Performance comparison:</b> Both versions perform similarly for this query
                <br/>
                <b>Implication:</b> No clear performance advantage for either version
                """

            story.append(Paragraph(analysis, normal_style))
            story.append(Spacer(1, 0.3*inch))

            # Page break every 3 queries to avoid overcrowding
            if (idx + 1) % 3 == 0 and idx < len(self.df) - 1:
                story.append(PageBreak())

        # Overall Conclusion
        story.append(PageBreak())
        story.append(Paragraph("Overall Conclusion", heading_style))

        if significant_differences == 0:
            conclusion = """
            <b>No significant performance differences were found between Impala versions 4.3.0 and 4.5.0</b> across all tested queries. Both versions perform equivalently within the statistical bounds of this analysis.
            """
        else:
            conclusion = f"""
            <b>Significant performance differences were detected in {significant_differences} out of {total_queries} queries ({significant_differences/total_queries*100:.1f}%)**.</b>
            <br/>
            {'Overall, <b>Impala 4.5.0 shows better performance</b> in the majority of queries where differences were significant.' if sample_b_better > sample_a_better else 'Overall, <b>Impala 4.3.0 shows better performance</b> in the majority of queries where differences were significant.' if sample_a_better > sample_b_better else 'The performance advantages are evenly distributed between the two versions.'}
            <br/><br/>
            <b>Recommendation:</b> Consider upgrading to Impala 4.5.0 for potential performance improvements, but test specific query patterns in your environment before deployment.
            """

        story.append(Paragraph(conclusion, normal_style))

        # Build the PDF
        doc.build(story)
        print(f"PDF report saved to {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate performance PDF report from test_result folder.')
    parser.add_argument('--data-folder', type=str, default='test_result', help='Folder containing two performance CSV files')
    parser.add_argument('--output', type=str, default='performance_comparison_report.pdf', help='Output PDF path')

    args = parser.parse_args()

    data_folder = os.path.abspath(args.data_folder)
    if not os.path.isdir(data_folder):
        raise FileNotFoundError(f"Data folder not found: {data_folder}")

    cp = ComparePerformance(data_folder)
    result, raw_data_a, raw_data_b, sample_a_name, sample_b_name = cp.perform_paired_ttest()

    report_gen = PerformanceReport(result, raw_data_a, raw_data_b, sample_a_name, sample_b_name)
    report_gen.generate_pdf_report(output_file=args.output)
    print(f"Done. PDF report written to {args.output}")
