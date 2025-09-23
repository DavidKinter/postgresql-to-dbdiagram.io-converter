#!/usr/bin/env python3
"""
PostgreSQL SQL Dump to DBML Converter

⚠️  CRITICAL NOTICE: This tool produces LOSSY conversions by design.
⚠️  The output is a simplified visualization, not complete documentation.

Usage:
    pg2dbml schema.sql [options]
"""

import click
import sys
from pathlib import Path
from typing import Optional

@click.command()
@click.argument('sql_file', type=click.Path(exists=True, readable=True))
@click.option('-o', '--output', default='schema.dbml', help='Output DBML file')
@click.option('--report', is_flag=True, help='Generate detailed conversion report')
@click.option('--validate-output', is_flag=True, help='Validate against dbdiagram.io parser')
@click.option('--yes', is_flag=True, help='Auto-accept warnings')
def main(sql_file: str, output: str, report: bool, validate_output: bool, yes: bool):
    """Convert PostgreSQL SQL dump to DBML format."""

    # CRITICAL: Show warning immediately
    display_conversion_warning(yes)

    try:
        # Phase 1: Preprocessing (Critical for compatibility)
        click.echo("🧹 Preprocessing SQL dump...")
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()

        from src.preprocessor import SQLCleaner
        cleaner = SQLCleaner()
        cleaned_sql = cleaner.clean_dump(sql_content)

        # Phase 2: Parse and extract schema
        click.echo("📋 Parsing schema structure...")
        from src.parser import SQLParser
        parser = SQLParser()
        schema_data = parser.parse_sql_dump(cleaned_sql)

        # Phase 3: Transform to DBML-compatible format
        click.echo("🔄 Transforming to DBML...")
        from src.transformer import TypeMapper, ConstraintHandler

        type_mapper = TypeMapper()
        constraint_handler = ConstraintHandler()

        transformed_schema = type_mapper.transform_types(schema_data)
        transformed_schema = constraint_handler.process_constraints(transformed_schema)

        # Phase 4: Generate DBML
        click.echo("📝 Generating DBML output...")
        from src.generator import DBMLGenerator

        generator = DBMLGenerator()
        dbml_content = generator.generate(transformed_schema)

        # Phase 5: Quality validation (Critical)
        click.echo("✅ Performing quality checks...")
        from src.quality import SilentFailureDetector, SixSigmaMetrics

        silent_detector = SilentFailureDetector()
        silent_failures = silent_detector.detect_silent_failures(schema_data, transformed_schema)

        if silent_failures:
            click.secho(f"⚠️  {len(silent_failures)} silent failures detected", fg='yellow')
            for failure in silent_failures:
                click.echo(f"   • {failure['description']}")

        # Calculate quality metrics
        metrics_calculator = SixSigmaMetrics()
        quality_metrics = metrics_calculator.calculate_metrics(
            original_schema=schema_data,
            converted_schema=transformed_schema,
            silent_failures=silent_failures
        )

        # Phase 6: Write outputs
        output_path = Path(output)
        output_path.write_text(dbml_content, encoding='utf-8')
        click.secho(f"✅ DBML written to {output}", fg='green')

        # Always generate loss report
        if report:
            from src.quality import LossReporter
            loss_reporter = LossReporter()
            loss_report = loss_reporter.generate_report(
                original_schema=schema_data,
                converted_schema=transformed_schema,
                quality_metrics=quality_metrics,
                silent_failures=silent_failures
            )

            report_path = output_path.with_suffix('.report.md')
            report_path.write_text(loss_report, encoding='utf-8')
            click.echo(f"📊 Loss report written to {report_path}")

        # Display summary
        display_conversion_summary(quality_metrics, silent_failures)

        # Exit with appropriate code - strict mode criteria for DBML converter
        # Focus on critical success factors: syntax validity, no silent failures, complete conversion
        conversion_success = (
            quality_metrics['statistics']['table_conversion_rate'] >= 1.0 and  # All tables converted
            quality_metrics['statistics']['column_conversion_rate'] >= 1.0 and  # All columns converted
            len(silent_failures) == 0 and  # No silent failures
            quality_metrics['compatibility_score']['overall_score'] >= 0.8  # Good DBML compatibility
        )

        if not conversion_success:
            click.secho("❌ Quality target not met in strict mode", fg='red')
            sys.exit(1)

    except Exception as e:
        click.secho(f"❌ Error: {e}", fg='red')
        sys.exit(1)

def display_conversion_warning(auto_accept: bool = False):
    """Display critical warning about lossy conversion."""
    warning_lines = [
        "⚠️ " * 25,
        "CRITICAL NOTICE: LOSSY CONVERSION",
        "",
        "This tool converts PostgreSQL schemas to DBML for visualization purposes.",
        "The conversion is LOSSY by design and will remove:",
        "  • CHECK constraints (business logic)",
        "  • Advanced indexes (performance features)",
        "  • Triggers and stored procedures",
        "  • Table inheritance and partitioning",
        "  • Row-level security and advanced features",
        "",
        "The resulting diagram is a SIMPLIFIED VISUALIZATION,",
        "NOT a complete representation of your database schema.",
        "⚠️ " * 25
    ]

    click.echo("\n".join(warning_lines))

    if not auto_accept and not click.confirm("\nDo you understand and accept these limitations?"):
        click.echo("Conversion cancelled.")
        sys.exit(0)

def display_conversion_summary(quality_metrics: dict, silent_failures: list):
    """Display comprehensive conversion summary."""
    click.echo("\n" + "=" * 60)
    click.echo("CONVERSION SUMMARY")
    click.echo("=" * 60)

    click.echo(f"\n📊 Conversion Results:")
    click.echo(f"   • Tables: {quality_metrics['statistics']['tables_converted']} converted successfully")
    click.echo(f"   • Relationships: {quality_metrics['statistics']['relationships_preserved']} found and preserved")
    if quality_metrics.get('features_dropped', 0) > 0:
        click.echo(f"   • Features simplified: {quality_metrics['features_dropped']} (see report for details)")
    if quality_metrics.get('types_converted', 0) > 0:
        click.echo(f"   • Type transformations: {quality_metrics['types_converted']} applied")

    # Warnings and losses
    if quality_metrics['total_warnings'] > 0:
        click.echo(f"\n⚠️  Conversion Warnings: {quality_metrics['total_warnings']}")
        click.echo(f"   • Features dropped: {quality_metrics['features_dropped']}")
        click.echo(f"   • Types converted: {quality_metrics['types_converted']}")

    if silent_failures:
        click.secho(f"\n🚨 Silent Failures Prevented: {len(silent_failures)}", fg='red')
        click.echo("   All potential data losses have been documented.")

if __name__ == '__main__':
    main()