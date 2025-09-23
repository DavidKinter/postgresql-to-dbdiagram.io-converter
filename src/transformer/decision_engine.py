"""
Interactive decision engine for handling ambiguous conversion decisions.
"""

from typing import Dict, List, Any, Optional
import click

class DecisionEngine:
    """Handle interactive decisions for ambiguous PostgreSQL to DBML conversions."""

    def __init__(self, interactive: bool = True):
        self.interactive = interactive
        self.decisions_made = []
        self.default_decisions = {
            'ARRAY_TYPE': 'quoted',
            'UNKNOWN_TYPE_FALLBACK': 'text',
            'CHECK_CONSTRAINT_ACTION': 'drop',
            'COMPLEX_INDEX_ACTION': 'simplify',
            'INHERITANCE_ACTION': 'flatten',
            'PARTITIONING_ACTION': 'separate_tables'
        }

    def get_decisions(self, detected_features: List[Dict]) -> Dict[str, str]:
        """
        Get conversion decisions for detected features.

        Returns decision mapping for use by other components.
        """

        decisions = self.default_decisions.copy()

        if not self.interactive:
            return decisions

        # Group features by decision type
        decision_groups = self._group_features_by_decision_type(detected_features)

        for decision_type, features in decision_groups.items():
            if features:
                decision = self._get_interactive_decision(decision_type, features)
                decisions[decision_type] = decision

        return decisions

    def _group_features_by_decision_type(self, features: List[Dict]) -> Dict[str, List[Dict]]:
        """Group features by the type of decision required."""

        groups = {
            'ARRAY_TYPE': [],
            'UNKNOWN_TYPE_FALLBACK': [],
            'CHECK_CONSTRAINT_ACTION': [],
            'COMPLEX_INDEX_ACTION': [],
            'INHERITANCE_ACTION': [],
            'PARTITIONING_ACTION': []
        }

        for feature in features:
            feature_type = feature['feature_type']

            if feature_type == 'ARRAY_TYPE':
                groups['ARRAY_TYPE'].append(feature)
            elif feature_type in ['POSTGRESQL_SPECIFIC_TYPE', 'GEOMETRIC_TYPE', 'NETWORK_TYPE']:
                groups['UNKNOWN_TYPE_FALLBACK'].append(feature)
            elif feature_type == 'CHECK_CONSTRAINT':
                groups['CHECK_CONSTRAINT_ACTION'].append(feature)
            elif feature_type in ['PARTIAL_INDEX', 'EXPRESSION_INDEX', 'OPERATOR_CLASS']:
                groups['COMPLEX_INDEX_ACTION'].append(feature)
            elif feature_type == 'TABLE_INHERITANCE':
                groups['INHERITANCE_ACTION'].append(feature)
            elif feature_type == 'TABLE_PARTITIONING':
                groups['PARTITIONING_ACTION'].append(feature)

        return groups

    def _get_interactive_decision(self, decision_type: str, features: List[Dict]) -> str:
        """Get interactive decision from user for a specific decision type."""

        click.echo(f"\n{'=' * 60}")
        click.echo(f"CONVERSION DECISION REQUIRED: {decision_type}")
        click.echo(f"{'=' * 60}")

        # Show affected features
        click.echo(f"\nAffected features ({len(features)}):")
        for i, feature in enumerate(features[:5], 1):  # Show first 5
            location = feature.get('location', 'unknown')
            description = feature.get('description', 'No description')
            click.echo(f"  {i}. {location}: {description}")

        if len(features) > 5:
            click.echo(f"  ... and {len(features) - 5} more")

        # Present options based on decision type
        if decision_type == 'ARRAY_TYPE':
            return self._get_array_type_decision(features)
        elif decision_type == 'UNKNOWN_TYPE_FALLBACK':
            return self._get_unknown_type_decision(features)
        elif decision_type == 'CHECK_CONSTRAINT_ACTION':
            return self._get_check_constraint_decision(features)
        elif decision_type == 'COMPLEX_INDEX_ACTION':
            return self._get_complex_index_decision(features)
        elif decision_type == 'INHERITANCE_ACTION':
            return self._get_inheritance_decision(features)
        elif decision_type == 'PARTITIONING_ACTION':
            return self._get_partitioning_decision(features)
        else:
            return self.default_decisions.get(decision_type, 'default')

    def _get_array_type_decision(self, features: List[Dict]) -> str:
        """Get decision for array type handling."""

        click.echo("\nArray Type Conversion Options:")
        click.echo("1. quoted - Quote array syntax for DBML compatibility (recommended)")
        click.echo("   Result: text[] becomes \"text []\"")
        click.echo("   Pros: Preserves array information, DBML compatible")
        click.echo("   Cons: May look unusual in diagrams")
        click.echo("")
        click.echo("2. text_fallback - Convert all arrays to text")
        click.echo("   Result: text[] becomes text")
        click.echo("   Pros: Clean, simple representation")
        click.echo("   Cons: Loses array semantics completely")

        choice = click.prompt(
            "\nChoose option (1-2)",
            type=click.Choice(['1', '2']),
            default='1'
        )

        decision_map = {'1': 'quoted', '2': 'text_fallback'}
        decision = decision_map[choice]

        self._log_decision('ARRAY_TYPE', decision, f"Applied to {len(features)} array columns")
        return decision

    def _get_unknown_type_decision(self, features: List[Dict]) -> str:
        """Get decision for unknown type fallback."""

        click.echo("\nUnknown Type Fallback Options:")
        click.echo("1. text - Convert to text type (recommended)")
        click.echo("   Result: inet, xml, point etc. become text")
        click.echo("   Pros: Safe, compatible")
        click.echo("   Cons: Loses type-specific validation")
        click.echo("")
        click.echo("2. varchar - Convert to varchar type")
        click.echo("   Result: inet, xml, point etc. become varchar")
        click.echo("   Pros: More restrictive than text")
        click.echo("   Cons: May need length specification")

        choice = click.prompt(
            "\nChoose option (1-2)",
            type=click.Choice(['1', '2']),
            default='1'
        )

        decision_map = {'1': 'text', '2': 'varchar'}
        decision = decision_map[choice]

        self._log_decision('UNKNOWN_TYPE_FALLBACK', decision, f"Applied to {len(features)} columns")
        return decision

    def _get_check_constraint_decision(self, features: List[Dict]) -> str:
        """Get decision for CHECK constraint handling."""

        click.echo("\nCHECK Constraint Options:")
        click.echo("1. drop - Remove all CHECK constraints (recommended)")
        click.echo("   Result: CHECK constraints removed with warnings")
        click.echo("   Pros: Clean import, no parser errors")
        click.echo("   Cons: Business logic validation lost")
        click.echo("")
        click.echo("2. comment - Convert to table comments")
        click.echo("   Result: CHECK logic documented in table notes")
        click.echo("   Pros: Logic preserved as documentation")
        click.echo("   Cons: Not enforced, may clutter diagrams")

        choice = click.prompt(
            "\nChoose option (1-2)",
            type=click.Choice(['1', '2']),
            default='1'
        )

        decision_map = {'1': 'drop', '2': 'comment'}
        decision = decision_map[choice]

        self._log_decision('CHECK_CONSTRAINT_ACTION', decision, f"Applied to {len(features)} constraints")
        return decision

    def _get_complex_index_decision(self, features: List[Dict]) -> str:
        """Get decision for complex index handling."""

        click.echo("\nComplex Index Options:")
        click.echo("1. simplify - Create simple column indexes")
        click.echo("   Result: Partial/expression indexes become basic column indexes")
        click.echo("   Pros: Some index information preserved")
        click.echo("   Cons: Optimization strategies lost")
        click.echo("")
        click.echo("2. drop - Remove complex indexes")
        click.echo("   Result: Only basic indexes preserved")
        click.echo("   Pros: Clean, simple representation")
        click.echo("   Cons: Index optimization information lost")

        choice = click.prompt(
            "\nChoose option (1-2)",
            type=click.Choice(['1', '2']),
            default='1'
        )

        decision_map = {'1': 'simplify', '2': 'drop'}
        decision = decision_map[choice]

        self._log_decision('COMPLEX_INDEX_ACTION', decision, f"Applied to {len(features)} indexes")
        return decision

    def _get_inheritance_decision(self, features: List[Dict]) -> str:
        """Get decision for table inheritance handling."""

        click.echo("\nTable Inheritance Options:")
        click.echo("1. flatten - Merge inherited columns into child tables")
        click.echo("   Result: All columns visible in child tables")
        click.echo("   Pros: Complete column information visible")
        click.echo("   Cons: Hierarchy relationship lost")
        click.echo("")
        click.echo("2. separate - Keep as separate tables")
        click.echo("   Result: Parent and child as separate tables")
        click.echo("   Pros: Cleaner representation")
        click.echo("   Cons: Shared columns not obvious")

        choice = click.prompt(
            "\nChoose option (1-2)",
            type=click.Choice(['1', '2']),
            default='1'
        )

        decision_map = {'1': 'flatten', '2': 'separate'}
        decision = decision_map[choice]

        self._log_decision('INHERITANCE_ACTION', decision, f"Applied to {len(features)} inheritance relationships")
        return decision

    def _get_partitioning_decision(self, features: List[Dict]) -> str:
        """Get decision for table partitioning handling."""

        click.echo("\nTable Partitioning Options:")
        click.echo("1. separate_tables - Represent partitions as separate tables")
        click.echo("   Result: Each partition becomes a table")
        click.echo("   Pros: All data structures visible")
        click.echo("   Cons: Partitioning strategy not obvious")
        click.echo("")
        click.echo("2. main_table_only - Show only main table")
        click.echo("   Result: Partitions hidden, main table structure shown")
        click.echo("   Pros: Cleaner diagram")
        click.echo("   Cons: Partition structure completely hidden")

        choice = click.prompt(
            "\nChoose option (1-2)",
            type=click.Choice(['1', '2']),
            default='1'
        )

        decision_map = {'1': 'separate_tables', '2': 'main_table_only'}
        decision = decision_map[choice]

        self._log_decision('PARTITIONING_ACTION', decision, f"Applied to {len(features)} partitioned tables")
        return decision

    def _log_decision(self, decision_type: str, decision: str, context: str):
        """Log a decision that was made."""

        decision_record = {
            'decision_type': decision_type,
            'decision': decision,
            'context': context,
            'timestamp': self._get_timestamp()
        }

        self.decisions_made.append(decision_record)

    def _get_timestamp(self) -> str:
        """Get current timestamp for logging."""
        from datetime import datetime
        return datetime.now().isoformat()

    def get_decisions_report(self) -> Dict[str, Any]:
        """Get report of all decisions made."""

        return {
            'total_decisions': len(self.decisions_made),
            'decisions': self.decisions_made,
            'interactive_mode': self.interactive
        }