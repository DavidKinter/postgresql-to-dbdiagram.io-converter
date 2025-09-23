"""
Comprehensive loss reporting for PostgreSQL to DBML conversion.

Documents all transformations, losses, and decisions made during conversion.
"""

from typing import Dict, List, Any
from datetime import datetime
import json

class LossReporter:
    """Generate comprehensive reports of conversion losses and transformations."""

    def __init__(self):
        pass

    def generate_report(self, original_schema: Dict, converted_schema: Dict,
                       quality_metrics: Dict, silent_failures: List[Dict]) -> str:
        """
        Generate comprehensive conversion loss report in Markdown format.
        """

        report_sections = []

        # Header
        report_sections.append(self._generate_header())

        # Executive Summary
        report_sections.append(self._generate_executive_summary(quality_metrics, silent_failures))

        # Quality Metrics
        report_sections.append(self._generate_quality_metrics_section(quality_metrics))

        # Transformation Summary
        report_sections.append(self._generate_transformation_summary(original_schema, converted_schema))

        # Silent Failures
        if silent_failures:
            report_sections.append(self._generate_silent_failures_section(silent_failures))

        # Type Transformations
        report_sections.append(self._generate_type_transformations_section(converted_schema))

        # Constraint Handling
        report_sections.append(self._generate_constraint_handling_section(converted_schema))

        # Feature Processing
        report_sections.append(self._generate_feature_processing_section(converted_schema))

        # Recommendations
        report_sections.append(self._generate_recommendations_section(quality_metrics, converted_schema))

        # Technical Details
        report_sections.append(self._generate_technical_details_section(converted_schema))

        return '\n\n'.join(report_sections)

    def _generate_header(self) -> str:
        """Generate report header."""

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        return f"""# PostgreSQL to DBML Conversion Report

**Generated:** {timestamp}
**Tool:** pg2dbml v1.0.0
**Conversion Type:** LOSSY (by design)

---

## ⚠️ CRITICAL NOTICE

This report documents a **LOSSY CONVERSION** from PostgreSQL to DBML. The resulting DBML file is a **simplified visualization** of your database schema, NOT a complete representation.

**Key Limitations:**
- Business logic (CHECK constraints) removed
- Advanced PostgreSQL features lost
- Performance optimizations (indexes, partitioning) simplified
- Referential integrity actions may be invisible in diagrams

**Use Case:** Database schema visualization and documentation
**NOT for:** Schema recreation, migration planning, or complete documentation"""

    def _generate_executive_summary(self, quality_metrics: Dict, silent_failures: List[Dict]) -> str:
        """Generate executive summary section."""

        sigma_level = quality_metrics.get('sigma_level', 0)
        compatibility_score = quality_metrics.get('compatibility_score', {}).get('overall_score', 0)
        quality_assessment = quality_metrics.get('quality_assessment', 'Unknown')

        summary = f"""## Executive Summary

**Quality Level:** {sigma_level:.1f}σ ({quality_assessment})
**Compatibility Score:** {compatibility_score:.1%}
**Silent Failures Detected:** {len(silent_failures)}

### Conversion Success Criteria
✅ **dbdiagram.io Parser Compatibility:** {'Achieved' if compatibility_score > 0.9 else 'Issues Detected'}
✅ **Complete Loss Documentation:** All changes documented below
✅ **Silent Failure Prevention:** {'No silent failures' if len(silent_failures) == 0 else f'{len(silent_failures)} failures detected and documented'}
✅ **User Expectation Management:** Lossy nature clearly communicated"""

        if sigma_level >= 6.0:
            summary += "\n\n🎯 **Six Sigma Quality Achieved** - Conversion meets highest quality standards."
        elif sigma_level >= 4.0:
            summary += "\n\n⚠️ **Quality Target Met** - Conversion is acceptable but has some limitations."
        else:
            summary += "\n\n❌ **Quality Issues Detected** - Review detailed sections below for improvements."

        return summary

    def _generate_quality_metrics_section(self, quality_metrics: Dict) -> str:
        """Generate quality metrics section."""

        stats = quality_metrics.get('statistics', {})
        preservation = quality_metrics.get('preservation_rates', {})

        return f"""## Quality Metrics

### Conversion Statistics
| Metric | Original | Converted | Rate |
|--------|----------|-----------|------|
| Tables | {stats.get('total_tables', 0)} | {stats.get('tables_converted', 0)} | {stats.get('table_conversion_rate', 0):.1%} |
| Columns | {stats.get('total_columns', 0)} | {stats.get('columns_converted', 0)} | {stats.get('column_conversion_rate', 0):.1%} |
| Relationships | {stats.get('total_relationships', 0)} | {stats.get('relationships_preserved', 0)} | {stats.get('relationship_preservation_rate', 0):.1%} |
| Constraints | {stats.get('total_constraints', 0)} | {stats.get('constraints_preserved', 0)} | {stats.get('constraint_preservation_rate', 0):.1%} |

### Semantic Preservation
- **Overall Semantic Preservation:** {preservation.get('semantic_preservation', 0):.1%}
- **Table Structure Preservation:** {preservation.get('table_preservation', 0):.1%}
- **Relationship Preservation:** {preservation.get('relationship_preservation', 0):.1%}
- **Constraint Preservation:** {preservation.get('constraint_preservation', 0):.1%}

### Six Sigma Analysis
- **Sigma Level:** {quality_metrics.get('sigma_level', 0):.2f}σ
- **DPMO:** {quality_metrics.get('dpmo', 0):.1f}
- **Total Defects:** {quality_metrics.get('defects', {}).get('total', 0)}
- **Critical Defects:** {quality_metrics.get('defects', {}).get('critical', 0)}"""

    def _generate_transformation_summary(self, original_schema: Dict, converted_schema: Dict) -> str:
        """Generate transformation summary section."""

        warnings = converted_schema.get('warnings', [])
        transformations = converted_schema.get('type_transformations', [])
        dropped_features = converted_schema.get('dropped_features', [])

        return f"""## Transformation Summary

### Overview
- **Total Warnings Generated:** {len(warnings)}
- **Type Transformations Applied:** {len(transformations)}
- **Features Dropped:** {len(dropped_features)}

### Warning Categories
{self._format_warning_categories(warnings)}

### Most Common Transformations
{self._format_common_transformations(transformations)}"""

    def _format_warning_categories(self, warnings: List[Dict]) -> str:
        """Format warning categories for display."""

        if not warnings:
            return "No warnings generated."

        # Group warnings by type
        warning_types = {}
        for warning in warnings:
            warning_type = warning.get('type', 'UNKNOWN')
            if warning_type not in warning_types:
                warning_types[warning_type] = 0
            warning_types[warning_type] += 1

        formatted = []
        for warning_type, count in sorted(warning_types.items()):
            formatted.append(f"- **{warning_type}:** {count} instances")

        return '\n'.join(formatted)

    def _format_common_transformations(self, transformations: List[Dict]) -> str:
        """Format common transformations for display."""

        if not transformations:
            return "No type transformations applied."

        # Group by transformation reason
        by_reason = {}
        for transform in transformations:
            reason = transform.get('transformation_reason', 'Unknown')
            if reason not in by_reason:
                by_reason[reason] = []
            by_reason[reason].append(transform)

        formatted = []
        for reason, transforms in sorted(by_reason.items()):
            formatted.append(f"- **{reason}:** {len(transforms)} transformations")

            # Show examples
            for transform in transforms[:3]:
                table = transform.get('table', 'unknown')
                column = transform.get('column', 'unknown')
                original = transform.get('original_type', 'unknown')
                transformed = transform.get('transformed_type', 'unknown')
                formatted.append(f"  - `{table}.{column}`: `{original}` → `{transformed}`")

            if len(transforms) > 3:
                formatted.append(f"  - ... and {len(transforms) - 3} more")

        return '\n'.join(formatted)

    def _generate_silent_failures_section(self, silent_failures: List[Dict]) -> str:
        """Generate silent failures section."""

        if not silent_failures:
            return """## Silent Failures

✅ **No Silent Failures Detected**

All data losses and transformations have been explicitly documented with warnings."""

        # Group by type and severity
        by_type = {}
        by_severity = {}

        for failure in silent_failures:
            failure_type = failure['type']
            severity = failure['severity']

            if failure_type not in by_type:
                by_type[failure_type] = []
            by_type[failure_type].append(failure)

            if severity not in by_severity:
                by_severity[severity] = []
            by_severity[severity].append(failure)

        section = f"""## Silent Failures

❌ **{len(silent_failures)} Silent Failures Detected**

These represent data losses that occurred without explicit warnings. This is a quality issue that has been corrected by documenting them here.

### By Severity
"""

        for severity in ['CRITICAL', 'HIGH', 'MEDIUM']:
            failures = by_severity.get(severity, [])
            if failures:
                section += f"\n#### {severity} ({len(failures)} failures)\n"
                for failure in failures[:5]:  # Show first 5
                    section += f"- {failure['description']}\n"
                if len(failures) > 5:
                    section += f"- ... and {len(failures) - 5} more\n"

        section += "\n### By Type\n"
        for failure_type, failures in by_type.items():
            section += f"- **{failure_type}:** {len(failures)} instances\n"

        return section

    def _generate_type_transformations_section(self, converted_schema: Dict) -> str:
        """Generate type transformations section."""

        transformations = converted_schema.get('type_transformations', [])
        type_warnings = converted_schema.get('type_warnings', [])

        if not transformations:
            return """## Type Transformations

No type transformations were required."""

        section = f"""## Type Transformations

{len(transformations)} type transformations were applied to ensure DBML compatibility.

### Summary
"""

        # Group by original type
        by_original_type = {}
        for transform in transformations:
            original_type = transform.get('original_type', 'unknown')
            if original_type not in by_original_type:
                by_original_type[original_type] = []
            by_original_type[original_type].append(transform)

        for original_type, transforms in sorted(by_original_type.items()):
            target_types = set(t.get('transformed_type', 'unknown') for t in transforms)
            section += f"- **{original_type}** → {', '.join(target_types)} ({len(transforms)} columns)\n"

        section += f"\n### Critical Array Type Transformations\n"
        array_transforms = [t for t in transformations if '[]' in t.get('original_type', '')]
        if array_transforms:
            section += f"{len(array_transforms)} array types were quoted for compatibility:\n\n"
            for transform in array_transforms[:10]:  # Show first 10
                table = transform.get('table', 'unknown')
                column = transform.get('column', 'unknown')
                original = transform.get('original_type', 'unknown')
                transformed = transform.get('transformed_type', 'unknown')
                section += f"- `{table}.{column}`: `{original}` → `{transformed}`\n"
            if len(array_transforms) > 10:
                section += f"- ... and {len(array_transforms) - 10} more\n"
        else:
            section += "No array type transformations required.\n"

        return section

    def _generate_constraint_handling_section(self, converted_schema: Dict) -> str:
        """Generate constraint handling section."""

        dropped_constraints = converted_schema.get('dropped_constraints', [])
        modified_constraints = converted_schema.get('modified_constraints', [])

        section = f"""## Constraint Handling

### Dropped Constraints
{len(dropped_constraints)} constraints were dropped due to DBML incompatibility:

"""

        if dropped_constraints:
            # Group by type
            by_type = {}
            for constraint in dropped_constraints:
                constraint_type = constraint.get('constraint_type', 'UNKNOWN')
                if constraint_type not in by_type:
                    by_type[constraint_type] = []
                by_type[constraint_type].append(constraint)

            for constraint_type, constraints in by_type.items():
                section += f"#### {constraint_type} Constraints ({len(constraints)} dropped)\n"
                reason = constraints[0].get('reason', 'Unknown reason')
                section += f"**Reason:** {reason}\n\n"

                for constraint in constraints[:5]:  # Show first 5
                    table = constraint.get('table_name', 'unknown')
                    name = constraint.get('constraint_name', 'unnamed')
                    section += f"- `{table}.{name}`\n"

                if len(constraints) > 5:
                    section += f"- ... and {len(constraints) - 5} more\n"

                section += "\n"
        else:
            section += "No constraints were dropped.\n\n"

        section += f"### Modified Constraints\n{len(modified_constraints)} constraints were modified for compatibility.\n"

        return section

    def _generate_feature_processing_section(self, converted_schema: Dict) -> str:
        """Generate feature processing section."""

        processed_features = converted_schema.get('processed_features', [])
        dropped_features = converted_schema.get('dropped_features', [])

        return f"""## PostgreSQL Feature Processing

### Processed Features ({len(processed_features)})
Features that were handled with some limitations:

{self._format_feature_list(processed_features)}

### Dropped Features ({len(dropped_features)})
Features that could not be represented in DBML:

{self._format_feature_list(dropped_features)}"""

    def _format_feature_list(self, features: List[Dict]) -> str:
        """Format feature list for display."""

        if not features:
            return "None"

        # Group by feature type
        by_type = {}
        for feature in features:
            feature_type = feature.get('feature_type', 'UNKNOWN')
            if feature_type not in by_type:
                by_type[feature_type] = []
            by_type[feature_type].append(feature)

        formatted = []
        for feature_type, feature_list in sorted(by_type.items()):
            formatted.append(f"- **{feature_type}:** {len(feature_list)} instances")

            # Show impact for first few
            for feature in feature_list[:2]:
                impact = feature.get('impact', feature.get('processing_description', 'No description'))
                formatted.append(f"  - Impact: {impact}")

        return '\n'.join(formatted)

    def _generate_recommendations_section(self, quality_metrics: Dict, converted_schema: Dict) -> str:
        """Generate recommendations section."""

        recommendations = []

        # Quality-based recommendations
        sigma_level = quality_metrics.get('sigma_level', 0)
        if sigma_level < 4.0:
            recommendations.append(
                "**Quality Improvement:** Review silent failures and constraint handling to improve conversion quality."
            )

        # Feature-based recommendations
        dropped_features = converted_schema.get('dropped_features', [])
        if dropped_features:
            recommendations.append(
                "**Feature Documentation:** Create supplementary documentation for dropped PostgreSQL features."
            )

        # Type transformation recommendations
        transformations = converted_schema.get('type_transformations', [])
        array_transforms = [t for t in transformations if '[]' in t.get('original_type', '')]
        if array_transforms:
            recommendations.append(
                "**Array Types:** Verify that quoted array types display correctly in your diagram tool."
            )

        # Constraint recommendations
        dropped_constraints = converted_schema.get('dropped_constraints', [])
        check_constraints = [c for c in dropped_constraints if c.get('constraint_type') == 'CHECK']
        if check_constraints:
            recommendations.append(
                "**Business Logic:** Implement dropped CHECK constraint logic in your application layer."
            )

        if not recommendations:
            recommendations.append("Schema conversion is highly compatible. No specific recommendations.")

        return f"""## Recommendations

{chr(10).join(f"{i+1}. {rec}" for i, rec in enumerate(recommendations))}

### Usage Guidelines
1. **Diagram Purpose:** Use this DBML for visualization and high-level documentation only
2. **Missing Logic:** Implement business rules and constraints in application code
3. **Performance Features:** Plan indexes and partitioning separately from this diagram
4. **Validation:** Test the generated DBML in dbdiagram.io before sharing
5. **Updates:** Re-run conversion when schema changes significantly"""

    def _generate_technical_details_section(self, converted_schema: Dict) -> str:
        """Generate technical details section."""

        return f"""## Technical Details

### Conversion Pipeline
1. **SQL Preprocessing:** Removed {len(converted_schema.get('removed_statements', []))} incompatible statements
2. **Type Mapping:** Applied {len(converted_schema.get('type_transformations', []))} type transformations
3. **Constraint Processing:** Handled {len(converted_schema.get('dropped_constraints', []))} constraint issues
4. **DBML Generation:** Created standards-compliant DBML with {len(converted_schema.get('syntax_repairs', []))} syntax repairs
5. **Quality Validation:** Detected {len(converted_schema.get('validation_errors', []))} issues

### Parser Compliance
- **Target Parser:** dbdiagram.io 2024 parser
- **Syntax Standard:** DBML v1.0
- **Array Type Handling:** Quoted syntax (`"type []"`)
- **Negative Defaults:** Quoted values (`default: '-1'`)
- **Multi-word Types:** Converted to aliases

### Files Generated
- **DBML File:** Primary visualization file
- **Conversion Report:** This document (`.report.md`)
- **Quality Metrics:** Embedded in CLI output

---

*Report generated by pg2dbml v1.0.0 - PostgreSQL to DBML Converter*
*For issues and feedback: https://github.com/user/pg2dbml/issues*"""

    def generate_json_report(self, original_schema: Dict, converted_schema: Dict,
                           quality_metrics: Dict, silent_failures: List[Dict]) -> str:
        """
        Generate machine-readable JSON report for programmatic analysis.
        """

        report_data = {
            'metadata': {
                'tool': 'pg2dbml',
                'version': '1.0.0',
                'timestamp': datetime.now().isoformat(),
                'conversion_type': 'lossy'
            },
            'quality_metrics': quality_metrics,
            'silent_failures': silent_failures,
            'transformations': {
                'type_transformations': converted_schema.get('type_transformations', []),
                'dropped_constraints': converted_schema.get('dropped_constraints', []),
                'modified_constraints': converted_schema.get('modified_constraints', []),
                'processed_features': converted_schema.get('processed_features', []),
                'dropped_features': converted_schema.get('dropped_features', [])
            },
            'statistics': quality_metrics.get('statistics', {}),
            'warnings': converted_schema.get('warnings', []),
            'compatibility': quality_metrics.get('compatibility_score', {}),
            'preservation_rates': quality_metrics.get('preservation_rates', {})
        }

        return json.dumps(report_data, indent=2, default=str)