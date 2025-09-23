"""
Process and handle PostgreSQL features that are unsupported in DBML.
"""

from typing import Dict, List, Any

class FeatureProcessor:
    """Process PostgreSQL-specific features for DBML compatibility."""

    def __init__(self):
        self.processed_features = []
        self.dropped_features = []
        self.warnings_generated = []

    def process_features(self, schema: Dict, detected_features: List[Dict]) -> Dict:
        """
        Process detected PostgreSQL features and handle incompatibilities.
        """

        processed_schema = schema.copy()

        for feature in detected_features:
            self._process_single_feature(feature, processed_schema)

        # Add metadata about feature processing
        processed_schema['processed_features'] = self.processed_features
        processed_schema['dropped_features'] = self.dropped_features
        processed_schema['feature_warnings'] = self.warnings_generated

        return processed_schema

    def _process_single_feature(self, feature: Dict, schema: Dict):
        """Process a single detected feature."""

        feature_type = feature['feature_type']
        severity = feature['severity']

        if feature_type == 'ARRAY_TYPE':
            self._process_array_type_feature(feature, schema)
        elif feature_type == 'CHECK_CONSTRAINT':
            self._process_check_constraint_feature(feature, schema)
        elif feature_type == 'CASCADE_ACTION':
            self._process_cascade_action_feature(feature, schema)
        elif feature_type in ['GEOMETRIC_TYPE', 'NETWORK_TYPE', 'RANGE_TYPE']:
            self._process_type_conversion_feature(feature, schema)
        elif feature_type == 'TABLE_INHERITANCE':
            self._process_table_inheritance_feature(feature, schema)
        elif feature_type == 'TABLE_PARTITIONING':
            self._process_table_partitioning_feature(feature, schema)
        else:
            self._process_generic_feature(feature, schema)

    def _process_array_type_feature(self, feature: Dict, schema: Dict):
        """Process array type feature - already handled by TypeMapper."""
        self._log_feature_processed(
            feature,
            'Array type handled by TypeMapper with quoting strategy'
        )

    def _process_check_constraint_feature(self, feature: Dict, schema: Dict):
        """Process CHECK constraint feature - already handled by ConstraintHandler."""
        self._log_feature_processed(
            feature,
            'CHECK constraint handled by ConstraintHandler (dropped with warning)'
        )

    def _process_cascade_action_feature(self, feature: Dict, schema: Dict):
        """Process CASCADE action feature - document the invisibility issue."""
        location = feature.get('location', 'unknown')

        self._log_feature_processed(
            feature,
            f"CASCADE action at {location} preserved but will not be visible in diagram"
        )

        # Add specific warning about CASCADE visibility
        self._generate_warning(
            f"CASCADE action at {location} exists in DBML but is not visualized in diagrams. "
            f"Critical referential integrity behavior is hidden from users."
        )

    def _process_type_conversion_feature(self, feature: Dict, schema: Dict):
        """Process type conversion features - already handled by TypeMapper."""
        feature_type = feature['feature_type']
        location = feature.get('location', 'unknown')
        data_type = feature.get('data_type', 'unknown')

        self._log_feature_processed(
            feature,
            f"{feature_type} '{data_type}' at {location} converted to text by TypeMapper"
        )

    def _process_table_inheritance_feature(self, feature: Dict, schema: Dict):
        """Process table inheritance feature - critical loss."""
        location = feature.get('location', 'unknown')

        self._log_feature_dropped(
            feature,
            f"Table inheritance for '{location}' completely lost - no DBML equivalent"
        )

        # Add critical warning
        self._generate_warning(
            f"Table inheritance for '{location}' cannot be represented in DBML. "
            f"Consider flattening hierarchy or documenting separately."
        )

    def _process_table_partitioning_feature(self, feature: Dict, schema: Dict):
        """Process table partitioning feature - critical loss."""
        location = feature.get('location', 'unknown')

        self._log_feature_dropped(
            feature,
            f"Table partitioning for '{location}' completely lost - no DBML equivalent"
        )

        # Add critical warning
        self._generate_warning(
            f"Table partitioning for '{location}' cannot be represented in DBML. "
            f"Consider representing partitions as separate tables."
        )

    def _process_generic_feature(self, feature: Dict, schema: Dict):
        """Process any other detected feature."""
        feature_type = feature['feature_type']
        severity = feature['severity']
        location = feature.get('location', 'unknown')

        if severity == 'CRITICAL':
            self._log_feature_dropped(
                feature,
                f"{feature_type} at {location} not supported in DBML"
            )
        else:
            self._log_feature_processed(
                feature,
                f"{feature_type} at {location} handled with limitations"
            )

    def _log_feature_processed(self, feature: Dict, processing_description: str):
        """Log that a feature was processed (handled with potential limitations)."""

        processed_record = {
            'feature_type': feature['feature_type'],
            'severity': feature['severity'],
            'location': feature.get('location', 'unknown'),
            'original_description': feature.get('description', ''),
            'processing_description': processing_description,
            'impact': feature.get('impact', ''),
            'workaround': feature.get('workaround', ''),
            'action': 'PROCESSED'
        }

        self.processed_features.append(processed_record)

    def _log_feature_dropped(self, feature: Dict, drop_reason: str):
        """Log that a feature was completely dropped."""

        dropped_record = {
            'feature_type': feature['feature_type'],
            'severity': feature['severity'],
            'location': feature.get('location', 'unknown'),
            'original_description': feature.get('description', ''),
            'drop_reason': drop_reason,
            'impact': feature.get('impact', ''),
            'workaround': feature.get('workaround', ''),
            'action': 'DROPPED'
        }

        self.dropped_features.append(dropped_record)

        # Generate user-facing warning for dropped features
        warning_message = (
            f"Dropped {feature['feature_type']} at {feature.get('location', 'unknown')}: "
            f"{drop_reason}"
        )
        self._generate_warning(warning_message)

    def _generate_warning(self, message: str):
        """Generate a warning message."""
        self.warnings_generated.append({
            'type': 'FEATURE_WARNING',
            'message': message
        })

    def get_feature_processing_report(self) -> Dict[str, Any]:
        """Generate comprehensive feature processing report."""

        # Group processed features by type
        processed_by_type = {}
        for processed in self.processed_features:
            feature_type = processed['feature_type']
            if feature_type not in processed_by_type:
                processed_by_type[feature_type] = []
            processed_by_type[feature_type].append(processed)

        # Group dropped features by type
        dropped_by_type = {}
        for dropped in self.dropped_features:
            feature_type = dropped['feature_type']
            if feature_type not in dropped_by_type:
                dropped_by_type[feature_type] = []
            dropped_by_type[feature_type].append(dropped)

        # Calculate severity distribution
        severity_distribution = {}
        all_features = self.processed_features + self.dropped_features
        for feature in all_features:
            severity = feature['severity']
            if severity not in severity_distribution:
                severity_distribution[severity] = 0
            severity_distribution[severity] += 1

        return {
            'total_processed': len(self.processed_features),
            'total_dropped': len(self.dropped_features),
            'total_warnings': len(self.warnings_generated),
            'processed_by_type': processed_by_type,
            'dropped_by_type': dropped_by_type,
            'severity_distribution': severity_distribution,
            'all_processed': self.processed_features,
            'all_dropped': self.dropped_features,
            'warnings': self.warnings_generated
        }