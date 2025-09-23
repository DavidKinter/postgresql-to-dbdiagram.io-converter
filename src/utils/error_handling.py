"""
Error handling utilities for the pg2dbml converter.
"""

import sys
import traceback
from typing import Optional, Dict, Any, List
from enum import Enum

class ConversionError(Exception):
    """Base exception for conversion errors."""

    def __init__(self, message: str, error_code: str = None, context: Dict[str, Any] = None):
        super().__init__(message)
        self.message = message
        self.error_code = error_code or 'CONVERSION_ERROR'
        self.context = context or {}

class PreprocessingError(ConversionError):
    """Error during SQL preprocessing."""
    pass

class ParsingError(ConversionError):
    """Error during SQL parsing."""
    pass

class TransformationError(ConversionError):
    """Error during type/constraint transformation."""
    pass

class GenerationError(ConversionError):
    """Error during DBML generation."""
    pass

class QualityError(ConversionError):
    """Error in quality validation."""
    pass

class ErrorSeverity(Enum):
    """Error severity levels."""
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"

class ErrorHandler:
    """Central error handling and reporting."""

    def __init__(self, logger=None):
        self.logger = logger
        self.errors = []
        self.warnings = []

    def handle_error(self, error: Exception, severity: ErrorSeverity = ErrorSeverity.HIGH,
                    context: Dict[str, Any] = None, recoverable: bool = True):
        """
        Handle an error with appropriate logging and recovery.

        Args:
            error: The exception that occurred
            severity: Error severity level
            context: Additional context information
            recoverable: Whether the error is recoverable
        """

        error_record = {
            'type': type(error).__name__,
            'message': str(error),
            'severity': severity.value,
            'context': context or {},
            'recoverable': recoverable,
            'traceback': traceback.format_exc() if severity == ErrorSeverity.CRITICAL else None
        }

        if isinstance(error, ConversionError):
            error_record['error_code'] = error.error_code
            error_record['context'].update(error.context)

        if severity in [ErrorSeverity.CRITICAL, ErrorSeverity.HIGH]:
            self.errors.append(error_record)
        else:
            self.warnings.append(error_record)

        # Log the error
        if self.logger:
            if severity == ErrorSeverity.CRITICAL:
                self.logger.critical(f"CRITICAL ERROR: {error}")
                if error_record['traceback']:
                    self.logger.critical(f"Traceback: {error_record['traceback']}")
            elif severity == ErrorSeverity.HIGH:
                self.logger.error(f"ERROR: {error}")
            elif severity == ErrorSeverity.MEDIUM:
                self.logger.warning(f"WARNING: {error}")
            else:
                self.logger.info(f"INFO: {error}")

        # Handle non-recoverable errors
        if not recoverable and severity == ErrorSeverity.CRITICAL:
            self._handle_fatal_error(error_record)

    def _handle_fatal_error(self, error_record: Dict[str, Any]):
        """Handle fatal errors that require immediate termination."""

        print(f"\n{'='*60}")
        print("FATAL ERROR - CONVERSION TERMINATED")
        print(f"{'='*60}")
        print(f"Error: {error_record['message']}")
        print(f"Type: {error_record['type']}")

        if error_record.get('context'):
            print("\nContext:")
            for key, value in error_record['context'].items():
                print(f"  {key}: {value}")

        print(f"\n{'='*60}")
        print("Please check your input file and try again.")
        print("For support, include this error information in your report.")
        print(f"{'='*60}")

        sys.exit(1)

    def add_warning(self, message: str, context: Dict[str, Any] = None):
        """Add a warning without raising an exception."""

        warning_record = {
            'type': 'Warning',
            'message': message,
            'severity': ErrorSeverity.MEDIUM.value,
            'context': context or {},
            'recoverable': True
        }

        self.warnings.append(warning_record)

        if self.logger:
            self.logger.warning(f"WARNING: {message}")

    def get_error_summary(self) -> Dict[str, Any]:
        """Get summary of all errors and warnings."""

        # Group errors by severity
        by_severity = {}
        all_issues = self.errors + self.warnings

        for issue in all_issues:
            severity = issue['severity']
            if severity not in by_severity:
                by_severity[severity] = []
            by_severity[severity].append(issue)

        # Group errors by type
        by_type = {}
        for issue in all_issues:
            issue_type = issue['type']
            if issue_type not in by_type:
                by_type[issue_type] = []
            by_type[issue_type].append(issue)

        return {
            'total_errors': len(self.errors),
            'total_warnings': len(self.warnings),
            'by_severity': by_severity,
            'by_type': by_type,
            'has_critical_errors': any(e['severity'] == 'CRITICAL' for e in self.errors),
            'has_recoverable_errors': any(e['recoverable'] for e in self.errors),
            'all_errors': self.errors,
            'all_warnings': self.warnings
        }

    def should_continue_conversion(self) -> bool:
        """Determine if conversion should continue based on error severity."""

        critical_errors = [e for e in self.errors if e['severity'] == 'CRITICAL']
        non_recoverable_errors = [e for e in self.errors if not e['recoverable']]

        return len(critical_errors) == 0 and len(non_recoverable_errors) == 0

    def generate_error_report(self) -> str:
        """Generate human-readable error report."""

        if not self.errors and not self.warnings:
            return "✅ No errors or warnings detected."

        lines = []
        lines.append("Error Summary Report")
        lines.append("=" * 50)

        if self.errors:
            lines.append(f"\n❌ Errors ({len(self.errors)}):")
            for i, error in enumerate(self.errors, 1):
                lines.append(f"{i}. [{error['severity']}] {error['message']}")
                if error.get('context'):
                    for key, value in error['context'].items():
                        lines.append(f"   {key}: {value}")

        if self.warnings:
            lines.append(f"\n⚠️  Warnings ({len(self.warnings)}):")
            for i, warning in enumerate(self.warnings, 1):
                lines.append(f"{i}. {warning['message']}")

        # Recovery recommendations
        lines.append("\n" + "=" * 50)
        if self.should_continue_conversion():
            lines.append("✅ Conversion can continue with documented limitations.")
        else:
            lines.append("❌ Critical errors detected - conversion should be aborted.")

        return "\n".join(lines)


class ContextManager:
    """Context manager for error handling with automatic context tracking."""

    def __init__(self, error_handler: ErrorHandler, operation: str, context: Dict[str, Any] = None):
        self.error_handler = error_handler
        self.operation = operation
        self.context = context or {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            # Determine severity based on exception type
            if isinstance(exc_val, (PreprocessingError, ParsingError)):
                severity = ErrorSeverity.HIGH
            elif isinstance(exc_val, (TransformationError, GenerationError)):
                severity = ErrorSeverity.MEDIUM
            else:
                severity = ErrorSeverity.CRITICAL

            # Add operation context
            context = self.context.copy()
            context['operation'] = self.operation

            self.error_handler.handle_error(
                exc_val,
                severity=severity,
                context=context,
                recoverable=severity != ErrorSeverity.CRITICAL
            )

            # Return True to suppress the exception if it's recoverable
            return severity != ErrorSeverity.CRITICAL

        return False


def validate_input_file(file_path: str) -> None:
    """
    Validate input file exists and is readable.

    Raises:
        ConversionError: If file validation fails
    """

    import os

    if not os.path.exists(file_path):
        raise ConversionError(
            f"Input file does not exist: {file_path}",
            error_code="FILE_NOT_FOUND",
            context={'file_path': file_path}
        )

    if not os.path.isfile(file_path):
        raise ConversionError(
            f"Path is not a file: {file_path}",
            error_code="NOT_A_FILE",
            context={'file_path': file_path}
        )

    if not os.access(file_path, os.R_OK):
        raise ConversionError(
            f"File is not readable: {file_path}",
            error_code="FILE_NOT_READABLE",
            context={'file_path': file_path}
        )

    # Check file size (warn if very large)
    file_size = os.path.getsize(file_path)
    if file_size > 50 * 1024 * 1024:  # 50MB
        raise ConversionError(
            f"File is very large ({file_size / 1024 / 1024:.1f}MB) and may cause performance issues",
            error_code="LARGE_FILE_WARNING",
            context={'file_path': file_path, 'file_size_mb': file_size / 1024 / 1024}
        )


def validate_output_path(output_path: str) -> None:
    """
    Validate output path is writable.

    Raises:
        ConversionError: If output validation fails
    """

    import os
    from pathlib import Path

    output_dir = Path(output_path).parent

    if not output_dir.exists():
        try:
            output_dir.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            raise ConversionError(
                f"Cannot create output directory: {output_dir}",
                error_code="OUTPUT_DIR_NOT_CREATABLE",
                context={'output_dir': str(output_dir)}
            )

    if not os.access(output_dir, os.W_OK):
        raise ConversionError(
            f"Output directory is not writable: {output_dir}",
            error_code="OUTPUT_DIR_NOT_WRITABLE",
            context={'output_dir': str(output_dir)}
        )


def safe_execute(func, *args, error_handler: ErrorHandler = None,
                operation: str = None, **kwargs):
    """
    Safely execute a function with error handling.

    Args:
        func: Function to execute
        *args: Function arguments
        error_handler: Error handler instance
        operation: Operation description for context
        **kwargs: Function keyword arguments

    Returns:
        Function result or None if error occurred
    """

    try:
        return func(*args, **kwargs)
    except Exception as e:
        if error_handler:
            context = {'operation': operation} if operation else {}
            error_handler.handle_error(e, context=context)
        else:
            # Re-raise if no error handler
            raise
        return None