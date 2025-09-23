"""
Structured logging configuration for the pg2dbml converter.
"""

import logging
import sys
from typing import Optional
from pathlib import Path

def setup_logger(name: str = "pg2dbml", level: str = "INFO",
                log_file: Optional[str] = None) -> logging.Logger:
    """
    Set up structured logging for the application.

    Args:
        name: Logger name
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional file path for log output

    Returns:
        Configured logger instance
    """

    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))

    # Clear any existing handlers
    logger.handlers = []

    # Create formatter
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, level.upper()))
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler (if specified)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)  # Always debug level for file
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


class ConversionLogger:
    """Specialized logger for conversion operations."""

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.conversion_stats = {
            'warnings': 0,
            'errors': 0,
            'transformations': 0,
            'dropped_items': 0
        }

    def log_preprocessing(self, removed_statements: int, line_count: int):
        """Log preprocessing statistics."""
        self.logger.info(f"Preprocessing complete: {removed_statements} statements removed from {line_count} lines")

    def log_parsing(self, tables: int, relationships: int, constraints: int):
        """Log parsing statistics."""
        self.logger.info(f"Parsing complete: {tables} tables, {relationships} relationships, {constraints} constraints")

    def log_type_transformation(self, table: str, column: str, original_type: str,
                              new_type: str, reason: str):
        """Log type transformation."""
        self.conversion_stats['transformations'] += 1
        self.logger.debug(f"Type transformation: {table}.{column} {original_type} -> {new_type} ({reason})")

    def log_constraint_drop(self, table: str, constraint_name: str,
                           constraint_type: str, reason: str):
        """Log constraint drop."""
        self.conversion_stats['dropped_items'] += 1
        self.logger.warning(f"Dropped {constraint_type} constraint: {table}.{constraint_name} - {reason}")

    def log_feature_drop(self, feature_type: str, location: str, reason: str):
        """Log feature drop."""
        self.conversion_stats['dropped_items'] += 1
        self.logger.warning(f"Dropped {feature_type} at {location} - {reason}")

    def log_silent_failure(self, failure_type: str, description: str, severity: str):
        """Log silent failure detection."""
        if severity == 'CRITICAL':
            self.logger.error(f"SILENT FAILURE DETECTED: {failure_type} - {description}")
        else:
            self.logger.warning(f"Silent failure: {failure_type} - {description}")
        self.conversion_stats['errors'] += 1

    def log_quality_metrics(self, sigma_level: float, dpmo: float, compatibility_score: float):
        """Log quality metrics."""
        self.logger.info(f"Quality metrics: {sigma_level:.1f}σ, DPMO: {dpmo:.1f}, Compatibility: {compatibility_score:.1%}")

    def log_conversion_warning(self, message: str):
        """Log conversion warning."""
        self.conversion_stats['warnings'] += 1
        self.logger.warning(message)

    def log_conversion_error(self, message: str):
        """Log conversion error."""
        self.conversion_stats['errors'] += 1
        self.logger.error(message)

    def get_conversion_summary(self) -> dict:
        """Get conversion statistics summary."""
        return self.conversion_stats.copy()

    def log_summary(self):
        """Log final conversion summary."""
        stats = self.conversion_stats
        self.logger.info(
            f"Conversion summary: {stats['transformations']} transformations, "
            f"{stats['dropped_items']} items dropped, {stats['warnings']} warnings, "
            f"{stats['errors']} errors"
        )