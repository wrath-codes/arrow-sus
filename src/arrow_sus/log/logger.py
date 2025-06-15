from __future__ import annotations

import atexit
import datetime as dt
import json
import logging
import logging.config
import os
import pathlib
from typing import Dict, Optional, override

import yaml
from orjson import (
    OPT_APPEND_NEWLINE,
    OPT_NON_STR_KEYS,
    OPT_SERIALIZE_DATACLASS,
    OPT_SERIALIZE_NUMPY,
    OPT_UTC_Z,
    dumps,
)
from yaml import safe_load


def add_custom_level(level_name: str, level_num: int, method_name: str = None):
    """
    Add a new logging level to the `logging` module and the currently configured logger class.

    Args:
        level_name: The name of the new level (e.g., 'TRACE')
        level_num: The numeric value for the level (e.g., 5)
        method_name: The method name to add to Logger (defaults to level_name.lower())
    """
    if method_name is None:
        method_name = level_name.lower()

    # Add the level to the logging module
    logging.addLevelName(level_num, level_name)

    # Add method to Logger class
    def log_for_level(self, message, *args, **kwargs):
        if self.isEnabledFor(level_num):
            self._log(level_num, message, args, **kwargs)

    def log_to_root(message, *args, **kwargs):
        logging.log(level_num, message, *args, **kwargs)

    setattr(logging.Logger, method_name, log_for_level)
    setattr(logging, method_name, log_to_root)


# Add the three custom levels
add_custom_level("TRACE", 5)  # Below DEBUG (10) - for very detailed tracing
add_custom_level(
    "SUCCESS", 22
)  # Between INFO (20) and WARNING (30) - for success messages
add_custom_level("NOTICE", 25)  # Between INFO (20) and WARNING (30) - for notices

# Constants
LOG_RECORD_BUILTIN_ATTRS = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
    "taskName",
}

# Catppuccin Mocha palette colors
CATPPUCCIN = {
    "ROSEWATER": "\033[38;5;217m",  # Light pink
    "FLAMINGO": "\033[38;5;216m",  # Light coral
    "PINK": "\033[38;5;212m",  # Soft pink
    "MAUVE": "\033[38;5;183m",  # Light purple
    "RED": "\033[38;5;203m",  # Soft red
    "MAROON": "\033[38;5;210m",  # Pinkish red
    "PEACH": "\033[38;5;215m",  # Light orange
    "YELLOW": "\033[38;5;222m",  # Soft yellow
    "GREEN": "\033[38;5;151m",  # Pastel green
    "TEAL": "\033[38;5;152m",  # Soft teal
    "SKY": "\033[38;5;153m",  # Soft sky blue
    "BLUE": "\033[38;5;111m",  # Soft blue
    "LAVENDER": "\033[38;5;147m",  # Light lavender
    "TEXT": "\033[38;5;188m",  # Light text color
    "RESET": "\033[0m",  # Reset all colors
}


class JSONFormatter(logging.Formatter):
    """JSON formatter that properly handles Unicode characters."""

    def __init__(self, *, fmt_keys: Dict[str, str] = None):
        super().__init__()
        self.fmt_keys = fmt_keys if fmt_keys is not None else {}

    @override
    def format(self, record: logging.LogRecord) -> str:
        message = self._prepare_log_dict(record)
        return dumps(
            message,
            option=OPT_SERIALIZE_DATACLASS
            | OPT_SERIALIZE_NUMPY
            | OPT_NON_STR_KEYS
            | OPT_UTC_Z,
            default=str,
        ).decode("utf-8")

    def _prepare_log_dict(self, record: logging.LogRecord):
        always_fields = {
            "message": record.getMessage(),
            "timestamp": dt.datetime.fromtimestamp(
                record.created, tz=dt.timezone.utc
            ).isoformat(),
        }
        if record.exc_info is not None:
            always_fields["exc_info"] = self.formatException(record.exc_info)

        if record.stack_info is not None:
            always_fields["stack_info"] = self.formatStack(record.stack_info)

        message = {
            key: msg_val
            if (msg_val := always_fields.pop(val, None)) is not None
            else getattr(record, val)
            for key, val in self.fmt_keys.items()
        }
        message.update(always_fields)

        for key, val in record.__dict__.items():
            if key not in LOG_RECORD_BUILTIN_ATTRS:
                message[key] = val

        return message


class SimpleAlignedFormatter(logging.Formatter):
    """A formatter that provides aligned message fields without colors."""

    # Fixed column widths for alignment (same as CatppuccinFormatter)
    TIME_WIDTH = 19
    LEVEL_WIDTH = 8
    MODULE_WIDTH = 24
    FUNC_WIDTH = 32
    LINE_WIDTH = 5
    MODULE_INFO_TOTAL_WIDTH = 70

    # Level-specific icons (same as CatppuccinFormatter)
    LEVEL_ICONS = {
        5: "",  # TRACE
        logging.DEBUG: "",  # DEBUG
        logging.INFO: "󰋽",  # INFO
        22: "",  # SUCCESS
        25: "󰯪",  # NOTICE
        logging.WARNING: "",  # WARNING
        logging.ERROR: "",  # ERROR
        logging.CRITICAL: "󰚤",  # CRITICAL
    }

    @override
    def format(self, record: logging.LogRecord) -> str:
        def _format_level_padded(level_name: str) -> str:
            icon = self.LEVEL_ICONS.get(record.levelno, "📝")  # Default icon
            if len(level_name) <= self.LEVEL_WIDTH:
                return f"[{icon} {level_name:<{self.LEVEL_WIDTH}}]"
            else:
                return f"[{icon} {level_name[: self.LEVEL_WIDTH]}]"

        def _format_extras(record) -> str:
            """Format extra fields without colors."""
            extras = []
            for key, val in record.__dict__.items():
                if key not in LOG_RECORD_BUILTIN_ATTRS:
                    extras.append(f"{key}={val}")
            if extras:
                return f" | {' '.join(extras)}"
            return ""

        # Format the timestamp with the formatter's configuration
        timestamp = self.formatTime(record, self.datefmt)

        # Create the module info section with calculated spacing
        module_name = (
            record.module[: self.MODULE_WIDTH]
            if len(record.module) > self.MODULE_WIDTH
            else record.module
        )
        func_name = (
            record.funcName[: self.FUNC_WIDTH - 3] + "..."
            if len(record.funcName) > self.FUNC_WIDTH
            else record.funcName
        )
        line_str = (
            str(record.lineno)[: self.LINE_WIDTH]
            if len(str(record.lineno)) > self.LINE_WIDTH
            else str(record.lineno)
        )

        # Calculate spacing between function name and line number
        func_section_width = self.FUNC_WIDTH + 6  # 6 = len(") - L[") + len("]")
        func_and_brackets = f"({func_name}) - L[{line_str:>{self.LINE_WIDTH}}]"
        current_width = len(func_and_brackets)

        if current_width < func_section_width:
            spaces_needed = func_section_width - current_width
            spacing = " " * spaces_needed
            func_and_brackets = (
                f"({func_name}){spacing} - L[{line_str:>{self.LINE_WIDTH}}]"
            )

        # Build module info with calculated spacing
        module_info_content = f"{module_name:<{self.MODULE_WIDTH}} {func_and_brackets}"

        # Create components without colors
        formatted_time = f"[{timestamp:<{self.TIME_WIDTH}}]"
        formatted_level = _format_level_padded(record.levelname)
        extras = _format_extras(record)

        # Get the formatted message safely
        try:
            message = record.getMessage()
        except (TypeError, ValueError):
            message = getattr(record, "msg", "Unknown message")

        # Combine all parts
        log_line = f"{formatted_level}{formatted_time}: {module_info_content} | {message}{extras}"

        # Add exception info if present
        if record.exc_info:
            exc_text = self.formatException(record.exc_info)
            log_line = f"{log_line}\n{exc_text}"

        return log_line


class CatppuccinFormatter(logging.Formatter):
    """A formatter that adds Catppuccin-themed colors with aligned message fields."""

    # Fixed column widths for alignment
    TIME_WIDTH = 19  # Width for timestamp field
    LEVEL_WIDTH = 8  # Width for level name field
    MODULE_WIDTH = 24  # Width for module name field
    FUNC_WIDTH = 32  # Width for function name field
    LINE_WIDTH = 5  # Width for line number field
    LEVEL_WIDTH = 8  # Width for level name field
    MODULE_INFO_TOTAL_WIDTH = 70  # Total width for module info section

    # Level-specific colors
    LEVEL_COLORS = {
        5: CATPPUCCIN["LAVENDER"],
        logging.DEBUG: CATPPUCCIN["SKY"],
        22: CATPPUCCIN["GREEN"],
        25: CATPPUCCIN["TEAL"],
        logging.INFO: CATPPUCCIN["BLUE"],
        logging.WARNING: CATPPUCCIN["PEACH"],
        logging.ERROR: CATPPUCCIN["MAROON"],
        logging.CRITICAL: CATPPUCCIN["RED"],
    }

    # Level-specific icons (same as CatppuccinFormatter)
    LEVEL_ICONS = {
        5: "",  # TRACE
        logging.DEBUG: "",  # DEBUG
        logging.INFO: "󰋽",  # INFO
        22: "",  # SUCCESS
        25: "󰯪",  # NOTICE
        logging.WARNING: "",  # WARNING
        logging.ERROR: "",  # ERROR
        logging.CRITICAL: "󰚤",  # CRITICAL
    }

    TIME_COLOR = CATPPUCCIN["TEXT"]

    @override
    def format(self, record: logging.LogRecord) -> str:
        def _format_level_padded(level_name: str) -> str:
            icon = self.LEVEL_ICONS.get(record.levelno, "📝")  # Default icon
            if len(level_name) <= self.LEVEL_WIDTH:
                return f"{level_color}[{icon} {level_name:<{self.LEVEL_WIDTH}}]{CATPPUCCIN['RESET']}"
            else:
                return f"{level_color}[{icon} {level_name[: self.LEVEL_WIDTH]}]{CATPPUCCIN['RESET']}"

        def _format_extras(record) -> str:
            """Format extra fields with field names in level color and values in message color."""
            extras = []
            for key, val in record.__dict__.items():
                if key not in LOG_RECORD_BUILTIN_ATTRS:
                    extras.append(
                        f"{level_color}{key}{CATPPUCCIN['RESET']}={CATPPUCCIN['TEXT']}{val}{CATPPUCCIN['RESET']}"
                    )
            if extras:
                return f" | {' '.join(extras)}"
            return ""

        # Format the timestamp with the formatter's configuration
        timestamp = self.formatTime(record, self.datefmt)

        # Get the appropriate color for the log level
        level_color = self.LEVEL_COLORS.get(record.levelno, CATPPUCCIN["TEXT"])

        # Create the module info section with calculated spacing
        module_name = (
            record.module[: self.MODULE_WIDTH]
            if len(record.module) > self.MODULE_WIDTH
            else record.module
        )
        func_name = (
            record.funcName[: self.FUNC_WIDTH - 3] + "..."
            if len(record.funcName) > self.FUNC_WIDTH
            else record.funcName
        )
        line_str = (
            str(record.lineno)[: self.LINE_WIDTH]
            if len(str(record.lineno)) > self.LINE_WIDTH
            else str(record.lineno)
        )

        # Calculate spacing between function name and line number
        # Total space available for func + spacing + " L[" + line + "]"
        func_section_width = self.FUNC_WIDTH + 6  # 6 = len(") - L[") + len("]")
        func_and_brackets = f"({func_name}) L[{line_str:>{self.LINE_WIDTH}}]"
        current_width = len(func_and_brackets)

        if current_width < func_section_width:
            # Insert spaces between ") L[" to reach target width
            spaces_needed = func_section_width - current_width
            # Add characters to the right of the line number
            spacing = "." * spaces_needed
            func_and_brackets = (
                f"({func_name}){spacing} L[{line_str:>{self.LINE_WIDTH}}]"
            )

        # Build module info with calculated spacing
        module_info_content = f"{module_name:<{self.MODULE_WIDTH}} {func_and_brackets}"
        colored_module_info = f"{level_color}{module_info_content}{CATPPUCCIN['RESET']}"

        # Create other colored components
        colored_time = (
            f"{self.TIME_COLOR}[{timestamp:<{self.TIME_WIDTH}}]{CATPPUCCIN['RESET']}"
        )
        colored_level = _format_level_padded(record.levelname)
        extras = _format_extras(record)

        # Get the formatted message safely
        try:
            message = record.getMessage()
            colored_message = f"{CATPPUCCIN['TEXT']}{message}{CATPPUCCIN['RESET']}"
        except (TypeError, ValueError):
            raw_msg = getattr(record, "msg", "Unknown message")
            colored_message = f"{level_color}{raw_msg}{CATPPUCCIN['RESET']}"

        # Combine all parts with fixed-width module info section
        log_line = f"{colored_level}{colored_time}: {colored_module_info} | {colored_message}{extras}"

        # Add exception info if present
        if record.exc_info:
            exc_text = self.formatException(record.exc_info)
            colored_exc_text = f"{level_color}{exc_text}{CATPPUCCIN['RESET']}"
            log_line = f"{log_line}\n{colored_exc_text}"

        return log_line


class VerboseFilter(logging.Filter):
    """Filter that hides messages marked as verbose_only from console output."""

    @override
    def filter(self, record: logging.LogRecord) -> bool:
        return not getattr(record, "verbose_only", False)


class NonErrorFilter(logging.Filter):
    """Filter that only allows messages with level <= INFO."""

    @override
    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno <= logging.INFO


def get_dynamic_log_filename(
    app_name: Optional[str] = None, include_pid: bool = False, prefix: str = "app"
) -> str:
    """Generate a dynamic log filename with various components.

    Args:
        app_name: Optional application name to include in filename
        include_pid: Whether to include process ID in filename
        prefix: Default prefix if no app_name is provided

    Returns:
        A string path to the log file
    """
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    components = []

    # Use provided app_name or default prefix
    components.append(app_name if app_name else prefix)

    # Always include timestamp
    components.append(timestamp)

    # Optionally include process ID
    if include_pid:
        components.append(f"pid{os.getpid()}")

    # Create logs directory if it doesn't exist
    log_dir = pathlib.Path("a_sus_logs")
    log_dir.mkdir(exist_ok=True)

    return f"a_sus_logs/{'_'.join(components)}.log.jsonl"


# Track configured loggers to prevent duplicate configuration
_CONFIGURED_LOGGERS = set()
_QUEUE_LISTENERS = {}


def setup_logging(
    logger_name: str = "root",
    theme: str = "catppuccin",
    config_dir: str = "src/arrow_sus/log/config",
    log_file: Optional[str] = None,
    app_name: Optional[str] = None,
    include_pid: bool = False,
) -> logging.Logger:
    """Set up logging with themed colored output and advanced features.

    Args:
        logger_name: Name of the logger to configure
        theme: The color theme to use ("none", "colored", "catppuccin")
        config_dir: Directory containing the logging config files
        log_file: Custom log file path, if None a dynamic one will be generated
        app_name: Optional application name to include in the log filename
        include_pid: Whether to include process ID in the dynamic log filename

    Returns:
        Configured logger instance
    """
    global _CONFIGURED_LOGGERS, _QUEUE_LISTENERS

    # If this logger has already been configured, just return it
    if logger_name in _CONFIGURED_LOGGERS:
        return logging.getLogger(logger_name)

    # Load the appropriate config file based on theme
    if theme == "catppuccin":
        config_file = pathlib.Path(f"{config_dir}/catppuccin_stderr_json_file.yml")
    else:
        config_file = pathlib.Path(f"{config_dir}/queued_stderr_json_file.yml")

    # Load the config file
    with open(config_file) as f_in:
        config = safe_load(f_in)

    # Generate a dynamic log file name if none provided
    if log_file is None:
        log_file = get_dynamic_log_filename(app_name, include_pid)

    # Update the log file name in the configuration
    for handler in config.get("handlers", {}).values():
        if handler.get("class") == "logging.handlers.RotatingFileHandler":
            handler["filename"] = log_file
            handler["encoding"] = "utf-8"  # Ensure UTF-8 encoding

    # If configuring a non-root logger, update the config
    if logger_name != "root":
        # Create a logger section for this specific logger
        config.setdefault("loggers", {})
        config["loggers"][logger_name] = {
            "level": "DEBUG",
            "handlers": ["queue_handler"],
            "propagate": False,  # Important: prevent propagation to avoid duplicate logs
        }

    # Apply the configuration
    logging.config.dictConfig(config)

    # Start the queue listener if present and not already started
    queue_handler = logging.getHandlerByName("queue_handler")
    if queue_handler is not None and logger_name not in _QUEUE_LISTENERS:
        queue_handler.listener.start()
        _QUEUE_LISTENERS[logger_name] = queue_handler.listener
        atexit.register(queue_handler.listener.stop)

    # Add helper method for verbose-only logging if not already added
    if not hasattr(logging.Logger, "debug_verbose"):

        def debug_verbose(self, message, *args, **kwargs):
            """Log debug message to file only, not to console"""
            kwargs_copy = kwargs.copy()
            kwargs_copy.setdefault("extra", {})["verbose_only"] = True
            self.debug(message, *args, **kwargs_copy)

        # Attach the method to the Logger class
        logging.Logger.debug_verbose = debug_verbose

    # Mark this logger as configured
    _CONFIGURED_LOGGERS.add(logger_name)

    logger = logging.getLogger(logger_name)
    logger.info(f"Logging to file: {log_file}")
    return logger


def get_logger(name: str = "my_app") -> logging.Logger:
    """Get a configured logger by name.

    Args:
        name: Name of the logger to retrieve

    Returns:
        A configured logger instance
    """
    # Check if this logger or the root logger has been configured
    if name not in _CONFIGURED_LOGGERS and "root" not in _CONFIGURED_LOGGERS:
        # Configure this logger
        setup_logging(logger_name=name)

    return logging.getLogger(name)


__all__ = [
    "JSONFormatter",
    "SimpleAlignedFormatter",
    "CatppuccinFormatter",
    "VerboseFilter",
    "NonErrorFilter",
    "get_dynamic_log_filename",
    "setup_logging",
    "get_logger",
]
