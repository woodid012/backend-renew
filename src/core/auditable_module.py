# backend-renew/src/core/auditable_module.py
"""
AuditableModule: A wrapper to add Glassbox-style transparency
to existing calculation functions WITHOUT changing the logic.

Usage:
    1. Create a subclass that wraps your existing function
    2. Register inputs before calling the function
    3. Set outputs after the function returns
    4. Use AuditTrail to collect all module executions
"""

import pandas as pd
import numpy as np
from datetime import datetime, date
from typing import Any, Dict, List, Optional
from dateutil.relativedelta import relativedelta


class Timeline:
    """
    Shared time index for all modules - matches Glassbox Timeline pattern.
    Provides consistent date ranges and mask generation.
    """
    
    def __init__(self, start_date: date, end_date: date, frequency: str = 'MS'):
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.frequency = frequency
        
        # Generate the main time index
        self.dt_index = pd.date_range(start=self.start_date, end=self.end_date, freq=self.frequency)
        self.periods = len(self.dt_index)
    
    def get_zeros_series(self, name: str = None) -> pd.Series:
        """Returns a Series of zeros indexed by this timeline."""
        return pd.Series(0.0, index=self.dt_index, name=name)
    
    def get_mask(self, start_date: date, end_date: date) -> pd.Series:
        """Returns a boolean mask (1/0) for the range [start, end]."""
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        mask = (self.dt_index >= start) & (self.dt_index <= end)
        return mask.astype(float)
    
    def to_dict(self) -> Dict[str, Any]:
        """Return timeline metadata for audit."""
        return {
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "frequency": self.frequency,
            "periods": self.periods
        }


class AuditableModule:
    """
    Base class for wrapping calculation functions with audit logging.
    Captures all inputs and outputs for transparency and debugging.
    """
    
    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description
        self.inputs: Dict[str, Any] = {}
        self.outputs: Dict[str, Any] = {}
        self.metadata: Dict[str, Any] = {
            "created_at": datetime.now().isoformat(),
            "module_version": "1.0"
        }
    
    def register_input(self, key: str, value: Any, description: str = ""):
        """Register an input value with optional description."""
        self.inputs[key] = {
            "value": self._serialize_value(value),
            "type": type(value).__name__,
            "description": description
        }
    
    def register_inputs(self, inputs_dict: Dict[str, Any]):
        """Register multiple inputs at once."""
        for key, value in inputs_dict.items():
            self.register_input(key, value)
    
    def set_output(self, key: str, value: Any, description: str = ""):
        """Set an output value with optional description."""
        self.outputs[key] = {
            "value": self._serialize_value(value),
            "type": type(value).__name__,
            "description": description
        }
    
    def set_outputs(self, outputs_dict: Dict[str, Any]):
        """Set multiple outputs at once."""
        for key, value in outputs_dict.items():
            self.set_output(key, value)
    
    def _serialize_value(self, value: Any) -> Any:
        """Convert complex types to JSON-serializable format."""
        if isinstance(value, pd.DataFrame):
            return {"__type": "DataFrame", "shape": list(value.shape), "columns": list(value.columns)}
        elif isinstance(value, pd.Series):
            return {"__type": "Series", "length": len(value), "sum": float(value.sum()) if pd.api.types.is_numeric_dtype(value) else None}
        elif isinstance(value, np.ndarray):
            return {"__type": "ndarray", "shape": list(value.shape), "sum": float(value.sum()) if np.issubdtype(value.dtype, np.number) else None}
        elif isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, (list, dict)):
            # For large lists/dicts, just store summary
            if isinstance(value, list) and len(value) > 10:
                return {"__type": "list", "length": len(value), "sample": value[:3]}
            return value
        elif hasattr(value, '__dict__'):
            return {"__type": type(value).__name__, "id": getattr(value, 'id', None)}
        else:
            return value
    
    def to_audit_dict(self) -> Dict[str, Any]:
        """Return a dictionary suitable for audit logging."""
        return {
            "name": self.name,
            "description": self.description,
            "inputs": self.inputs,
            "outputs": self.outputs,
            "metadata": self.metadata
        }
    
    def summary(self) -> str:
        """Return a human-readable summary."""
        input_count = len(self.inputs)
        output_count = len(self.outputs)
        return f"[{self.name}] {input_count} inputs → {output_count} outputs"


class AuditTrail:
    """
    Collects audit data from multiple AuditableModule executions.
    Can be saved to database or returned with API response.
    """
    
    def __init__(self, run_id: Optional[str] = None):
        self.run_id = run_id or datetime.now().strftime("%Y%m%d_%H%M%S")
        self.started_at = datetime.now().isoformat()
        self.modules: List[Dict[str, Any]] = []
        self.metadata: Dict[str, Any] = {}
    
    def register(self, module: AuditableModule):
        """Add a module's audit data to the trail."""
        self.modules.append(module.to_audit_dict())
    
    def set_metadata(self, key: str, value: Any):
        """Add metadata to the audit trail (e.g., user_id, portfolio_id)."""
        self.metadata[key] = value
    
    def to_dict(self) -> Dict[str, Any]:
        """Return the full audit trail as a dictionary."""
        return {
            "run_id": self.run_id,
            "started_at": self.started_at,
            "completed_at": datetime.now().isoformat(),
            "metadata": self.metadata,
            "modules": self.modules,
            "module_count": len(self.modules)
        }
    
    def summary(self) -> str:
        """Return a human-readable summary of the audit trail."""
        module_names = [m["name"] for m in self.modules]
        return f"AuditTrail[{self.run_id}]: {len(self.modules)} modules - {', '.join(module_names)}"
