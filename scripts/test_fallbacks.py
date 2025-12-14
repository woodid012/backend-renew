"""
Test script to check fallback execution for portfolio PRIe3oRLfO4uck35xwYFJ - asset 1.
Monitors and reports all fallback mechanisms that are triggered during model execution.
"""

import sys
import os
import io
from contextlib import redirect_stdout, redirect_stderr
import re

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from src.core.database import get_data_from_mongodb, database_lifecycle
from src.core.input_processor import load_price_data
from src.main import run_cashflow_model

class FallbackMonitor:
    """Captures and categorizes fallback messages from stdout/stderr"""
    
    def __init__(self):
        self.fallbacks = []
        self.output_lines = []
        
    def capture_output(self, text):
        """Capture output and identify fallback messages"""
        lines = text.split('\n')
        for line in lines:
            self.output_lines.append(line)
            
            # Check for explicit fallback indicators
            line_lower = line.lower()
            
            # Look for explicit fallback messages
            fallback_keywords = [
                'fallback',
                'falling back',
                'using fallback',
                'found fallback',
                'no defaults found in mongodb, falling back',
                'mongodb load failed',
                'fallback to',
                'fallback:',
                '⚠️'  # Warning emoji often indicates fallbacks
            ]
            
            # Check if line contains any fallback indicators
            is_fallback = any(keyword in line_lower for keyword in fallback_keywords)
            
            # Also check for specific fallback patterns
            if is_fallback or self._is_explicit_fallback(line):
                self.fallbacks.append({
                    'line': line.strip(),
                    'type': self._categorize_fallback(line)
                })
    
    def _is_explicit_fallback(self, line):
        """Check for explicit fallback patterns"""
        line_lower = line.lower()
        
        # Specific fallback patterns from the codebase
        patterns = [
            'falling back to json',
            'falling back to file',
            'fallback to json file',
            'fallback to file',
            'fallback to portfolio',
            'fallback to old fields',
            'fallback: use',
            'operatingstartdate (fallback)',
            'earliest asset operatingstartdate (fallback)',
            'no capacity factors for',
            'unknown asset type',
            'found fallback price from',
            'default fallback',
            'fallback defaults',
            'fallback price',
            'deleting.*records for portfolios (fallback)',
            'deleting.*records for unique_ids',
        ]
        
        return any(pattern in line_lower for pattern in patterns)
    
    def _categorize_fallback(self, line):
        """Categorize the type of fallback based on message content"""
        line_lower = line.lower()
        
        if 'mongodb' in line_lower and 'fallback' in line_lower:
            if 'assetdefaults' in line_lower or 'asset_defaults' in line_lower:
                return 'Asset Defaults: MongoDB → JSON'
            elif 'sensitivity' in line_lower:
                return 'Sensitivity Config: MongoDB → JSON'
        
        if 'json' in line_lower and 'fallback' in line_lower:
            return 'Asset Defaults: JSON → Hardcoded'
        
        if 'fallback price' in line_lower or 'found fallback price' in line_lower:
            return 'Price Curves: Backward Search'
        
        if 'no capacity factors' in line_lower and 'fallback' in line_lower:
            return 'Capacity Factors: Region Fallback'
        
        if 'unknown asset type' in line_lower and 'fallback' in line_lower:
            return 'Asset Defaults: Unknown Type'
        
        if 'operatingstartdate (fallback)' in line_lower:
            return 'Model Dates: OperatingStartDate Fallback'
        
        if 'portfolio' in line_lower and 'fallback' in line_lower and 'unique_id' in line_lower:
            return 'Database: Portfolio Field Fallback'
        
        if 'fallback' in line_lower:
            return 'General Fallback'
        
        if '⚠️' in line:
            return 'Warning (Potential Fallback)'
        
        return 'Unknown Fallback'
    
    def get_summary(self):
        """Generate summary report of all fallbacks"""
        summary = {
            'total_fallbacks': len(self.fallbacks),
            'by_category': {},
            'all_fallbacks': self.fallbacks
        }
        
        for fb in self.fallbacks:
            category = fb['type']
            if category not in summary['by_category']:
                summary['by_category'][category] = []
            summary['by_category'][category].append(fb['line'])
        
        return summary


def test_fallbacks():
    """Test fallback execution for portfolio PRIe3oRLfO4uck35xwYFJ - asset 1"""
    
    portfolio_unique_id = "PRIe3oRLfO4uck35xwYFJ"
    asset_id = 1
    
    print("="*80)
    print("FALLBACK TESTING SCRIPT")
    print("="*80)
    print(f"Portfolio unique_id: {portfolio_unique_id}")
    print(f"Asset ID: {asset_id}")
    print("="*80)
    print()
    
    monitor = FallbackMonitor()
    
    # Capture stdout and stderr
    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()
    
    try:
        with database_lifecycle():
            # Load config from MongoDB
            print("Loading asset data from MongoDB...")
            config_data = get_data_from_mongodb('CONFIG_Inputs', {'unique_id': portfolio_unique_id})
            
            if not config_data:
                print(f"ERROR: No config found for unique_id: {portfolio_unique_id}")
                return
            
            # Get the most recent config
            config = config_data[-1]
            assets = config.get('asset_inputs', [])
            
            # Find Asset 1
            asset = None
            for a in assets:
                if a.get('id') == asset_id:
                    asset = a
                    break
            
            if not asset:
                print(f"ERROR: Asset {asset_id} not found in portfolio")
                print(f"Available asset IDs: {[a.get('id') for a in assets]}")
                return
            
            print(f"[OK] Found asset: {asset.get('name', f'Asset_{asset_id}')}")
            print(f"   Asset Type: {asset.get('type', 'unknown')}")
            print(f"   Region: {asset.get('region', 'unknown')}")
            print()
            
            # Load price data
            current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            monthly_price_path = os.path.join(current_dir, 'data', 'raw_inputs', 'merchant_price_monthly.csv')
            yearly_spread_path = os.path.join(current_dir, 'data', 'raw_inputs', 'merchant_yearly_spreads.csv')
            
            print("Loading price data...")
            monthly_prices, yearly_spreads = load_price_data(monthly_price_path, yearly_spread_path)
            print(f"[OK] Loaded {len(monthly_prices)} monthly price records")
            print(f"[OK] Loaded {len(yearly_spreads)} yearly spread records")
            print()
            
            # Get portfolio name
            portfolio_name = config.get('PlatformName', portfolio_unique_id)
            
            print("="*80)
            print("RUNNING CASHFLOW MODEL")
            print("="*80)
            print("Monitoring for fallback execution...")
            print()
            
            # Run the model with output capture
            # Note: run_cashflow_model expects database connection to be active
            with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
                try:
                    result = run_cashflow_model(
                        assets=[asset],
                        monthly_prices=monthly_prices,
                        yearly_spreads=yearly_spreads,
                        portfolio_name=portfolio_name,
                        portfolio_unique_id=portfolio_unique_id,
                        replace_data=False  # Don't replace existing data
                    )
                    # This print won't be captured, but that's OK
                    pass
                except Exception as e:
                    # Error will be in stderr_capture
                    import traceback
                    traceback.print_exc()
            
            # Capture the output
            stdout_text = stdout_capture.getvalue()
            stderr_text = stderr_capture.getvalue()
            
            monitor.capture_output(stdout_text)
            monitor.capture_output(stderr_text)
            
            print("[OK] Model execution completed")
            
    except Exception as e:
        print(f"[ERROR] Error during test execution: {e}")
        import traceback
        traceback.print_exc()
    
    # Generate and display summary
    print()
    print("="*80)
    print("FALLBACK EXECUTION SUMMARY")
    print("="*80)
    
    summary = monitor.get_summary()
    
    print(f"\nTotal fallback triggers detected: {summary['total_fallbacks']}")
    print()
    
    # Filter out non-fallback warnings
    actual_fallbacks = [fb for fb in summary['all_fallbacks'] 
                       if 'fallback' in fb['line'].lower() or 
                          'falling back' in fb['line'].lower() or
                          fb['type'] != 'Warning (Potential Fallback)']
    
    if len(actual_fallbacks) == 0:
        print("[OK] No fallback mechanisms were executed during model run.")
        print()
        print("This means:")
        print("  - Asset defaults loaded from MongoDB (primary source)")
        print("  - Price data found with exact matches (no backward search needed)")
        print("  - All required data available from primary sources")
        print()
        if summary['total_fallbacks'] > 0:
            print(f"Note: {summary['total_fallbacks']} warning(s) detected, but these are")
            print("      informational warnings, not fallback mechanisms.")
    else:
        print(f"[WARNING] {len(actual_fallbacks)} fallback mechanism(s) detected:")
        print()
        
        # Group actual fallbacks by category
        actual_by_category = {}
        for fb in actual_fallbacks:
            category = fb['type']
            if category not in actual_by_category:
                actual_by_category[category] = []
            actual_by_category[category].append(fb['line'])
        
        for category, messages in actual_by_category.items():
            print(f"  {category}:")
            for msg in messages:
                # Clean up the message for display and handle Unicode
                try:
                    clean_msg = msg.replace('⚠️', '[WARNING]').strip()
                    if len(clean_msg) > 100:
                        clean_msg = clean_msg[:97] + "..."
                    # Try to encode safely
                    safe_msg = clean_msg.encode('ascii', 'replace').decode('ascii')
                    print(f"    - {safe_msg}")
                except:
                    print(f"    - {repr(msg)}")
            print()
        
        # Also show other warnings if any
        other_warnings = [fb for fb in summary['all_fallbacks'] if fb not in actual_fallbacks]
        if other_warnings:
            print("\nOther warnings (not fallbacks):")
            for fb in other_warnings:
                try:
                    clean_msg = fb['line'].replace('⚠️', '[WARNING]').strip()
                    safe_msg = clean_msg.encode('ascii', 'replace').decode('ascii')
                    print(f"  - {safe_msg}")
                except:
                    print(f"  - {repr(fb['line'])}")
            print()
        
        print("\nDetailed fallback messages:")
        print("-" * 80)
        for i, fb in enumerate(summary['all_fallbacks'], 1):
            print(f"\n{i}. [{fb['type']}]")
            # Safely encode the line to avoid Unicode errors
            try:
                safe_line = fb['line'].encode('ascii', 'replace').decode('ascii')
                print(f"   {safe_line}")
            except:
                print(f"   {repr(fb['line'])}")
    
    print()
    print("="*80)
    print("TEST COMPLETE")
    print("="*80)
    
    # Also write full output to a file for detailed analysis
    output_file = os.path.join(project_root, 'fallback_test_output.txt')
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("FALLBACK TEST OUTPUT\n")
        f.write("="*80 + "\n\n")
        f.write(f"Portfolio: {portfolio_unique_id}\n")
        f.write(f"Asset ID: {asset_id}\n\n")
        f.write("FULL OUTPUT:\n")
        f.write("-"*80 + "\n")
        f.write(stdout_text)
        f.write("\n\nSTDERR:\n")
        f.write("-"*80 + "\n")
        f.write(stderr_text)
        f.write("\n\nFALLBACK SUMMARY:\n")
        f.write("-"*80 + "\n")
        f.write(f"Total fallbacks: {summary['total_fallbacks']}\n\n")
        for category, messages in summary['by_category'].items():
            f.write(f"{category}:\n")
            for msg in messages:
                f.write(f"  {msg}\n")
            f.write("\n")
    
    print(f"\n[INFO] Full output saved to: {output_file}")


if __name__ == "__main__":
    test_fallbacks()

