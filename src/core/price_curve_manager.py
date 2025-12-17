
import pandas as pd
import os
import re
from datetime import datetime
from pymongo import MongoClient


def _normalize_duration_hours(duration_val):
    """
    Normalises a duration cell from the Excel spreads tab into a float (hours).

    Handles both numeric (0.5, 1, 2, 4) and textual labels like
    "Half-Hourly", "1hr", "2hr", "4hr".
    """
    if pd.isna(duration_val):
        raise ValueError("Duration is NaN")

    # First, try direct numeric conversion
    try:
        return float(duration_val)
    except (TypeError, ValueError):
        pass

    raw = str(duration_val).strip()
    if not raw:
        raise ValueError("Duration is empty")

    # Normalise textual labels for matching
    key = raw.lower().replace(" ", "").replace("-", "")

    alias_map = {
        # Half-hourly aliases
        "halfhourly": 0.5,
        "halfhour": 0.5,
        "halfanhour": 0.5,
        "0_5h": 0.5,
        "0_5hr": 0.5,
        # 1 hour aliases
        "1h": 1.0,
        "1hr": 1.0,
        "1hour": 1.0,
        "onehour": 1.0,
        # 2 hour aliases
        "2h": 2.0,
        "2hr": 2.0,
        "2hour": 2.0,
        "twohours": 2.0,
        # 4 hour aliases
        "4h": 4.0,
        "4hr": 4.0,
        "4hour": 4.0,
        "fourhours": 4.0,
    }

    if key in alias_map:
        return alias_map[key]

    raise ValueError(f"Unrecognised spread duration value: {duration_val}")


def _build_spread_type(duration_val):
    """
    Builds a canonical spread TYPE string from a raw duration cell.

    Canonical form matches the legacy CSV pipeline:
      - 0.5  -> SPREAD_0_5HR
      - 1.0  -> SPREAD_1_0HR
      - 2.0  -> SPREAD_2_0HR
      - 4.0  -> SPREAD_4_0HR

    Also supports textual labels like "Half-Hourly", "1hr", "2hr", "4hr".
    """
    hours = _normalize_duration_hours(duration_val)

    if float(hours).is_integer():
        core = f"{int(hours)}_0"
    else:
        core = str(hours).replace(".", "_")

    return f"SPREAD_{core}HR"


def parse_dates(df, year_row_idx, month_row_idx, start_col_idx):
    """Parses dates for monthly data (Central Scenario)"""
    years = df.iloc[year_row_idx]
    months = df.iloc[month_row_idx]
    
    col_date_map = {}
    current_year = None
    
    for col_idx in range(start_col_idx, len(years)):
        val_year = years.iloc[col_idx]
        val_month = months.iloc[col_idx]
        
        if pd.notna(val_year):
            try:
                current_year = int(float(val_year))
            except:
                pass
        
        if pd.notna(val_month) and current_year:
            try:
                clean_month = str(val_month).strip()[:3]
                date_str = f"{clean_month} {current_year}"
                dt = datetime.strptime(date_str, "%b %Y")
                col_date_map[col_idx] = dt
            except:
                pass
                
    return col_date_map

def parse_fy_dates(df, year_row_idx, start_col_idx):
    """Parses Financial Year headers."""
    years_row = df.iloc[year_row_idx]
    col_year_map = {}
    
    for col_idx in range(start_col_idx, len(years_row)):
        val = years_row.iloc[col_idx]
        if pd.notna(val):
            try:
                yr = int(float(val))
                if 2020 <= yr <= 2060:
                    col_year_map[col_idx] = yr
            except:
                pass
    return col_year_map

def expand_fy_to_monthly(price, year, profile, p_type, region, curve_name):
    """Expands a single FY price into 12 monthly records."""
    docs = []
    # FY2025 starts July (Year-1)
    start_date = datetime(year - 1, 7, 1)
    
    for month_offset in range(12):
        current_month = start_date.month + month_offset
        current_year = start_date.year + (current_month - 1) // 12
        final_month = (current_month - 1) % 12 + 1
        
        dt = datetime(current_year, final_month, 1)
        
        docs.append({
            "REGION": region,
            "PROFILE": profile,
            "TYPE": p_type,
            "TIME": dt,
            "PRICE": float(price),
            "curve_name": curve_name
        })
    return docs

def process_monthly_section(df, start_row, end_row, profile, price_type_mapping, date_map, curve_name, region_col_idx=2):
    """Standard processing for monthly data rows"""
    subset = df.iloc[start_row-1 : end_row]
    documents = []
    
    for _, row in subset.iterrows():
        region = row.iloc[region_col_idx]
        if pd.isna(region): continue
            
        for col_idx, date_val in date_map.items():
            price = row.iloc[col_idx]
            if pd.isna(price): continue
            
            documents.append({
                "REGION": region,
                "PROFILE": profile,
                "TYPE": price_type_mapping,
                "TIME": date_val,
                "PRICE": float(price),
                "curve_name": curve_name
            })
    return documents

def process_spreads(df, start_row, end_row, col_year_map, curve_name):
    """
    Specific processing for Spreads (Annual -> Monthly).
    Source: Central Scenario rows 227-251.
    Headers: Row 226 (FY Years).
    Col C (2): Region
    Col D (3): Duration (0.5, 1, 2, 4...)
    """
    print(f"Processing Spreads (rows {start_row}-{end_row})...")
    subset = df.iloc[start_row-1 : end_row]
    documents = []
    
    current_region = None
    
    for _, row in subset.iterrows():
        region_val = row.iloc[2] # Col C
        duration_val = row.iloc[3] # Col D
        
        # Forward fill region
        if pd.notna(region_val):
            current_region = region_val
        
        if not current_region:
            continue
            
        region = current_region
        
        # Skip if duration is missing (empty row)
        if pd.isna(duration_val): 
            continue
        
        # Construct Type: SPREAD_0_5HR, SPREAD_1_0HR, SPREAD_4_0HR
        try:
            # Handle "Half-hourly" specifically
            dur_str_lower = str(duration_val).lower()
            if 'half' in dur_str_lower:
                dur_float = 0.5
            elif isinstance(duration_val, (int, float)):
                 dur_float = float(duration_val)
            else:
                 # Handle strings like "1h", "0.5h", "1hr"
                 clean_dur = dur_str_lower.replace('hr','').replace('h','').strip()
                 dur_float = float(clean_dur)
            
            # Format to X_Y string
            if dur_float.is_integer():
                dur_str = f"{int(dur_float)}_0"
            else:
                dur_str = str(dur_float).replace('.', '_')
                
            p_type = f"SPREAD_{dur_str}HR"
            
        except Exception as e:
             # Fallback if parsing fails
             print(f"Warning: Could not parse duration '{duration_val}' for region {region}: {e}")
             p_type = f"SPREAD_{str(duration_val)}HR"

        # Iterate over FY columns
        for col_idx, year in col_year_map.items():
            price = row.iloc[col_idx]
            if pd.isna(price): continue
            
            # Expand annual to monthly
            docs = expand_fy_to_monthly(price, year, "storage", p_type, region, curve_name)
            documents.extend(docs)
            
    return documents

def process_lgc(df, start_row, end_row, col_year_map, curve_name):
    """Specific processing for LGC (Row 110 Only). Duplicates to all profiles."""
    subset = df.iloc[start_row-1 : end_row]
    documents = []
    regions = ['NSW', 'VIC', 'QLD', 'SA', 'TAS']
    target_profiles = ['green', 'baseload', 'solar', 'wind']
    
    for _, row in subset.iterrows():
        for col_idx, year in col_year_map.items():
            price = row.iloc[col_idx]
            if pd.isna(price): continue
            
            # Expand to monthly (using placeholder)
            monthly_docs = expand_fy_to_monthly(price, year, "PLACEHOLDER", "GREEN", "placeholder", curve_name)
            
            # Duplicate for all regions AND all profiles
            for doc in monthly_docs:
                for r in regions:
                    for prof in target_profiles:
                        new_doc = doc.copy()
                        new_doc['REGION'] = r
                        new_doc['PROFILE'] = prof
                        documents.append(new_doc)
                    
    return documents

def suggest_curve_name(filename):
    """Suggests a curve name from filename: Aurora_Oct25_... -> AC Oct 2025"""
    # Regex for Aurora_MonYY or Aurora_MonthYear
    match = re.search(r'Aurora_([A-Za-z]+)(\d{2})_', filename)
    if match:
        month = match.group(1)
        year = "20" + match.group(2)
        return f"AC {month} {year}"
    return "AC Custom Upload"

def analyze_excel_file(file_path, filename):
    """
    Parses file to return preview data and suggested name.
    Does NOT ingest.
    """
    try:
        suggested_name = suggest_curve_name(filename)
        
        # 1. Metadata Extraction
        df_meta = pd.read_excel(file_path, sheet_name="Central inputs", header=None, nrows=10)
        metadata = []
        # C1-D3 are indices [0..2], cols [2, 3]
        for i in range(3):
            label = df_meta.iloc[i, 2]
            value = df_meta.iloc[i, 3]
            
            # Safe string conversion
            label_str = str(label).strip() if pd.notna(label) else ""
            value_str = str(value).strip() if pd.notna(value) else ""
            
            if label_str or value_str:
                metadata.append({"label": label_str, "value": value_str})
        
        print(f"DEBUG: Metadata extracted: {metadata}")

        # 2. Baseload Preview
        df_cen = pd.read_excel(file_path, sheet_name="Central scenario", header=None)
        date_map = parse_dates(df_cen, 29, 30, 3)
        print(f"DEBUG: Date Map keys count: {len(date_map)}")
        
        # Reuse ingestion logic to get raw monthly data
        # Rows 32-38 cover the regions. profile="baseload", type="ENERGY"
        raw_docs = process_monthly_section(df_cen, 32, 38, "baseload", "ENERGY", date_map, "PREVIEW")
        
        target_regions = ['NSW', 'VIC', 'QLD', 'SA']
        baseload_data_map = {} 
        
        # Aggregate raw documents to FY
        for doc in raw_docs:
            region = str(doc.get('REGION')).strip()
            if region in target_regions:
                date_val = doc['TIME']
                price = doc['PRICE']
                
                is_fy_year = date_val.month >= 7
                fy = date_val.year + 1 if is_fy_year else date_val.year
                
                if fy not in baseload_data_map:
                    baseload_data_map[fy] = {}
                
                if region not in baseload_data_map[fy]:
                    baseload_data_map[fy][region] = {'total': 0.0, 'count': 0}
                
                baseload_data_map[fy][region]['total'] += price
                baseload_data_map[fy][region]['count'] += 1
                
        # Format for frontend
        baseload_data = []
        for fy in sorted(baseload_data_map.keys()):
            row = {'fy': fy}
            for region in target_regions:
                if region in baseload_data_map[fy]:
                    data = baseload_data_map[fy][region]
                    row[region] = round(data['total'] / data['count'], 2)
            baseload_data.append(row)

        print(f"DEBUG: Baseload records: {len(baseload_data)}")

        # 3. LGC Preview
        df_inp = pd.read_excel(file_path, sheet_name="Central inputs", header=None)
        # FY logic
        fy_map = parse_fy_dates(df_inp, 6, 2) # Row 7 headers
        row_lgc = df_inp.iloc[109] # Row 110
        lgc_data = []
        
        for col_idx, year in fy_map.items():
            price = row_lgc.iloc[col_idx]
            if pd.notna(price):
                lgc_data.append({
                    "date": f"FY{year}", # Display label
                    "price": float(price),
                    "label": f"FY{year}"
                })
        
        return {
            "suggestedName": suggested_name,
            "metadata": metadata,
            "preview": {
                "baseload": baseload_data,
                "lgc": lgc_data
            }
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise e

def ingest_excel_file(file_path, curve_name, db):
    """Full ingestion logic."""
    collection = db['PRICE_Curves_2']
    
    print(f"[PRICE][INGEST] Starting ingest for curve '{curve_name}'", flush=True)

    # 1. Clear existing
    delete_result = collection.delete_many({'curve_name': curve_name})
    print(f"[PRICE][INGEST] Cleared {delete_result.deleted_count} existing documents for '{curve_name}'", flush=True)
    
    all_documents = []
    
    # Read Sheets
    print("[PRICE][INGEST] Reading Excel sheets (Central scenario, Central inputs)...", flush=True)
    df_cen = pd.read_excel(file_path, sheet_name="Central scenario", header=None)
    df_inp = pd.read_excel(file_path, sheet_name="Central inputs", header=None)
    
    # 1. Monthly Dates
    print("[PRICE][INGEST] Parsing monthly dates...", flush=True)
    date_map_monthly = parse_dates(df_cen, 29, 30, 3)
    print(f"[PRICE][INGEST] Monthly date columns detected: {len(date_map_monthly)}", flush=True)
    
    print("[PRICE][INGEST] Processing monthly ENERGY prices (baseload/solar/wind)...", flush=True)
    baseload_docs = process_monthly_section(df_cen, 32, 38, "baseload", "ENERGY", date_map_monthly, curve_name)
    solar_docs = process_monthly_section(df_cen, 77, 82, "solar", "ENERGY", date_map_monthly, curve_name)
    wind_docs = process_monthly_section(df_cen, 83, 87, "wind", "ENERGY", date_map_monthly, curve_name)
    print(f"[PRICE][INGEST]   Baseload docs: {len(baseload_docs)}", flush=True)
    print(f"[PRICE][INGEST]   Solar docs:    {len(solar_docs)}", flush=True)
    print(f"[PRICE][INGEST]   Wind docs:     {len(wind_docs)}", flush=True)

    all_documents.extend(baseload_docs)
    all_documents.extend(solar_docs)
    all_documents.extend(wind_docs)
    
    # 2. Spreads
    print("[PRICE][INGEST] Processing spreads (storage profile)...", flush=True)
    fy_map_spreads = parse_fy_dates(df_cen, 225, 3)
    print(f"[PRICE][INGEST]   Spread FY columns detected: {len(fy_map_spreads)}", flush=True)
    spread_docs = process_spreads(df_cen, 227, 251, fy_map_spreads, curve_name)
    print(f"[PRICE][INGEST]   Spread docs: {len(spread_docs)}", flush=True)
    all_documents.extend(spread_docs)
    
    # 3. LGC (Row 110)
    # Header row 7 (index 6)
    print("[PRICE][INGEST] Processing LGC (GREEN) data...", flush=True)
    fy_map_lgc = parse_fy_dates(df_inp, 6, 2)
    print(f"[PRICE][INGEST]   LGC FY columns detected: {len(fy_map_lgc)}", flush=True)
    lgc_docs = process_lgc(df_inp, 110, 110, fy_map_lgc, curve_name)
    print(f"[PRICE][INGEST]   LGC docs: {len(lgc_docs)}", flush=True)
    all_documents.extend(lgc_docs)
    
    total_docs = len(all_documents)
    print(f"[PRICE][INGEST] Total documents to insert for '{curve_name}': {total_docs}", flush=True)
    
    if all_documents:
        print("[PRICE][INGEST] Inserting documents into MongoDB (this may take some time)...", flush=True)
        collection.insert_many(all_documents)
        print(f"[PRICE][INGEST] Insert complete for '{curve_name}'", flush=True)
        return total_docs
    
    print(f"[PRICE][INGEST] No documents generated for '{curve_name}'", flush=True)
    return 0

def get_price_curves_list(db):
    """Returns a list of unique price curve names."""
    collection = db['PRICE_Curves_2']
    return collection.distinct('curve_name')

def load_price_data_from_mongo(db, curve_name):
    """
    Loads price data from MongoDB and transforms it into the expected DataFrames
    for run_cashflow_model (monthly_prices, yearly_spreads).

    IMPORTANT: The returned DataFrames must match the legacy CSV-based schema
    used across the model so that helpers like get_merchant_price() continue
    to work without modification.

    Expected shapes:
      - monthly_prices columns:
          ['profile', 'type', 'REGION', 'time', '_time_dt', 'price']
      - yearly_spreads columns:
          ['REGION', 'DURATION', 'YEAR', 'SPREAD']
    """
    collection = db['PRICE_Curves_2']
    cursor = collection.find({'curve_name': curve_name})

    monthly_rows = []
    spread_rows = []

    for doc in cursor:
        p_type = doc.get('TYPE')
        # Check if this is a spread
        if p_type and str(p_type).startswith('SPREAD_'):
            # It is a spread (e.g. SPREAD_0.5h, SPREAD_2h, SPREAD_0_5HR, SPREAD_2HR)
            try:
                raw = str(p_type)
                # Strip known prefix/suffix noise
                core = raw.replace('SPREAD_', '')
                core = core.upper().replace('HRS', '').replace('HR', '').replace('H', '')
                # Allow underscore as decimal separator (e.g. 0_5 -> 0.5)
                core = core.replace('_', '.')

                # Extract the first numeric token
                match = re.search(r'(\d+(?:\.\d+)?)', core)
                if not match:
                    raise ValueError(f"Could not find numeric duration in '{p_type}'")

                duration = float(match.group(1))

                # Extract year from TIME
                dt = doc.get('TIME')
                if dt:
                    year = dt.year

                    spread_rows.append({
                        'REGION': doc.get('REGION'),
                        'DURATION': duration,
                        'YEAR': year,
                        'SPREAD': doc.get('PRICE')
                    })
            except Exception as e:
                print(f"Warning: Could not parse spread type {p_type}: {e}")

        else:
            # It is monthly price data (Energy, LGC, etc.)
            # Target columns: profile, type, REGION, time, _time_dt, price
            dt = doc.get('TIME')

            raw_type = doc.get('TYPE')
            norm_type = raw_type
            if isinstance(raw_type, str):
                upper = raw_type.upper()
                # Normalise to match legacy CSV values expected by get_merchant_price
                if upper == 'ENERGY':
                    norm_type = 'Energy'
                elif upper == 'GREEN':
                    norm_type = 'green'

            monthly_rows.append({
                'profile': doc.get('PROFILE'),
                'type': norm_type,
                'REGION': doc.get('REGION'),
                '_time_dt': dt,  # datetime object
                'time': dt.strftime('%d/%m/%Y') if dt else None,
                'price': doc.get('PRICE')
            })

    # Create DataFrames
    if monthly_rows:
        monthly_prices = pd.DataFrame(monthly_rows)
    else:
        # Return empty DF with expected columns
        monthly_prices = pd.DataFrame(
            columns=['profile', 'type', 'REGION', 'time', '_time_dt', 'price']
        )

    if spread_rows:
        yearly_spreads = pd.DataFrame(spread_rows)
        # Drop duplicates: we strictly need annual values.
        # Since we map from monthly, we'll have 12 rows per year.
        # They should all have the same annual price (spread), so dropping duplicates is safe.
        yearly_spreads = yearly_spreads.drop_duplicates(subset=['REGION', 'DURATION', 'YEAR'])
    else:
        yearly_spreads = pd.DataFrame(columns=['REGION', 'DURATION', 'YEAR', 'SPREAD'])

    return monthly_prices, yearly_spreads
