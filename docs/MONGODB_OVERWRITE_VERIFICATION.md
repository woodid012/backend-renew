# MongoDB Overwrite Verification

## Summary
This document verifies that MongoDB write operations **overwrite** data on each run rather than appending, preventing duplicate/stacked data issues.

## Cleanup Results
✅ **Cleared all duplicate data:**
- ASSET_cash_flows: 3,609 records deleted
- ASSET_inputs_summary: 135 records deleted  
- ASSET_Output_Summary: 150 records deleted
- SENS_Asset_Outputs: 50,526 records deleted
- **Total: 54,420 records deleted**

## Overwrite Logic Verification

### 1. Main Cash Flow Data (`ASSET_cash_flows`)
**Location:** `src/main.py:625-630`
```python
insert_dataframe_to_mongodb(
    final_cash_flow, 
    MONGO_ASSET_OUTPUT_COLLECTION, 
    scenario_id=scenario_id,
    replace_scenario=True  # ✅ Always replace for clean data
)
```
✅ **VERIFIED:** Uses `replace_scenario=True`

### 2. Asset Output Summary (`ASSET_Output_Summary`)
**Location:** `src/main.py:271-276`
```python
insert_dataframe_to_mongodb(
    summary_df, 
    MONGO_ASSET_OUTPUT_SUMMARY_COLLECTION, 
    scenario_id=scenario_id,
    replace_scenario=True
)
```
✅ **VERIFIED:** Uses `replace_scenario=True`

### 3. Asset Inputs Summary (`ASSET_inputs_summary`)
**Location:** `src/main.py:689-693`
```python
insert_dataframe_to_mongodb(
    asset_summary_df, 
    MONGO_ASSET_INPUTS_SUMMARY_COLLECTION, 
    scenario_id=scenario_id,
    replace_scenario=True  # ✅ Always replace for clean data
)
```
✅ **VERIFIED:** Uses `replace_scenario=True`

## Deletion Logic (Fixed)

### Base Case Records
When writing base case data (no `scenario_id`), the deletion logic now:
1. ✅ Deletes only records for the specific `asset_ids` being written
2. ✅ Handles edge cases (empty string `scenario_id`, None, missing field)
3. ✅ Prevents accidental deletion of other assets' data

**Location:** `src/core/database.py:160-189`

```python
if replace_scenario:
    if scenario_id:
        query = {"scenario_id": scenario_id}
    else:
        # Base case: filter by asset_ids to only delete records for assets being written
        base_case_query = {"$or": [
            {"scenario_id": {"$exists": False}}, 
            {"scenario_id": None}, 
            {"scenario_id": ""}
        ]}
        
        if 'asset_id' in df.columns:
            asset_ids = df['asset_id'].unique().tolist()
            query = {
                "$and": [
                    base_case_query,
                    {"asset_id": {"$in": asset_ids}}
                ]
            }
```

### Scenario Records
When writing scenario data (with `scenario_id`), the deletion logic:
1. ✅ Deletes all records matching the `scenario_id`
2. ✅ Ensures clean scenario data on each run

## Pre-Write Cleanup

Before writing data, the model also calls:
- `clear_base_case_data()` - Clears all base case records (if `replace_data=True` and no `scenario_id`)
- `clear_all_scenario_data(scenario_id)` - Clears all records for a specific scenario

**Location:** `src/main.py:612-619`

## Conclusion

✅ **All write operations use `replace_scenario=True`**
✅ **Deletion logic properly filters by asset_ids for base case**
✅ **Pre-write cleanup ensures no duplicate data**
✅ **All duplicate records have been cleared from MongoDB**

The system now **overwrites** data on each run rather than appending, preventing CAPEX and other metrics from stacking/duplicating.



