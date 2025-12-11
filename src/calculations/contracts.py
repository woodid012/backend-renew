from datetime import datetime

def calculate_contract_revenue(contract, current_date, monthly_generation, buyers_percentage, degradation_factor):
    """
    Calculates revenue for a single contract based on its type and terms.
    
    Args:
        contract (dict): Contract definition
        current_date (datetime): Current simulation date
        monthly_generation (float): Monthly generation volume
        buyers_percentage (float): Percentage of generation covered by this contract (0.0 to 1.0)
        degradation_factor (float): Asset degradation factor
        
    Returns:
        dict: Breakdown of revenue components
    """
    
    # Initialize return values
    contracted_green = 0.0
    contracted_energy = 0.0
    
    # Calculate indexation
    contract_start_date = datetime.strptime(contract['startDate'], '%Y-%m-%d')
    years_in_contract = (current_date.year - contract_start_date.year) + (current_date.month - contract_start_date.month) / 12
    indexation = float(contract.get('indexation', 0)) / 100
    indexation_factor = (1 + indexation) ** max(0, years_in_contract)

    contract_type = contract.get('type')

    if contract_type == 'fixed':
        annual_revenue = float(contract.get('strikePrice', 0))
        # Fixed revenue is spread over the year, adjusted for indexation and degradation
        contract_revenue = annual_revenue / 12 * indexation_factor * degradation_factor
        contracted_energy += contract_revenue

    elif contract_type == 'bundled':
        green_price = float(contract.get('greenPrice', 0) or 0)
        energy_price = float(contract.get('EnergyPrice', 0) or 0)

        green_price *= indexation_factor
        energy_price *= indexation_factor

        # Apply floor logic if applicable
        if contract.get('hasFloor') and (green_price + energy_price) < float(contract.get('floorValue', 0)):
            floor_value = float(contract['floorValue'])
            total_price = green_price + energy_price
            if total_price > 0:
                green_price = (green_price / total_price) * floor_value
                energy_price = (energy_price / total_price) * floor_value
            else:
                green_price = floor_value / 2
                energy_price = floor_value / 2

        contracted_green += (monthly_generation * buyers_percentage * green_price) / 1_000_000
        contracted_energy += (monthly_generation * buyers_percentage * energy_price) / 1_000_000

    else: # Single product contracts (green or Energy)
        price = float(contract.get('strikePrice', 0))
        price *= indexation_factor

        if contract.get('hasFloor') and price < float(contract.get('floorValue', 0)):
            price = float(contract['floorValue'])

        contract_revenue = (monthly_generation * buyers_percentage * price) / 1_000_000

        if contract_type == 'green':
            contracted_green += contract_revenue
        elif contract_type == 'Energy':
            contracted_energy += contract_revenue
            
    return {
        'contracted_green': contracted_green,
        'contracted_energy': contracted_energy
    }

def calculate_storage_contract_revenue(contract, current_date, monthly_volume, capacity, buyers_percentage, degradation_factor, volume_loss_adjustment, hours_in_month):
    """
    Calculates revenue for storage contracts (CFD, Tolling, Fixed).
    """
    contracted_revenue = 0.0
    
    contract_start_date = datetime.strptime(contract['startDate'], '%Y-%m-%d')
    years_in_contract = (current_date.year - contract_start_date.year) + (current_date.month - contract_start_date.month) / 12
    indexation = float(contract.get('indexation', 0)) / 100
    indexation_factor = (1 + indexation) ** max(0, years_in_contract)
    
    contract_type = contract.get('type')

    if contract_type == 'fixed':
        annual_revenue = float(contract.get('strikePrice', 0))
        contracted_revenue += (annual_revenue / 12 * indexation_factor * degradation_factor)

    elif contract_type == 'cfd':
        price_spread = float(contract.get('strikePrice', 0))
        adjusted_spread = price_spread * indexation_factor
        revenue = monthly_volume * adjusted_spread * buyers_percentage
        contracted_revenue += revenue / 1_000_000

    elif contract_type == 'tolling':
        hourly_rate = float(contract.get('strikePrice', 0))
        adjusted_rate = hourly_rate * indexation_factor
        revenue = capacity * hours_in_month * adjusted_rate * degradation_factor * volume_loss_adjustment
        contracted_revenue += (revenue / 1_000_000)
        
    return contracted_revenue
