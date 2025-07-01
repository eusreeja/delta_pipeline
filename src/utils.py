# src/utils.py

def map_production_attributes(prod_type: str) -> tuple:
    """Map production type to gold attributes"""
    prod_type = prod_type.lower()
    if "solar" in prod_type:
        return ("Solar", "Renewable", "Weather_Dependent", "Photovoltaic and solar thermal power")
    elif "wind" in prod_type:
        return ("Wind", "Renewable", "Weather_Dependent", "Onshore and offshore wind power")
    elif "hydro" in prod_type:
        return ("Hydro", "Renewable", "Controllable", "Hydroelectric power")
    elif "nuclear" in prod_type:
        return ("Nuclear", "Non-Renewable", "Controllable", "Nuclear power plants")
    elif "coal" in prod_type:
        return ("Coal", "Non-Renewable", "Controllable", "Coal-fired power plants")
    elif "gas" in prod_type:
        return ("Fossil Gas", "Non-Renewable", "Controllable", "Gas-fired power plants")
    elif "biomass" in prod_type:
        return ("Biomass", "Renewable", "Controllable", "Biomass and biogas plants")
    elif "oil" in prod_type:
        return ("Oil", "Non-Renewable", "Controllable", "Oil-fired power plants")
    elif "battery" in prod_type:
        return ("Battery Storage", "Renewable", "Mixed", "Battery energy storage systems")
    else:
        return (prod_type.title(), "Mixed", "Mixed", "Other or unknown type")
