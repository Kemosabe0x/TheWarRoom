import nfl_data_py as nfl
import pandas as pd
import os
from datetime import datetime

# --- CONFIGURATION ---
# Get current season (if it's Jan/Feb, we usually want the 'previous' year's season)
current_date = datetime.now()
current_year = current_date.year if current_date.month > 2 else current_date.year - 1
DATA_DIR = "data"

# Create data directory if it doesn't exist
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

print(f"Starting Data Update for Season: {current_year}")

# --- 1. FETCH PLAY-BY-PLAY (The Heavy Lifting) ---
print("Fetching Play-by-Play Data...")
try:
    pbp = nfl.import_pbp_data([current_year])

    # FILTER 1: THE ANALYTICS AGENT (Math & Efficiency)
    # Needs: EPA, CPOE, WPA, Success Rate
    analytics_cols = [
        'game_id', 'week', 'posteam', 'defteam', 'qtr', 'down', 'ydstogo',
        'play_type', 'epa', 'wpa', 'cpoe', 'success', 'cp', 'xyac_mean_yardage'
    ]
    # Filter to only rows where these stats exist (removes timeouts/end of quarters)
    analytics_df = pbp[analytics_cols].dropna(subset=['epa'])
    analytics_df.to_csv(f"{DATA_DIR}/agent_analytics_kb.csv", index=False)
    print(" -> Analytics Agent data saved.")

    # FILTER 2: COACH GRUDEN (The "Tape" & Descriptions)
    # Needs: The raw text description, play type, and yards. Hates math.
    gruden_cols = [
        'game_id', 'week', 'posteam', 'defteam', 'desc',
        'play_type', 'yards_gained', 'touchdown', 'fumble', 'interception'
    ]
    gruden_df = pbp[gruden_cols]
    gruden_df.to_csv(f"{DATA_DIR}/agent_gruden_kb.csv", index=False)
    print(" -> Coach Gruden data saved.")

except Exception as e:
    print(f"Error fetching PBP: {e}")

# --- 2. FETCH NEXT GEN STATS (The Scout) ---
print("Fetching Next Gen Stats...")
try:
    # We fetch passing, rushing, and receiving stats separately and combine them
    ngs_pass = nfl.import_ngs_data(stat_type='passing', years=[current_year])
    ngs_rush = nfl.import_ngs_data(stat_type='rushing', years=[current_year])
    ngs_rec = nfl.import_ngs_data(stat_type='receiving', years=[current_year])

    # Combine relevant scout metrics
    # Scout cares about: Speed (time_to_throw), Aggression (aggressiveness), Separation
    ngs_combined = pd.concat([ngs_pass, ngs_rush, ngs_rec], ignore_index=True)
    ngs_combined.to_csv(f"{DATA_DIR}/agent_scout_kb.csv", index=False)
    print(" -> Scout Agent data saved.")
except Exception as e:
    print(f"Error fetching NGS: {e}")

# --- 3. FETCH CONTRACTS & ROSTERS (The GM) ---
print("Fetching Contract Data...")
try:
    # Import contracts (Spotrac/OTC data)
    contracts = nfl.import_contracts()
    # Filter for active players only to keep file size manageable
    active_contracts = contracts[contracts['is_active'] == True]

    # Simplifies to key financial columns
    gm_cols = ['player', 'position', 'team', 'cols', 'avg_salary', 'guaranteed', 'expires', 'cap_hit']
    # Note: Column names vary slightly in source, this is a generic map.
    # We save the raw active contracts to ensure we get everything.
    active_contracts.to_csv(f"{DATA_DIR}/agent_gm_kb.csv", index=False)
    print(" -> GM Agent data saved.")
except Exception as e:
    print(f"Error fetching Contracts: {e}")

# --- 4. FETCH FANTASY STATS (The Casual Fan) ---
print("Fetching Fantasy Data...")
try:
    fantasy = nfl.import_seasonal_pfr(s_type='all', years=[current_year])
    fantasy.to_csv(f"{DATA_DIR}/agent_fantasy_kb.csv", index=False)
    print(" -> Fantasy Agent data saved.")
except Exception as e:
    print(f"Error fetching Fantasy data: {e}")

print("Update Complete. Timestamp:", datetime.now())
