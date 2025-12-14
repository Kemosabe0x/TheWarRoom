import nflreadpy as nfl
import pandas as pd
import os
from datetime import datetime

# --- CONFIGURATION ---
current_date = datetime.now()
# If month > 2 (March), assume new season year, otherwise previous year
current_year = current_date.year if current_date.month > 2 else current_date.year - 1
DATA_DIR = "data"

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

print(f"Starting Data Update (nflreadpy) for Season: {current_year}")

# --- 1. FETCH PLAY-BY-PLAY (Analytics & Gruden) ---
print("Fetching Play-by-Play Data...")
try:
    # nflreadpy returns Polars by default; convert to pandas for easier filtering
    pbp = nfl.load_pbp(seasons=[current_year]).to_pandas()
    
    # AGENT 1: ANALYTICS (EPA, CPOE)
    analytics_cols = [
        'game_id', 'week', 'posteam', 'defteam', 'qtr', 'down', 
        'play_type', 'epa', 'wpa', 'cpoe', 'success'
    ]
    # Filter for rows with actual stats
    analytics_df = pbp[analytics_cols].dropna(subset=['epa'])
    analytics_df.to_csv(f"{DATA_DIR}/agent_analytics_kb.csv", index=False)
    print(" -> Analytics Agent data saved.")

    # AGENT 2: COACH GRUDEN (Raw Descriptions)
    gruden_cols = [
        'game_id', 'week', 'posteam', 'defteam', 'desc', 
        'play_type', 'yards_gained', 'touchdown'
    ]
    gruden_df = pbp[gruden_cols]
    gruden_df.to_csv(f"{DATA_DIR}/agent_gruden_kb.csv", index=False)
    print(" -> Coach Gruden data saved.")

except Exception as e:
    print(f"Error fetching PBP: {e}")

# --- 2. FETCH NEXT GEN STATS (The Scout) ---
print("Fetching Next Gen Stats...")
try:
    # nflreadpy uses load_nextgen_stats()
    ngs = nfl.load_nextgen_stats(seasons=[current_year]).to_pandas()
    
    # Filter to only this season if the function returns multiple
    if 'season' in ngs.columns:
        ngs = ngs[ngs['season'] == current_year]
        
    ngs.to_csv(f"{DATA_DIR}/agent_scout_kb.csv", index=False)
    print(" -> Scout Agent data saved.")
except Exception as e:
    print(f"Error fetching NGS: {e}")

# --- 3. FETCH CONTRACTS (The GM) ---
print("Fetching Contract Data...")
try:
    # nflreadpy uses load_contracts()
    contracts = nfl.load_contracts().to_pandas()
    
    # Basic filtering for active contracts
    if 'is_active' in contracts.columns:
        contracts = contracts[contracts['is_active'] == True]
        
    contracts.to_csv(f"{DATA_DIR}/agent_gm_kb.csv", index=False)
    print(" -> GM Agent data saved.")
except Exception as e:
    print(f"Error fetching Contracts: {e}")

# --- 4. FETCH FANTASY STATS (The Casual Fan) ---
print("Fetching Fantasy Data...")
try:
    # nflreadpy's load_player_stats includes fantasy points
    player_stats = nfl.load_player_stats(seasons=[current_year]).to_pandas()
    
    # Keep relevant fantasy columns
    fantasy_cols = [
        'player_display_name', 'position', 'recent_team', 'week',
        'completions', 'passing_yards', 'passing_tds',
        'carries', 'rushing_yards', 'rushing_tds',
        'receptions', 'receiving_yards', 'receiving_tds',
        'fantasy_points', 'fantasy_points_ppr'
    ]
    
    # Filter columns that exist (in case naming varies)
    existing_cols = [c for c in fantasy_cols if c in player_stats.columns]
    fantasy_df = player_stats[existing_cols]
    
    fantasy_df.to_csv(f"{DATA_DIR}/agent_fantasy_kb.csv", index=False)
    print(" -> Fantasy Agent data saved.")
except Exception as e:
    print(f"Error fetching Fantasy data: {e}")

print("Update Complete.")
