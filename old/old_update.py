import nfl_data_py as nfl
import pandas as pd
from datetime import datetime

# 1. PLAY-BY-PLAY & EPA (The "Analytics" & "Gruden" Data)
# Pulls data for the current season
current_year = datetime.now().year
pbp = nfl.import_pbp_data([current_year])
# Filter for what you need to save memory
pbp_lite = pbp[['game_id', 'posteam', 'defteam', 'desc', 'epa', 'cpoe']]

# 2. NEXT GEN STATS (The "Scout" Data)
# 'passing', 'rushing', or 'receiving'
ngs_passing = nfl.import_ngs_data(stat_type='passing', years=[current_year])

# 3. CONTRACTS & ROSTERS (The "Business" Data)
# nfl_data_py maps IDs to OverTheCap and Spotrac
rosters = nfl.import_seasonal_rosters([current_year])
contracts = nfl.import_contracts() # Check documentation for specific availability

# 4. FANTASY DATA (The "Casual" Data)
# Pulls directly from Pro-Football-Reference
fantasy = nfl.import_seasonal_pfr(s_type='pass', years=[current_year])

# SAVE TO CSV (Or connect to SQL DB here)
pbp_lite.to_csv("data/play_by_play.csv")
ngs_passing.to_csv("data/next_gen_stats.csv")
print("Data successfully updated!")
