import os
import pandas as pd
import json

# Directory containing JSON files
directory = 'json_data'
# directory = 't20s_male_json'

# List all JSON files in the directory
json_files = [file for file in os.listdir(directory) if file.endswith('.json')]

# Initialize an empty list to store match info
match_info_list = []

# Initialize an empty DataFrame to store the combined data
combined_df = pd.DataFrame()

# Loop through each JSON file
for file in json_files:
    # Read JSON file
    with open(os.path.join(directory, file)) as f:
        data = json.load(f)

    try:
        match_type_number = 'T20I # {}'.format(data['info']['match_type_number'])
        # --------city----------
        try:
            city = data['info']['city']
        except:
            city = '-'
        # --------player_of_match----------
        try:
            player_of_match = data['info']['player_of_match']
        except:
            player_of_match = '-'
        # --------winner----------
        try:
            winner = data['info']['outcome']['winner']
        except:
            winner = '-'


        # ------------------
        team1 = data['info']['teams'][0]
        team2 = data['info']['teams'][1]

        team1_players = data['info']['players'][team1]
        team2_players = data['info']['players'][team2]

        # ------------------
        try:
            if 'bat' in data['info']['toss']['decision']:
                if data['info']['toss']['winner'] == team1:
                    first_bat = team1
                    first_ball = team2
                else:
                    first_bat = team2
                    first_ball = team1
        except KeyError as e:
            pass
        try:
            if 'field' in data['info']['toss']['decision']:
                if data['info']['toss']['winner'] == team1:
                    first_bat = team2
                    first_ball = team1
                else:
                    first_bat = team1
                    first_ball = team2
        except KeyError as e:
            pass

        # ------------------
        try:
            if 'wickets' in data['info']['outcome']['by']:
                winner_method = 'wickets'
                winner_margin = data['info']['outcome']['by']['wickets']
        except KeyError as e:
            pass
        try:
            if 'runs' in data['info']['outcome']['by']:
                winner_method = 'runs'
                winner_margin = data['info']['outcome']['by']['runs']
        except KeyError as e:
            pass
        try:
            if 'tie' in data['info']['outcome']['result']:
                winner_method = 'tie'
                winner_margin = '-'
        except KeyError as e:
            pass
        try:
            if 'no result' in data['info']['outcome']['result']:
                winner_method = 'no result'
                winner_margin = '-'
        except KeyError as e:
            pass

        # ------------------


        # Extract relevant information from the JSON data
        match_info = {
            'match_type_number': match_type_number,
            'date': data['info']['dates'][0],
            'city': city,
            'winner': 'winner',
            'winner_method': winner_method,
            'winner_margin': winner_margin,
            'total_overs': data['info']['overs'],
            'player_of_match': player_of_match,
            'team1': team1,
            'team2': team2,
            'team1_players': team1_players,
            'team2_players': team2_players,
            'first_bat': first_bat,
            'first_ball': first_ball,
            'venue': data['info']['venue']
        }
        # Append match info to list
        match_info_list.append(match_info)
        # print(match_info)

        # Extract innings data
        innings_data = []
        for inning in data['innings']:
            for over in inning['overs']:
                for delivery in over['deliveries']:
                    inning_info = {
                        'inning': inning['team'],
                        'over': over['over'],
                        'batter': delivery['batter'],
                        'bowler': delivery['bowler'],
                        'runs_batter': delivery['runs']['batter'],
                        'extras': delivery['runs'].get('extras', 0) if isinstance(delivery['runs'], dict) else 0,
                        'total_runs': delivery['runs']['total']
                    }
                    if 'wickets' in delivery:
                        inning_info['wicket_player_out'] = delivery['wickets'][0]['player_out']
                        inning_info['wicket_kind'] = delivery['wickets'][0]['kind']
                    else:
                        inning_info['wicket_player_out'] = None
                        inning_info['wicket_kind'] = None
                    innings_data.append(inning_info)

        # Create DataFrame for the current match
        df = pd.DataFrame(innings_data)

        # Append the DataFrame to the combined DataFrame
        combined_df = pd.concat([combined_df, df], ignore_index=True)

    except KeyError as e:
        print(f"KeyError occurred while processing file {file}: {e}")

# Create DataFrame for match info
match_info_df = pd.DataFrame(match_info_list)

# Repeat match info for each record in combined DataFrame
combined_df = pd.concat([match_info_df.reindex(combined_df.index, method='ffill'), combined_df], axis=1)

# Print the column list of the combined DataFrame
print("Column List:")
print(combined_df.columns.tolist())

# Write the combined DataFrame to a CSV file
combined_df.to_csv('combined_data.csv', index=False)

# Display the combined DataFrame
print(combined_df)

# Write the combined DataFrame to a CSV file
combined_df.to_csv('combined_data.csv', index=False)

# Display the combined DataFrame
print(combined_df)

