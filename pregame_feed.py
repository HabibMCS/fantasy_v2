import os
import pyodbc
import json
from dotenv import load_dotenv
import time
from datetime import datetime
import requests

class PregameTextGenerator:
    def __init__(self):
        self.db_connection = None
        self.cursor = None
        self.texts_dir = './texts'
        self.config_file = './config/game_config.json'
        load_dotenv()
    def fetch_api_data(self, endpoint, params=None):
        """Fetch data from API"""
        try:
            api_host = "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
            url = f"https://{api_host}/{endpoint}"
            
            headers = {
                "X-RapidAPI-Key": os.getenv('RAPIDAPI_KEY'),
                "X-RapidAPI-Host": api_host
            }
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as err:
            print(f"API Error ({endpoint}): {err}")
            return None


    def connect_to_database(self):
        """Establish connection to SQL Server database"""
        try:
            connection_string = (
                "Driver={ODBC Driver 18 for SQL Server};"
                "Server=tcp:34.237.12.44,1433;"
                "Database=NFLv2;"
                "UID=%s;"
                "PWD=%s;"
                "TrustServerCertificate=yes;"
                "Encrypt=yes;"
            ) % (os.getenv('ODBC_DB_USER'), os.getenv('ODBC_DB_PASS'))
            
            self.db_connection = pyodbc.connect(connection_string)
            self.cursor = self.db_connection.cursor()
            print("Database connection established successfully")
        except pyodbc.Error as err:
            print(f"Error connecting to database: {err}")
            raise

    def read_config(self):
        """Read configuration from JSON file"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    return json.load(f)
            print(f"Config file not found: {self.config_file}")
            return None
        except json.JSONDecodeError as err:
            print(f"Error reading config file: {err}")
            return None


    def get_game_data(self, match_id):
        """Get comprehensive game data including top performers"""
        try:
            game_date = match_id.split('_')[0]
            response = self.fetch_api_data("getNFLTeams")
            
            if not response or 'body' not in response:
                return None
                
            game_data = response['body'].get(match_id)
            if not game_data:
                print(f"No game data found for match_id: {match_id}")
                return None
                
            return game_data
        except Exception as err:
            print(f"Error fetching game data: {err}")
            return None
    def create_texts_directory(self):
        """Create texts directory if it doesn't exist"""
        if not os.path.exists(self.texts_dir):
            os.makedirs(self.texts_dir)
        if not os.path.exists(os.path.dirname(self.config_file)):
            os.makedirs(os.path.dirname(self.config_file))

    def save_text_file(self, match_id, variation, content):
        """Save text content to file"""
        filename = f"{self.texts_dir}/pregame_{match_id}_{variation}.txt"
        try:
            with open(filename, 'w') as f:
                f.write(content)
            print(f"Generated {filename}")
        except IOError as err:
            print(f"Error saving file {filename}: {err}")

    def generate_pregame_1(self, game_data):
        """Generate Pregame Scroll #1 with game info"""
        gdate = game_data['gameID'].split('_')[0]
        formatted_date = f"{gdate[6:]}.{gdate[4:6]}.{gdate[:4]}"
        return f"{game_data['home']} vs {game_data['away']} | {formatted_date} {game_data['gameTime']} EST | {game_data['gameStatus']}"

    def generate_pregame_2(self, team_data):
        """Generate team standings format"""
        return (
            f"{team_data['teamAbv']} {team_data['teamName']} {team_data['wins']}-{team_data['loss']}-{team_data['tie']} | "
            f"{team_data['conferenceAbv']} {team_data['division']} | "
            f"PF {team_data['pf']} | PA {team_data['pa']}"
        )

    def format_team_stats(self, team_abv, team_data):
        """Format team statistics with improved structure"""
        try:
            passing = team_data['Passing']
            return (
                f"{team_abv} | "
                f" Pass: {passing['passYds']['total']} YDs, {passing['passTD']['total']} TD | "
                f" INT: {passing['int']['total']}"
            )
        except Exception as err:
            print(f"Error formatting team stats for {team_abv}: {err}")
            return f"{team_abv} - Stats unavailable"

    def generate_performance_stats(self, team_data, player_names, stat_type):
        """Generate performance statistics with unified approach"""
        try:
            if stat_type == 'A':
                return self.format_passing_stats(team_data['teamAbv'], team_data['topPerformers']['Passing'], player_names)
            elif stat_type == 'B':
                return self.format_rushing_stats(team_data['teamAbv'], team_data['topPerformers']['Rushing'], player_names)
            elif stat_type == 'C':
                return self.format_receiving_stats(team_data['teamAbv'], team_data['topPerformers']['Receiving'], player_names)
            elif stat_type == 'D':
                return self.format_defense_stats(team_data['teamAbv'], team_data['topPerformers']['Defense'], player_names)
        except Exception as err:
            print(f"Error generating performance stats: {err}")
            return f"{team_data['teamAbv']} - Stats unavailable"


    def format_passing_stats(self, team_abv, stats, player_names):
        """Format passing statistics with player names"""
        return (
            f"{team_abv} Passing Statistics\n"
            f"{player_names.get(stats['passYds']['playerID'][0], 'Unknown')} - {stats['passYds']['total']} Pass YDs | "
            f"{player_names.get(stats['passTD']['playerID'][0], 'Unknown')} - {stats['passTD']['total']} Pass TD"
        )

    def format_rushing_stats(self, team_abv, stats, player_names):
        """Format rushing statistics with player names"""
        return (
            f"{team_abv} Rushing Statistics\n"
            f"{player_names.get(stats['rushYds']['playerID'][0], 'Unknown')} - {stats['rushYds']['total']} Rush YDs | "
            f"{player_names.get(stats['rushTD']['playerID'][0], 'Unknown')} - {stats['rushTD']['total']} Rush TD"
        )

    def format_receiving_stats(self, team_abv, stats, player_names):
        """Format receiving statistics with player names"""
        return (
            f"{team_abv} Receiving Statistics\n"
            f"{player_names.get(stats['receptions']['playerID'][0], 'Unknown')} - {stats['receptions']['total']} Rec. | "
            f"{player_names.get(stats['recYds']['playerID'][0], 'Unknown')} - {stats['recYds']['total']} Rec. YDs | "
            f"{player_names.get(stats['recTD']['playerID'][0], 'Unknown')} - {stats['recTD']['total']} Rec. TD"
        )

    def format_defense_stats(self, team_abv, stats, player_names):
        """Format defense statistics with player names"""
        return (
            f"{team_abv} Defense Statistics\n"
            f"{player_names.get(stats['totalTackles']['playerID'][0], 'Unknown')} - {stats['totalTackles']['total']} Tackles | "
            f"{player_names.get(stats['sacks']['playerID'][0], 'Unknown')} - {stats['sacks']['total']} Sacks | "
            f"{player_names.get(stats['defensiveInterceptions']['playerID'][0], 'Unknown')} - {stats['defensiveInterceptions']['total']} Int."
        )

    def get_player_names(self, player_ids):
        """Fetch player names from database using player IDs"""
        try:
            # Convert player IDs list to unique set
            unique_ids = set()
            for pid in player_ids:
                if isinstance(pid, list):
                    unique_ids.update(pid)
                else:
                    unique_ids.add(pid)

            # Create placeholders for SQL query
            placeholders = ','.join(['?' for _ in unique_ids])

            query = f"""
                SELECT playerID, cbsLongName 
                FROM player_info 
                WHERE playerID IN ({placeholders})
            """
            self.cursor.execute(query, list(unique_ids))
            return {str(row[0]): row[1] for row in self.cursor.fetchall()}
        except Exception as err:
            print(f"Error fetching player names: {err}")
            return {}

    def process_game(self, match_id,postgame=False):
        """Process game data and generate all pregame variations"""
        try:
            params = {"sortBy":"standings","rosters":"false","topPerformers":"true","teamStats":"true","teamStatsSeason":"2024"} if postgame else {f"gameDate":{match_id.split("_")[0]},"topPerformers":"true"}
            response = self.fetch_api_data("getNFLTeams",params=params) if postgame else self.fetch_api_data("getNFLScoresOnly",params=params)
            if not response or 'body' not in response:
                print("No teams data received")
                return None
            teams_data = response['body']
            if not teams_data:
                print("No teams data found")
                return None
            game_info = {
                'gameID': match_id,
                'home': match_id.split('_')[1].split('@')[0],
                'away': match_id.split('_')[1].split('@')[1],
                'gametime': "8:20 PM",  # You might want to get this from another API
                'gameStatus': "Scheduled"
            }
            home_team = next((team for team in teams_data if team['teamAbv'] == game_info['home']), None)
            away_team = next((team for team in teams_data if team['teamAbv'] == game_info['away']), None)
            if not home_team or not away_team:
                print(f"Could not find data for teams: {game_info['home']} or {game_info['away']}")
                return None
            player_ids = []
            for team in [home_team, away_team]:
                for stat_type in team['topPerformers'].values():
                    for stat in stat_type.values():
                        if isinstance(stat, dict) and 'playerID' in stat:
                            player_ids.extend(stat['playerID'])
            
            
            player_names = self.get_player_names(player_ids)
            # Generate all pregame texts
            pregame_texts = []
            # Pregame 1 - Basic game info
           # pregame_1 = self.generate_pregame_1(game_info)
           # pregame_texts.append(pregame_1)

            # Pregame 2 - Team standings
            home_pregame_2 = self.generate_pregame_2(home_team)
            away_pregame_2 = self.generate_pregame_2(away_team)
            pregame_texts.extend([home_pregame_2, away_pregame_2])

            for stat_type in ['A', 'B', 'C', 'D']:
                stat = self.generate_performance_stats(home_team, player_names, stat_type)
                pregame_texts.append(stat)

            for stat_type in ['A', 'B', 'C', 'D']:
                stat = self.generate_performance_stats(away_team, player_names, stat_type)
                pregame_texts.append(stat)

            # Save all texts to files
            # for i, content in enumerate(pregame_texts):
            #     variation = None
            #     if i == 0:
            #         variation = "1"
            #     elif i in [1, 2]:
            #         variation = "2"
            #     elif 3 <= i <= 6:
            #         variation = f"3{chr(ord('A') + i - 3)}"
            #     else:
            #         variation = f"4{chr(ord('A') + i - 7)}"
                
            #     self.save_text_file(match_id, variation, content)
            if postgame:
                return pregame_texts[2:]
            return pregame_texts

        except Exception as err:
            print(f"Error processing game {match_id}: {err}")
            return None
# def process_game(self, match_id, variation_type=None):
#     """Process game data with improved error handling"""
#     try:
#         game_data = self.get_game_data(match_id)
#         if not game_data:
#             return

#         # Generate basic game info
#         pregame_1 = self.generate_pregame_1(game_data)
#         pregame_2 = self.generate_pregame_2(game_data)
#         pregame_3a = self.generate_performance_stats(game_data,'3','A')
#         pregame_3b = self.generate_performance_stats(game_data,'3','B')
#         pregame_3c = self.generate_performance_stats(game_data,'3','C')
#         pregame_3d = self.generate_performance_stats(game_data,'3','D')
#         pregame_4a = self.generate_performance_stats(game_data,'4','A')
#         pregame_4b = self.generate_performance_stats(game_data,'4','B')
#         pregame_4c = self.generate_performance_stats(game_data,'4','C')
#         pregame_4d = self.generate_performance_stats(game_data,'4','D')
#         return [pregame_1,pregame_2,pregame_3a,pregame_3b,pregame_3c,pregame_3d,
#                 pregame_4a,pregame_4b,pregame_4c,pregame_4d]

#         # if variation_type:
#         #     content = ""
#         #     if variation_type == "2":
#         #         content = self.generate_pregame_2(game_data)
#         #     elif variation_type[0] in ['3', '4'] and variation_type[1] in ['A', 'B', 'C', 'D']:
#         #         content = self.generate_performance_stats(
#         #             game_data, 
#         #             variation_type[0], 
#         #             variation_type[1]
#         #         )

#         #     if content:
#         #         self.save_text_file(match_id, variation_type, content)

#     except Exception as err:
#         print(f"Error processing game {match_id}: {err}")

#     # [Rest of the class methods remain the same...]
#     def run_continuous(self, interval=8):
#         """Run the generator continuously with specified interval"""
#         while True:
#             try:
#                 print(f"\nStarting text generation cycle at {datetime.now()}")
                
#                 # Ensure database connection is active
#                 if not self.db_connection or self.db_connection.closed:
#                     self.connect_to_database()
                
#                 self.create_texts_directory()
                
#                 # Read configuration
#                 config = self.read_config()
#                 if config:
#                     match_id = config.get('match_id')
#                     variation_type = config.get('variation_type')
                    
#                     if match_id:
#                         self.process_game(match_id, variation_type)
#                     else:
#                         print("No match_id specified in config")
#                 else:
#                     print("No valid configuration found")
                
#                 print(f"Completed cycle at {datetime.now()}")
#                 print(f"Waiting {interval} seconds before next update...")
#                 time.sleep(interval)
                
#             except Exception as err:
#                 print(f"Error in main loop: {err}")
#                 print("Attempting to reconnect...")
#                 time.sleep(5)
                
#                 try:
#                     if self.db_connection:
#                         self.db_connection.close()
#                     self.connect_to_database()
#                 except Exception as conn_err:
#                     print(f"Failed to reconnect: {conn_err}")
#                     time.sleep(5)

def main():
    generator = PregameTextGenerator()
    
    # Create initial config file if it doesn't exist
    if not os.path.exists(generator.config_file):
        initial_config = {
            "match_id": "20241201_SF_WAS",  # Example match ID
            "variation_type": "2"  # Example variation type
        }
        os.makedirs(os.path.dirname(generator.config_file), exist_ok=True)
        with open(generator.config_file, 'w') as f:
            json.dump(initial_config, f, indent=4)
    
    try:
        generator.connect_to_database()
        print("Starting continuous text generation...")
        generator.run_continuous(interval=8)
    except KeyboardInterrupt:
        print("\nStopping text generation...")
    except Exception as err:
        print(f"Application error: {err}")
    finally:
        if generator.db_connection:
            generator.db_connection.close()
            print("Database connection closed")

# if __name__ == "__main__":
#     main()