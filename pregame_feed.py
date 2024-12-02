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
        """Fetch data from API with improved error handling"""
        try:
            api_host = "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
            url = f"https://{api_host}/{endpoint}"
            
            headers = {
                "X-RapidAPI-Key": os.getenv('RAPIDAPI_KEY'),
                "X-RapidAPI-Host": api_host
            }
            
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data.get('statusCode') != 200:
                print(f"API Error ({endpoint}): Status {data.get('statusCode')}")
                return None
                
            return data
        except Exception as err:
            print(f"API Error ({endpoint}): {err}")
            return None
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
                "Database=ds-cbssports;"
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
            response = self.fetch_api_data("getNFLScoresOnly", {
                "gameDate": game_date,
                "topPerformers": "true"
            })
            
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

    def generate_pregame_2(self, game_data):
        """Generate team statistics from top performers data"""
        try:
            home_team = game_data['topPerformers'][game_data['home']]
            away_team = game_data['topPerformers'][game_data['away']]
            
            home_text = self.format_team_stats(game_data['home'], home_team)
            away_text = self.format_team_stats(game_data['away'], away_team)
            
            return f"{home_text}\n{away_text}"
        except Exception as err:
            print(f"Error generating team stats: {err}")
            return ""

    def format_team_stats(self, team_abv, team_data):
        """Format team statistics with improved structure"""
        try:
            passing = team_data['Passing']
            return (
                f"{team_abv} | "
                f"Pass: {passing['passYds']['total']} YDs, {passing['passTD']['total']} TD | "
                f"INT: {passing['int']['total']}"
            )
        except Exception as err:
            print(f"Error formatting team stats for {team_abv}: {err}")
            return f"{team_abv} - Stats unavailable"

    def generate_performance_stats(self, game_data, team_type, stat_type):
        """Generate performance statistics with unified approach"""
        try:
            team_abv = game_data['home'] if team_type == '3' else game_data['away']
            team_stats = game_data['topPerformers'][team_abv]
            
            if stat_type == 'A':
                return self.format_passing_stats(team_abv, team_stats['Passing'])
            elif stat_type == 'B':
                return self.format_rushing_stats(team_abv, team_stats['Rushing'])
            elif stat_type == 'C':
                return self.format_receiving_stats(team_abv, team_stats['Receiving'])
            elif stat_type == 'D':
                return self.format_defense_stats(team_abv, team_stats['Defense'])
                
        except Exception as err:
            print(f"Error generating performance stats: {err}")
            return f"{team_abv} - Stats unavailable"

    def format_passing_stats(self, team_abv, stats):
        """Format passing statistics"""
        return (
            f"{team_abv} Passing Statistics\n"
            f"Yards: {stats['passYds']['total']} | "
            f"TD: {stats['passTD']['total']} | "
            f"Comp: {stats['passCompletions']['total']}/{stats['passAttempts']['total']}"
        )

    def format_rushing_stats(self, team_abv, stats):
        """Format rushing statistics"""
        return (
            f"{team_abv} Rushing Statistics\n"
            f"Yards: {stats['rushYds']['total']} | "
            f"TD: {stats['rushTD']['total']} | "
            f"Carries: {stats['carries']['total']}"
        )

    def format_receiving_stats(self, team_abv, stats):
        """Format receiving statistics"""
        return (
            f"{team_abv} Receiving Statistics\n"
            f"Receptions: {stats['receptions']['total']} | "
            f"Yards: {stats['recYds']['total']} | "
            f"TD: {stats['recTD']['total']}"
        )

    def format_defense_stats(self, team_abv, stats):
        """Format defense statistics"""
        return (
            f"{team_abv} Defense Statistics\n"
            f"Tackles: {stats['totalTackles']['total']} | "
            f"Sacks: {stats['sacks']['total']} | "
            f"INT: {stats['defensiveInterceptions']['total']}"
        )

    def process_game(self, match_id, variation_type=None):
        """Process game data with improved error handling"""
        try:
            game_data = self.get_game_data(match_id)
            if not game_data:
                return

            # Generate basic game info
            pregame_1 = self.generate_pregame_1(game_data)
            pregame_2 = self.generate_pregame_2(game_data)
            pregame_3a = self.generate_performance_stats(game_data,'3','A')
            pregame_3b = self.generate_performance_stats(game_data,'3','B')
            pregame_3c = self.generate_performance_stats(game_data,'3','C')
            pregame_3d = self.generate_performance_stats(game_data,'3','D')
            pregame_4a = self.generate_performance_stats(game_data,'4','A')
            pregame_4b = self.generate_performance_stats(game_data,'4','B')
            pregame_4c = self.generate_performance_stats(game_data,'4','C')
            pregame_4d = self.generate_performance_stats(game_data,'4','D')
            return [pregame_1,pregame_2,pregame_3a,pregame_3b,pregame_3c,pregame_3d,
                    pregame_4a,pregame_4b,pregame_4c,pregame_4d]

            # if variation_type:
            #     content = ""
            #     if variation_type == "2":
            #         content = self.generate_pregame_2(game_data)
            #     elif variation_type[0] in ['3', '4'] and variation_type[1] in ['A', 'B', 'C', 'D']:
            #         content = self.generate_performance_stats(
            #             game_data, 
            #             variation_type[0], 
            #             variation_type[1]
            #         )

            #     if content:
            #         self.save_text_file(match_id, variation_type, content)

        except Exception as err:
            print(f"Error processing game {match_id}: {err}")

    # [Rest of the class methods remain the same...]
    def run_continuous(self, interval=8):
        """Run the generator continuously with specified interval"""
        while True:
            try:
                print(f"\nStarting text generation cycle at {datetime.now()}")
                
                # Ensure database connection is active
                if not self.db_connection or self.db_connection.closed:
                    self.connect_to_database()
                
                self.create_texts_directory()
                
                # Read configuration
                config = self.read_config()
                if config:
                    match_id = config.get('match_id')
                    variation_type = config.get('variation_type')
                    
                    if match_id:
                        self.process_game(match_id, variation_type)
                    else:
                        print("No match_id specified in config")
                else:
                    print("No valid configuration found")
                
                print(f"Completed cycle at {datetime.now()}")
                print(f"Waiting {interval} seconds before next update...")
                time.sleep(interval)
                
            except Exception as err:
                print(f"Error in main loop: {err}")
                print("Attempting to reconnect...")
                time.sleep(5)
                
                try:
                    if self.db_connection:
                        self.db_connection.close()
                    self.connect_to_database()
                except Exception as conn_err:
                    print(f"Failed to reconnect: {conn_err}")
                    time.sleep(5)

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