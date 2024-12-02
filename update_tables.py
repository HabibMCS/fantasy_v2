import requests
import pyodbc
import os
from dotenv import load_dotenv
import time
from datetime import datetime, date

class NFLDataUpdater:
    def __init__(self):
        load_dotenv()
        self.db_connection = None
        self.cursor = None
        
        # API Configuration
        self.api_host = "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
        self.api_key = os.getenv('RAPIDAPI_KEY')
        self.headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": self.api_host
        }
        self.base_url = f"https://{self.api_host}"

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
            self.create_tables()
        except pyodbc.Error as err:
            print(f"Error connecting to database: {err}")
            raise

    def create_tables(self):
        """Create required tables"""
        try:
            # Table for live game scores
            self.cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'nfl_live_scores_v2')
                CREATE TABLE nfl_live_scores_v2 (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    game_id VARCHAR(50),
                    game_status VARCHAR(50),
                    home_team VARCHAR(10),
                    away_team VARCHAR(10),
                    home_score INT,
                    away_score INT,
                    game_date VARCHAR(20),
                    game_time VARCHAR(20),
                    quarter VARCHAR(10),
                    clock VARCHAR(20),
                    created_at DATETIME DEFAULT GETDATE()
                )
            """)

            # Table for team stats
            self.cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'nfl_team_stats_v2')
                CREATE TABLE nfl_team_stats_v2 (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    game_id VARCHAR(50),
                    team_abv VARCHAR(10),
                    total_yards INT,
                    passing_yards INT,
                    rushing_yards INT,
                    first_downs INT,
                    third_down_efficiency VARCHAR(20),
                    fourth_down_efficiency VARCHAR(20),
                    possession_time VARCHAR(20),
                    turnovers INT,
                    penalties VARCHAR(20),
                    created_at DATETIME DEFAULT GETDATE()
                )
            """)

            # Table for general team info
            self.cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'nfl_teams_v2')
                CREATE TABLE nfl_teams_v2 (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    team_abv VARCHAR(10),
                    team_name VARCHAR(100),
                    team_city VARCHAR(100),
                    conference VARCHAR(50),
                    division VARCHAR(50),
                    wins INT,
                    losses INT,
                    ties INT,
                    points_for INT,
                    points_against INT,
                    current_streak_result CHAR(1),
                    current_streak_length INT,
                    created_at DATETIME DEFAULT GETDATE()
                )
            """)

            self.db_connection.commit()
            print("Tables created successfully")
        except pyodbc.Error as err:
            print(f"Error creating tables: {err}")
            raise


    def update_live_scores(self):
        """Update live game scores"""
        try:
            today = date.today().strftime("%Y%m%d")
            scores = self.fetch_api_data("getNFLScoresOnly", {"gameDate": today})
            
            if not scores or scores.get('statusCode') != 200:
                print("No live scores data available")
                return
            
            # Clear old scores
            self.cursor.execute("DELETE FROM nfl_live_scores_v2 WHERE game_date = ?", (today,))
            
            scores_data = scores.get('body', [])
            if not isinstance(scores_data, list):
                print("No games data available")
                return
                
            # Insert new scores
            for game in scores_data:
                try:
                    query = """
                        INSERT INTO nfl_live_scores_v2 (
                            game_id, game_status, home_team, away_team,
                            home_score, away_score, game_date, game_time,
                            quarter, clock
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    
                    # Extract line score data
                    line_score = game.get('lineScore', {})
                    home_score = line_score.get('home', {}).get('totalPts', 0)
                    away_score = line_score.get('away', {}).get('totalPts', 0)
                    
                    values = (
                        game.get('gameID', ''),
                        game.get('gameStatus', ''),
                        game.get('home', ''),
                        game.get('away', ''),
                        int(home_score) if home_score else 0,
                        int(away_score) if away_score else 0,
                        game.get('gameDate', ''),
                        game.get('gameTime', ''),
                        line_score.get('period', ''),
                        line_score.get('gameClock', '')
                    )
                    self.cursor.execute(query, values)
                except (KeyError, ValueError) as e:
                    print(f"Error processing game data: {e}")
                    continue
            
            self.db_connection.commit()
            print(f"Updated live scores for {today}")
        except Exception as err:
            print(f"Error updating live scores: {err}")
            self.db_connection.rollback()

    def update_team_stats(self):
        """Update team statistics"""
        try:
            today = date.today().strftime("%Y%m%d")
            games = self.fetch_api_data("getNFLScoresOnly", {"gameDate": today})
            
            if not games or not games.get('body'):
                print("No games data available")
                return
            
            games_data = games['body']
            
            # Handle case where games are in an object instead of array
            if isinstance(games_data, dict):
                games_list = []
                for game_id, game_info in games_data.items():
                    game_info['gameID'] = game_id
                    games_list.append(game_info)
            else:
                games_list = games_data
                
            self.cursor.execute("DELETE FROM nfl_team_stats_v2 WHERE game_id LIKE ?", (f"{today}%",))
            
            for game in games_list:
                try:
                    game_id = game.get('gameID')
                    if not game_id:
                        continue
                        
                    box_score = self.fetch_api_data("getNFLBoxScore", {"gameID": game_id})
                    print(f"Box score for {game_id}:", box_score)  # Debug print
                    
                    if not box_score or box_score.get('statusCode') != 200:
                        continue
                    
                    team_stats = box_score.get('body', {}).get('teamStats', {})
                    if not team_stats:
                        continue
                    
                    for team_type in ['home', 'away']:
                        stats = team_stats.get(team_type, {})
                        if not stats:
                            continue
                            
                        query = """
                            INSERT INTO nfl_team_stats_v2 (
                                game_id, team_abv, total_yards, passing_yards,
                                rushing_yards, first_downs, third_down_efficiency,
                                fourth_down_efficiency, possession_time, turnovers,
                                penalties
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """
                        
                        values = (
                            game_id,
                            stats.get('team', ''),
                            self.safe_int(stats.get('totalYards')),
                            self.safe_int(stats.get('passingYards')),
                            self.safe_int(stats.get('rushingYards')),
                            self.safe_int(stats.get('firstDowns')),
                            stats.get('thirdDownEfficiency', ''),
                            stats.get('fourthDownEfficiency', ''),
                            stats.get('possession', ''),
                            self.safe_int(stats.get('turnovers')),
                            stats.get('penalties', '')
                        )
                        self.cursor.execute(query, values)
                        
                except Exception as e:
                    print(f"Error processing game {game_id}: {e}")
                    continue
            
            self.db_connection.commit()
            print(f"Updated team stats for {today}")
            
        except Exception as err:
            print(f"Error updating team stats: {err}")
            self.db_connection.rollback()
    def safe_int(self, value, default=0):
        """Safely convert value to integer"""
        try:
            return int(str(value).strip()) if value else default
        except (ValueError, TypeError):
            return default

    def fetch_api_data(self, endpoint, params=None):
        """Fetch data from API with better error handling"""
        try:
            url = f"{self.base_url}/{endpoint}"
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as err:
            print(f"API Error ({endpoint}): {err}")
            return None
        except ValueError as err:
            print(f"JSON parsing error ({endpoint}): {err}")
            return None
    def update_team_info(self):
        """Update general team information"""
        try:
            teams = self.fetch_api_data("getNFLTeams")
            
            if not teams or teams.get('statusCode') != 200:
                print("No teams data available")
                return
            
            teams_data = teams.get('body', [])
            if not isinstance(teams_data, list):
                print("Invalid teams data format")
                return
            
            # Clear old team info
            self.cursor.execute("DELETE FROM nfl_teams_v2")
            
            # Insert new team info
            for team in teams_data:
                try:
                    query = """
                        INSERT INTO nfl_teams_v2 (
                            team_abv, team_name, team_city, conference,
                            division, wins, losses, ties, points_for,
                            points_against, current_streak_result,
                            current_streak_length
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    
                    streak = team.get('currentStreak', {})
                    values = (
                        team.get('teamAbv', ''),
                        team.get('teamName', ''),
                        team.get('teamCity', ''),
                        team.get('conference', ''),
                        team.get('division', ''),
                        int(team.get('wins', '0')),
                        int(team.get('loss', '0')),
                        int(team.get('tie', '0')),
                        int(team.get('pf', '0')),
                        int(team.get('pa', '0')),
                        streak.get('result', ''),
                        int(streak.get('length', '0'))
                    )
                    self.cursor.execute(query, values)
                except (KeyError, ValueError) as e:
                    print(f"Error processing team info: {e}")
                    continue
            
            self.db_connection.commit()
            print("Updated team information")
        except Exception as err:
            print(f"Error updating team info: {err}")
            self.db_connection.rollback()

    def run_continuous(self, interval=30):
        """Run continuous updates"""
        while True:
            try:
                print(f"\nStarting update cycle at {datetime.now()}")
                
               # self.update_live_scores()
                self.update_team_stats()
                self.update_team_info()
                
                print(f"Completed cycle at {datetime.now()}")
                print(f"Waiting {interval} seconds...")
                time.sleep(interval)
                
            except Exception as err:
                print(f"Error in main loop: {err}")
                time.sleep(5)

def main():
    updater = NFLDataUpdater()
    try:
        updater.connect_to_database()
        updater.run_continuous()
    except KeyboardInterrupt:
        print("\nStopping updater...")
    except Exception as err:
        print(f"Application error: {err}")
    finally:
        if updater.db_connection:
            updater.db_connection.close()

if __name__ == "__main__":
    main()