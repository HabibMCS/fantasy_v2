import requests
import pyodbc
import time
from datetime import datetime
import json
import os
from dotenv import load_dotenv

class NFLDataCollector:
    def __init__(self):
        self.api_url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLGamesForWeek"
        self.headers = {
            "X-RapidAPI-Key": "b2497c1c7bmsh60f6b72e7d3d024p1bf8b1jsn5fb3791fc07d",  # Replace with your RapidAPI key
            "X-RapidAPI-Host": "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
        }
        # Load environment variables
        load_dotenv()
        self.db_connection = None
        self.cursor = None

    def connect_to_database(self):
        """Establish connection to SQL Server database using environment variables"""
        try:
            # Use a more compatible connection string
            connection_string = (
                "Driver={ODBC Driver 18 for SQL Server};"
                "Server=tcp:34.237.12.44,1433;"
                "Database=ds-cbssports;"
                "UID=%s;"
                "PWD=%s;"
                "TrustServerCertificate=yes;"
                "Encrypt=yes;"
            ) % (os.getenv('ODBC_DB_USER'), os.getenv('ODBC_DB_PASS'))
            
            print("Attempting to connect with string:", connection_string)  # For debugging
            
            self.db_connection = pyodbc.connect(connection_string)
            self.cursor = self.db_connection.cursor()
            self.create_table()
            print("Database connection established successfully")
        except pyodbc.Error as err:
            print(f"Error connecting to database: {err}")
            # Print more detailed error information
            print(f"Error state: {err.args[0]}")
            print(f"Error message: {err.args[1] if len(err.args) > 1 else 'No additional message'}")
            raise
    
    def create_table(self):
        """Create table if it doesn't exist"""
        try:
            self.cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'nfl_games')
                CREATE TABLE nfl_games (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    game_id VARCHAR(50),
                    season_type VARCHAR(20),
                    away_team VARCHAR(10),
                    game_date VARCHAR(20),
                    espn_id VARCHAR(20),
                    team_id_home VARCHAR(10),
                    game_status VARCHAR(20),
                    game_week VARCHAR(20),
                    team_id_away VARCHAR(10),
                    home_team VARCHAR(10),
                    espn_link VARCHAR(255),
                    cbs_link VARCHAR(255),
                    game_time VARCHAR(20),
                    game_time_epoch FLOAT,
                    season VARCHAR(10),
                    neutral_site VARCHAR(10),
                    game_status_code VARCHAR(5),
                    created_at DATETIME DEFAULT GETDATE()
                )
            """)
            self.db_connection.commit()
            print("Table created/verified successfully")
        except pyodbc.Error as err:
            print(f"Error creating table: {err}")
            raise

    def fetch_games_data(self, week, season_type='reg', season='2024'):
        """Fetch games data from API for a specific week"""
        try:
            querystring = {
                "week": str(week),
                "seasonType": season_type,
                "season": str(season)
            }
            
            response = requests.get(
                self.api_url,
                headers=self.headers,
                params=querystring
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as err:
            print(f"Error fetching data for week {week}: {err}")
            return None

    def insert_game_data(self, game):
        """Insert single game data into database"""
        try:
            query = """
                IF NOT EXISTS (SELECT 1 FROM nfl_games WHERE game_id = ? AND game_week = ?)
                INSERT INTO nfl_games (
                    game_id, season_type, away_team, game_date, espn_id,
                    team_id_home, game_status, game_week, team_id_away,
                    home_team, espn_link, cbs_link, game_time,
                    game_time_epoch, season, neutral_site, game_status_code
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            values = (
                game['gameID'], game['gameWeek'],  # For the EXISTS check
                game['gameID'], game['seasonType'], game['away'],
                game['gameDate'], game.get('espnID', ''),
                game['teamIDHome'], game['gameStatus'], game['gameWeek'],
                game['teamIDAway'], game['home'], game.get('espnLink', ''),
                game.get('cbsLink', ''), game.get('gameTime', ''),
                float(game.get('gameTime_epoch', 0)), game.get('season', ''),
                game.get('neutralSite', 'False'), game.get('gameStatusCode', '')
            )
            
            self.cursor.execute(query, values)
            self.db_connection.commit()
            print(f"Inserted data for game {game['gameID']}")
        except pyodbc.Error as err:
            print(f"Error inserting game data: {err}")
            self.db_connection.rollback()

    def process_all_weeks(self, start_week=13, end_week=18):
        """Process all weeks in the regular season"""
        try:
            for week in range(start_week, end_week + 1):
                print(f"\nProcessing Week {week}")
                data = self.fetch_games_data(week)
                
                if data and data.get('statusCode') == 200 and 'body' in data:
                    games = data['body']
                    for game in games:
                        self.insert_game_data(game)
                
                # Add a small delay between weeks to avoid hitting API rate limits
                time.sleep(1)
                
        except Exception as err:
            print(f"Error processing weeks: {err}")
        finally:
            if self.db_connection:
                self.db_connection.close()
                print("\nDatabase connection closed")

def main():
    collector = NFLDataCollector()
    try:
        collector.connect_to_database()
        collector.process_all_weeks()
    except Exception as err:
        print(f"Application error: {err}")

if __name__ == "__main__":
    main()