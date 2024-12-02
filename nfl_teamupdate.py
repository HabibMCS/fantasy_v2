import requests
import pyodbc
import os
from dotenv import load_dotenv
import time
from datetime import datetime

class NFLTeamsUpdater:
    def __init__(self):
        self.api_url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLTeams"
        self.headers = {
            "X-RapidAPI-Key": "b2497c1c7bmsh60f6b72e7d3d024p1bf8b1jsn5fb3791fc07d",
            "X-RapidAPI-Host": "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
        }
        self.db_connection = None
        self.cursor = None

    def connect_to_database(self):
        """Establish connection to SQL Server database"""
        try:
            load_dotenv()
            connection_string = (
                "Driver={ODBC Driver 18 for SQL Server};"
                "Server=tcp:34.237.12.44,1433;"
                "Database=ds-cbssports;"
                "UID=%s;"
                "PWD=%s;"
                "TrustServerCertificate=yes;"
                "Encrypt=yes;"
            )% (os.getenv('ODBC_DB_USER'), os.getenv('ODBC_DB_PASS'))
            self.db_connection = pyodbc.connect(connection_string)
            self.cursor = self.db_connection.cursor()
            self.create_table()
            print("Database connection established successfully")
        except pyodbc.Error as err:
            print(f"Error connecting to database: {err}")
            raise

    def create_table(self):
        """Create table if it doesn't exist"""
        try:
            self.cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'nfl_teams_updated')
                CREATE TABLE nfl_teams_updated (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    team_id VARCHAR(10),
                    team_abv VARCHAR(10),
                    team_city VARCHAR(50),
                    team_name VARCHAR(50),
                    conference VARCHAR(50),
                    conference_abv VARCHAR(10),
                    division VARCHAR(20),
                    wins INT,
                    loss INT,
                    tie INT,
                    points_for INT,
                    points_against INT,
                    streak_result CHAR(1),
                    streak_length INT,
                    current_bye_week VARCHAR(10),
                    nfl_logo_url VARCHAR(255),
                    espn_logo_url VARCHAR(255),
                    updated_at DATETIME DEFAULT GETDATE()
                )
            """)
            self.db_connection.commit()
            print("Table created/verified successfully")
        except pyodbc.Error as err:
            print(f"Error creating table: {err}")
            raise

    def fetch_teams_data(self):
        """Fetch teams data from API"""
        try:
            response = requests.get(self.api_url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as err:
            print(f"Error fetching data from API: {err}")
            return None

    def update_teams_data(self, teams_data):
        """Update teams data in database"""
        try:
            # Clear existing data
            self.cursor.execute("DELETE FROM nfl_teams_updated")
            
            for team in teams_data['body']:
                query = """
                    INSERT INTO nfl_teams_updated (
                        team_id, team_abv, team_city, team_name, conference,
                        conference_abv, division, wins, loss, tie,
                        points_for, points_against, streak_result, streak_length,
                        current_bye_week, nfl_logo_url, espn_logo_url
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                values = (
                    team['teamID'],
                    team['teamAbv'],
                    team['teamCity'],
                    team['teamName'],
                    team['conference'],
                    team['conferenceAbv'],
                    team['division'],
                    int(team['wins']),
                    int(team['loss']),
                    int(team['tie']),
                    int(team['pf']),
                    int(team['pa']),
                    team['currentStreak']['result'],
                    int(team['currentStreak']['length']),
                    team['byeWeeks']['2024'][0],
                    team['nflComLogo1'],
                    team['espnLogo1']
                )
                
                self.cursor.execute(query, values)
            
            self.db_connection.commit()
            print(f"Successfully updated teams data at {datetime.now()}")
            
        except pyodbc.Error as err:
            print(f"Error updating teams data: {err}")
            self.db_connection.rollback()
            raise

    def process_teams(self):
        """Main process to fetch and update teams data"""
        try:
            data = self.fetch_teams_data()
            if data and data.get('statusCode') == 200:
                self.update_teams_data(data)
            else:
                print("Failed to fetch valid data from API")
        except Exception as err:
            print(f"Error processing teams: {err}")
        finally:
            if self.db_connection:
                self.db_connection.close()
                print("\nDatabase connection closed")

def main():
    updater = NFLTeamsUpdater()
    try:
        updater.connect_to_database()
        updater.process_teams()
    except Exception as err:
        print(f"Application error: {err}")

if __name__ == "__main__":
    main()