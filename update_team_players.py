import os
import pyodbc
import requests
from dotenv import load_dotenv
from datetime import datetime

class NFLPlayerDataFetcher:
    def __init__(self):
        load_dotenv()
        self.connection = None
        self.cursor = None
        self.api_key = os.getenv('RAPIDAPI_KEY')
        self.api_host = "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"

    def fetch_player_data(self):
        """Fetch player data from the API"""
        url = f"https://{self.api_host}/getNFLPlayerList"
        headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": self.api_host
        }

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()

            if data.get('statusCode') == 200:
                return data.get('body', [])
            else:
                print(f"API Error: Status {data.get('statusCode')}")
                return None
        except Exception as err:
            print(f"Error fetching player data: {err}")
            return None

    def connect_to_database(self):
        """Connect to the NFLv2 database"""
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

            self.connection = pyodbc.connect(connection_string)
            self.cursor = self.connection.cursor()
            print("Connected to database successfully")
        except pyodbc.Error as err:
            print(f"Error connecting to database: {err}")
            raise

    def process_player(self, player_data):
        try:
            is_free_agent = 1 if str(player_data.get('isFreeAgent', '')).upper() == 'TRUE' else 0

            # Handle birth date parsing
            bday = None
            if player_data.get('bDay'):
                try:
                    parts = player_data['bDay'].split('/')
                    if len(parts) == 3:
                        month, day, year = parts
                        bday = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                except:
                    bday = None

            injury = player_data.get('injury', {})

            # Check for duplicates before insertion
            sql_check = "SELECT COUNT(*) FROM player_info WHERE playerID = ?"
            self.cursor.execute(sql_check, (str(player_data.get('playerID', '')),))
            exists = self.cursor.fetchone()[0]

            if exists:
                print(f"Player {player_data.get('longName', 'Unknown')} already exists. Skipping...")
                return (True, 'exist')  # Duplicates are not considered "failed"

            # Adjust SQL and arguments to match column count
            sql = """
                INSERT INTO player_info (
                    playerID, espnID, cbsPlayerID, yahooPlayerID, sleeperBotID, 
                    rotoWirePlayerID, fRefID, longName, cbsLongName, cbsShortName,
                    espnName, team, teamID, isFreeAgent, lastGamePlayed, pos,
                    jerseyNum, height, weight, age, bDay, exp, espnLink,
                    yahooLink, espnHeadshot, espnIDFull, cbsPlayerIDFull,
                    rotoWirePlayerIDFull, injuryDescription, injuryDesignation
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                        ?, ?, ?, ?, ?, ?, ?, ?, ?,?)
            """

            self.cursor.execute(sql, (
                str(player_data.get('playerID', '')),
                str(player_data.get('espnID', '')),
                str(player_data.get('cbsPlayerID', '')),
                str(player_data.get('yahooPlayerID', '')),
                str(player_data.get('sleeperBotID', '')),
                str(player_data.get('rotoWirePlayerID', '')),
                str(player_data.get('fRefID', '')),
                str(player_data.get('longName', '')),
                str(player_data.get('cbsLongName', '')),
                str(player_data.get('cbsShortName', '')),
                str(player_data.get('espnName', '')),
                str(player_data.get('team', '')),
                str(player_data.get('teamID', '')),
                is_free_agent,
                str(player_data.get('lastGamePlayed', '')),
                str(player_data.get('pos', '')),
                str(player_data.get('jerseyNum', '')),
                str(player_data.get('height', '')),
                str(player_data.get('weight', '')),
                int(player_data.get('age', 0)),
                bday,  # This will be NULL if parsing failed
                str(player_data.get('exp', '')),
                str(player_data.get('espnLink', '')),
                str(player_data.get('yahooLink', '')),
                str(player_data.get('espnHeadshot', '')),
                str(player_data.get('espnIDFull', '')),
                str(player_data.get('cbsPlayerIDFull', '')),
                str(player_data.get('rotoWirePlayerIDFull', '')),
                str(injury.get('description', '')),
                str(injury.get('designation', ''))
            ))
            self.connection.commit()
            return (True, None)
        except Exception as err:
            print(f"Error processing player {player_data.get('longName', 'Unknown')} (ID: {player_data.get('playerID')}): {err}")
            return (False, player_data.get(player_data.get('playerID')))

    def create_players_table(self):
        try:
            self.cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'player_info')
                BEGIN
                    CREATE TABLE player_info (
                        playerID VARCHAR(20) PRIMARY KEY,
                        espnID VARCHAR(20),
                        cbsPlayerID VARCHAR(20),
                        yahooPlayerID VARCHAR(20),
                        sleeperBotID VARCHAR(20),
                        rotoWirePlayerID VARCHAR(20),
                        fRefID VARCHAR(20),
                        longName VARCHAR(100),
                        cbsLongName VARCHAR(100),
                        cbsShortName VARCHAR(20),
                        espnName VARCHAR(100),
                        team VARCHAR(5),
                        teamID VARCHAR(10),
                        isFreeAgent BIT,
                        lastGamePlayed VARCHAR(50),
                        pos VARCHAR(5),
                        jerseyNum VARCHAR(5),
                        height VARCHAR(10),
                        weight VARCHAR(10),
                        age INT,
                        bDay DATE,
                        school VARCHAR(100),
                        exp VARCHAR(5),
                        espnLink VARCHAR(255),
                        yahooLink VARCHAR(255),
                        espnHeadshot VARCHAR(255),
                        espnIDFull VARCHAR(255),
                        cbsPlayerIDFull VARCHAR(255),
                        rotoWirePlayerIDFull VARCHAR(255),
                        injuryDescription VARCHAR(255),
                        injuryDesignation VARCHAR(50),
                        injuryDate DATE,
                        injuryReturnDate DATE,
                        created_at DATETIME DEFAULT GETDATE(),
                        updated_at DATETIME DEFAULT GETDATE()
                    )
                    CREATE INDEX idx_player_team ON player_info(team);
                    CREATE INDEX idx_player_name ON player_info(longName, cbsLongName);
                    CREATE INDEX idx_player_pos ON player_info(pos);
                END
            """)
            self.connection.commit()
            print("Player_info table created successfully")
        except pyodbc.Error as err:
            print(f"Error creating player_info table: {err}")
            raise
def retry_failed_players_by_id(fetcher, failed_players_file):
    """Retry uploading players from the failed players list containing player IDs."""
    retry_failed = []
    try:
        if os.path.exists(failed_players_file):
            print(f"Retrying players from {failed_players_file}...")
            with open(failed_players_file, 'r') as file:
                player_ids = [line.strip() for line in file.readlines() if line.strip()]
            print(player_ids)
            for player_id in player_ids:
                # Fetch the player data from the API using the player ID
                player_data = fetch_player_by_id(fetcher, player_id)
                if player_data:
                    stat, _ = fetcher.process_player(player_data)
                    if not stat:
                        retry_failed.append(player_id)
                else:
                    print(f"Player data not found for ID {player_id}")
                    retry_failed.append(player_id)

            if retry_failed:
                print(f"{len(retry_failed)} players still failed after retry")
            else:
                print("All previously failed players processed successfully")
        else:
            print(f"No file found at {failed_players_file}. Skipping retries.")
    except Exception as err:
        print(f"Error retrying failed players: {err}")
        retry_failed = []  # Reset in case of an error

    return retry_failed



def fetch_player_by_id(fetcher, player_id):
    """Fetch a single player's data by ID from the API."""
    try:
        url = f"https://{fetcher.api_host}/getNFLPlayerInfo"
        headers = {
            "X-RapidAPI-Key": fetcher.api_key,
            "X-RapidAPI-Host": fetcher.api_host
        }
        params = {"playerID": player_id,"getStats":True}

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        data = response.json()
        if data.get('statusCode') == 200:
            return data.get('body')
        else:
            print(f"API Error: Status {data.get('statusCode')} for Player ID {player_id}")
            return None
    except Exception as err:
        print(f"Error fetching player {player_id}: {err}")
        return None

# def main():
#     fetcher = NFLPlayerDataFetcher()
#     failed_players = []

#     try:
#         # Connect to database
#         fetcher.connect_to_database()
#         fetcher.create_players_table()

#         # Fetch player data from API
#         print("Fetching player data from API...")
#         players = fetcher.fetch_player_data()

#         if not players:
#             print("No player data received from API")
#             return

#         # Process each player
#         total_players = len(players)
#         successful = 0
#         failed = 0

#         print(f"Processing {total_players} players...")

#         for i, player in enumerate(players, 1):
#             stat, val = fetcher.process_player(player)
#             if stat:
#                 successful += 1
#             else:
#                 failed += 1
#                 failed_players.append(val)

#             # Progress update every 100 players
#             if i % 100 == 0:
#                 print(f"Processed {i}/{total_players} players...")

#         print(f"\nProcessing complete:")
#         print(f"Total players: {total_players}")
#         print(f"Successfully processed: {successful}")
#         print(f"Failed to process: {failed}")

#     except Exception as err:
#         print(f"Error in main process: {err}")
#     finally:
#         if fetcher.connection:
#             fetcher.connection.close()
#             print("Database connection closed")

#         with open("failed_players.txt", 'w+') as file:
#             for player in failed_players:
#                 file.write(player + "\n")

# if __name__ == "__main__":
#     main()




def main():
    fetcher = NFLPlayerDataFetcher()
    failed_players_file = "failed_players.txt"
    failed_players = []

    try:
        # Connect to database
        fetcher.connect_to_database()
        # fetcher.create_players_table()

        # # Fetch player data from API
        # print("Fetching player data from API...")
        # players = fetcher.fetch_player_data()

        # if not players:
        #     print("No player data received from API")
        #     return

        # # Process each player
        # total_players = len(players)
        # successful = 0
        # failed = 0

        # print(f"Processing {total_players} players...")

        # for i, player in enumerate(players, 1):
        #     stat, val = fetcher.process_player(player)
        #     if stat:
        #         successful += 1
        #     else:
        #         failed += 1
        #         failed_players.append(player.get('playerID') or "Unknown Player")

        #     # Progress update every 100 players
        #     if i % 100 == 0:
        #         print(f"Processed {i}/{total_players} players...")

        # print(f"\nProcessing complete:")
        # print(f"Total players: {total_players}")
        # print(f"Successfully processed: {successful}")
        # print(f"Failed to process: {failed}")

        # Write failed player IDs to a file
        # with open(failed_players_file, 'w+') as file:
        #     for player_id in failed_players:
        #         file.write(player_id + "\n")

        # Retry processing failed players by ID
        retry_failed = retry_failed_players_by_id(fetcher, failed_players_file)

        # Write remaining failed players to a file
        if retry_failed:
            with open(failed_players_file, 'w+') as file:
                for player_id in retry_failed:
                    file.write(player_id + "\n")
        else:
            # Remove the file if no failures remain
            os.remove(failed_players_file)
            print("All players processed successfully. Removed failed_players.txt.")
    except Exception as err:
        print(f"Error in main process: {err}")
    finally:
        if fetcher.connection:
            fetcher.connection.close()
            print("Database connection closed")


if __name__ == "__main__":
    main()