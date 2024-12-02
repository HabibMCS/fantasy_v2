import json
import requests
import os
import uuid
import time
from difflib import get_close_matches
from datetime import datetime
from pregame_feed import PregameTextGenerator

class NFLGameTracker:
    def __init__(self):
        self.api_key = "b2497c1c7bmsh60f6b72e7d3d024p1bf8b1jsn5fb3791fc07d"
        self.api_host = "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
        
        # Dictionary of team names and their acronyms
        self.teams = {
            "buffalo bills": "BUF", "arizona cardinals": "ARI",
            "atlanta falcons": "ATL", "baltimore ravens": "BAL",
            "carolina panthers": "CAR", "chicago bears": "CHI",
            "cincinnati bengals": "CIN", "cleveland browns": "CLE",
            "dallas cowboys": "DAL", "denver broncos": "DEN",
            "detroit lions": "DET", "green bay packers": "GB",
            "houston texans": "HOU", "indianapolis colts": "IND",
            "jacksonville jaguars": "JAX", "kansas city chiefs": "KC",
            "las vegas raiders": "LV", "los angeles chargers": "LAC",
            "los angeles rams": "LAR", "miami dolphins": "MIA",
            "minnesota vikings": "MIN", "new england patriots": "NE",
            "new orleans saints": "NO", "new york giants": "NYG",
            "new york jets": "NYJ", "philadelphia eagles": "PHI",
            "pittsburgh steelers": "PIT", "san francisco 49ers": "SF",
            "seattle seahawks": "SEA", "tampa bay buccaneers": "TB",
            "tennessee titans": "TEN", "washington commanders": "WAS"
        }
        self.team_names = [name.lower() for name in self.teams.keys()]

    def fetch_api_data(self, endpoint, params=None):
        """Fetch data from Tank01 API with error handling"""
        try:
            url = f"https://{self.api_host}/{endpoint}"
            headers = {
                "X-RapidAPI-Key": self.api_key,
                "X-RapidAPI-Host": self.api_host
            }
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data.get('statusCode') != 200:
                print(f"API Error ({endpoint}): Status {data.get('statusCode')}")
                return None
                
            return data.get('body')
        except Exception as err:
            print(f"API Error ({endpoint}): {err}")
            return None

    def get_team_acronym(self, team_name):
        """Get team acronym with fuzzy matching"""
        team_name = team_name.lower()
        close_matches = get_close_matches(team_name, self.team_names, n=1, cutoff=0.8)
        return self.teams[close_matches[0]] if close_matches else None

    def write_to_folders(self, content, content2=None):
        """Write content to specified folders"""
        content1 = None
        folders = ["./texts/UID1000/", "./texts/UID1003/"]
        if '\n' in content:
            content1 = content.split('\n')[0]
            content2 = content.split('\n')[1]
        for folder in folders:
            os.makedirs(folder, exist_ok=True)
            guid = str(uuid.uuid4())
            file_path = os.path.join(folder, f"UNSENT_MESSAGE{guid}.txt")
            if not content1:
                final_content = content if content2 is None else f"{content}\n{content2}"
                with open(file_path, 'w') as file:
                    file.write(final_content)
            if content1:
                final_content =  f"{content1}\n{content2}"
                with open(file_path, 'w') as file:
                    file.write(final_content)

    def fetch_contest_info(self):
        """Read contest information from JSON file"""
        try:
            with open("./config/contest.json", "r") as file:
                data = json.load(file)
                return {
                    "contestid": data.get("contestid"),
                    "hometeam": data.get("hometeam"),
                    "awayteam": data.get("awayteam"),
                    "pregamestats": int(data.get("pregamestats", 0))
                }
        except Exception as err:
            print(f"Error reading contest file: {err}")
            return None

    def get_team_stats(self, team_name):
        """Get team statistics from API"""
        team_data = self.fetch_api_data("getNFLTeams")
        if not team_data:
            return None
            
        team_stats = next((team for team in team_data if team['teamName'].lower() == team_name.lower()), None)
        if team_stats:
            return (
                f"{team_stats['teamAbv']} {team_stats['wins']}-{team_stats['losses']}-{team_stats['ties']} | "
                f"WIN PCT {team_stats.get('winPercentage', '0.000')} | "
                f"PF {team_stats.get('pf', '0')} | PA {team_stats.get('pa', '0')}\n"
                f"Home: {team_stats.get('homeRecord', '0-0')} | Away: {team_stats.get('awayRecord', '0-0')} | "
                f"Div: {team_stats.get('divisionRecord', '0-0')} | Conf: {team_stats.get('conferenceRecord', '0-0')}"
            )
        return f"Team '{team_name}' not found in the standings."

    def process_final_game(self, game_data):
        """Process final game data"""
        home_team = game_data['home']
        away_team = game_data['away']
        home_score = game_data.get('homePts', '0')
        away_score = game_data.get('awayPts', '0')
        
        main_content = f"{home_team} {home_score} vs {away_team} {away_score} | Final"
        
        # Get detailed box score for quarter information
        box_score = self.fetch_api_data("getNFLBoxScore", {
            "gameID": game_data['gameID'],
            "playByPlay": "true"
        })
        
        if box_score:
            home_quarters = box_score.get('quarterDetails', {}).get('home', {})
            away_quarters = box_score.get('quarterDetails', {}).get('away', {})
            
            home_detail = (f"hometeam {home_team} "
                         f"{home_quarters.get('q1', '0')} | {home_quarters.get('q2', '0')} | "
                         f"{home_quarters.get('q3', '0')} | {home_quarters.get('q4', '0')} | "
                         f"{home_quarters.get('ot', '0')} | {home_score}")
            
            away_detail = (f"awayteam {away_team} "
                         f"{away_quarters.get('q1', '0')} | {away_quarters.get('q2', '0')} | "
                         f"{away_quarters.get('q3', '0')} | {away_quarters.get('q4', '0')} | "
                         f"{away_quarters.get('ot', '0')} | {away_score}")
            
            return main_content, f"{home_detail}\n{away_detail}"
        
        return main_content, None

    def process_live_game(self, game_data):
        """Process live game data and plays"""
        try:
            box_score = self.fetch_api_data("getNFLBoxScore", {
                "gameID": game_data['gameID'],
                "playByPlay": "true"
            })
            
            if not box_score or 'plays' not in box_score:
                return None, None
                
            plays = box_score['plays']
            if not plays:
                return None, None
                
            latest_play = plays[-1]
            
            # Format the base game status line
            status_line = (f"{game_data['home']} {game_data.get('homePts', '0')} vs "
                         f"{game_data['away']} {game_data.get('awayPts', '0')}")
            
            # Check if it's a scoring play
            if latest_play.get('scoringPlay'):
                play_description = (f"SCORING ALERT! {status_line}\n"
                                 f"{latest_play.get('description', 'No description available')}")
                
                # Add drive summary for scoring plays
                drive_summary = (f"Drive Scoring Summary\n"
                              f"{latest_play.get('driveDescription', 'Drive details unavailable')}")
                
                return play_description, drive_summary
            else:
                # Regular play
                game_clock = game_data.get('gameClock', '')
                down_distance = latest_play.get('downDistanceText', '')
                play_desc = latest_play.get('description', '')
                
                return f"{status_line}\n{game_clock} | {down_distance} {play_desc}", None
                
        except Exception as err:
            print(f"Error processing live game: {err}")
            return None, None

    def run(self):
        """Main running loop"""
        while True:
            try:
                # Fetch contest information
                contest_info = self.fetch_contest_info()
                if not contest_info:
                    time.sleep(10)
                    continue

                # Get game date from contest ID
                game_date = contest_info['contestid'].split('_')[0]

                # Fetch current game data
                games_data = self.fetch_api_data("getNFLScoresOnly", {
                    "gameDate": game_date,
                    "topPerformers": "true"
                })

                if not games_data:
                    self.write_to_folders(
                        f"| Match {contest_info['hometeam']} vs {contest_info['awayteam']} Not Found |",
                        "| Try Different Team with available upcoming matches |"
                    )
                    time.sleep(10)
                    continue

                game_data = games_data.get(contest_info['contestid'])
               # print(game_data)
                if not game_data:
                    continue

                # Process based on game status
                if game_data['gameStatus'] == "Not Started Yet":
                    gdate = game_data['gameID'].split('_')[0]
                    formatted_date = f"{gdate[6:]}.{gdate[4:6]}.{gdate[:4]}"
                    content = (f"{game_data['home']} vs {game_data['away']} | "
                             f"{formatted_date} {game_data['gameTime']} EST | {game_data['gameStatus']}")
                    self.write_to_folders(content)
                    time.sleep(8)
                    
                    # Handle pre-game stats if enabled
                    if contest_info['pregamestats']==1:
                        generator = PregameTextGenerator()
                        data = None
                        with open("./config/game_config.json","r") as file:
                            data = json.load(file)

                        overall_content = generator.process_game(match_id=data["match_id"],variation_type=data["variation_type"])
                        for i in overall_content[1:]:
                            self.write_to_folders(i)
                            time.sleep(8)


                elif game_data['gameStatus'] == "Final":
                    main_content, detail_content = self.process_final_game(game_data)
                    self.write_to_folders(main_content)
                    if detail_content and contest_info['pregamestats']:
                        self.write_to_folders(detail_content)

                else:  # Game in progress
                    play_content, drive_content = self.process_live_game(game_data)
                    if play_content:
                        self.write_to_folders(play_content)
                        if drive_content:
                            time.sleep(5)
                            self.write_to_folders(drive_content)

                time.sleep(20)

            except Exception as err:
                print(f"Error in main loop: {err}")
                time.sleep(5)

if __name__ == "__main__":
    tracker = NFLGameTracker()
    tracker.run()