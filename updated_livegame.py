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
        self.api_key = "fa1b0611e4mshe50b811ebe2d167p19cc57jsndc0bfbbbbed7"
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
            print(data)
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
        folders = ["../../UID1000/", "../../UID1003/"]
        # if '\n' in content:
        #     content1 = content.split('\n')[0]
        #     content2 = content.split('\n')[1]
        for folder in folders:
            os.makedirs(folder, exist_ok=True)
            guid = str(uuid.uuid4())
            file_path = os.path.join(folder, f"UNSENT_MESSAGE{guid}.txt")
            if not content2:
                final_content = content
                with open(file_path, 'w') as file:
                    file.write(final_content)
            if content2:
                final_content =  f"{content1}\n{content2}"
                with open(file_path, 'w') as file:
                    file.write(final_content)

    def fetch_contest_info(self):
        """Read contest information from JSON file"""
        try:
            with open("../../simpleapp/contest.json", "r") as file:
                data = json.load(file)
                return {
                    "contestid": data.get("contestid"),
                    "hometeam": data.get("hometeam"),
                    "awayteam": data.get("awayteam"),
                    "pregamestats": int(data.get("pregamestats", 0)),
                    "gametime":data.get("gametime")
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
    def compare_responses(self,previous_response, new_response):
        # Convert both responses to strings for comparison
        import json
        
        prev_json = json.dumps(previous_response, sort_keys=True)
        new_json = json.dumps(new_response, sort_keys=True)
        
        # Compare and set flag
        is_equal = prev_json == new_json
        
        # Return False if equal, as per requirement
        return not is_equal
    def process_live_game(self, game_data,prev_scplay):
        """Process live game data and plays"""
        scFlag = False
        try:
            if 'scoringPlays' in game_data:
                scplay = game_data['scoringPlays']
                scflag = self.compare_responses(prev_scplay , scplay[0])
                if scflag:
                    prev_scplay = scplay[0]
                    
            if 'allPlayByPlay' in game_data:
                plays = game_data['allPlayByPlay']
                if not plays:
                    return None, None
                
                latest_play = plays[0]  # Get most recent play (first in the list)
                
                # Get current score and game info
                home_score = game_data.get('homePts', '0')
                away_score = game_data.get('awayPts', '0')
                status_line = f"{game_data['home']} {home_score} vs {game_data['away']} {away_score}"
                
                # Check if there's a play
                if latest_play:
                    # Get play details
                    play_clock = latest_play.get('playClock', '')
                    play_period = latest_play.get('playPeriod', '')
                    down_distance = latest_play.get('downAndDistance', '')
                    play_desc = latest_play.get('play', '')
                    
                    play_content = f"{status_line} {down_distance}\n{play_clock}-{play_period} | {down_distance} {play_desc}"
                    # Check if it's a scoring play (you'll need to implement scoring play detection)
                    if scflag:
                        scFlag = False
                        return f"SCORING SUMMARY    \n{prev_scplay['scoreType']}-{prev_scplay['score']} ", f"DRIVE SCORING SUMMARY\n{prev_scplay['team']} ({prev_scplay['scorePeriod']}-{prev_scplay['scoreTime']} | {prev_scplay['scoreType']} {prev_scplay['scoreDetails']}) ", prev_scplay
                    else:
                        return play_content, None,prev_scplay
                        
            return None, None, prev_scplay
                    
        except Exception as err:
            print(f"Error processing live game: {err}")
            return None, None,scplay

    def run(self):
        generator = PregameTextGenerator()
        """Main running loop"""
        prev_scplay=None
        while True:
            try:
                # Fetch contest information
                contest_info = self.fetch_contest_info()
                if not contest_info:
                    time.sleep(10)
                    continue
                contest_id = contest_info['contestid']
                # Get current game data
                game_data = self.fetch_api_data("getNFLBoxScore", {
                    "gameID": contest_info['contestid'],
                    "playByPlay": "true"
                })

                if not game_data:
                    self.write_to_folders(
                        f"| Match {contest_info['hometeam']} vs {contest_info['awayteam']} API SYSTEM ERROR |",
                        "| API ERROR  API ERROR|"
                    )
                    time.sleep(10)
                    continue
                if "Game hasn't started" in game_data['error']:
                # if game_data['gameStatus'] == "Not Started Yet":
                    gdate = contest_info['contestid'].split('_')[0]
                    formatted_date = f"{gdate[6:]}.{gdate[4:6]}.{gdate[:4]}"
                    content = (f"{contest_info['hometeam']} vs {contest_info['awayteam']} | "
                            f"{formatted_date} {contest_info['gametime']} EST | Not Started Yet")
                    self.write_to_folders(content)
                    time.sleep(10)
                    generator.connect_to_database()
                    pregame_texts = generator.process_game(match_id=contest_info['contestid'])
                    for i in pregame_texts:
                        self.write_to_folders(content=i)
                        if contest_id != self.fetch_contest_info()['contestid']:
                            break
                        time.sleep(10)
                    continue
                    
                elif game_data['gameStatus'] == "Completed" :
                    home_score = game_data.get('homePts', '0')
                    away_score = game_data.get('awayPts', '0')
                    content = f"{game_data['home']} {home_score} vs {game_data['away']} {away_score} | Final"
                    self.write_to_folders(content)

                else:  # Game in progress
                    play_content, drive_content,prev_scplay= self.process_live_game(game_data,prev_scplay)
                    if play_content:
                        self.write_to_folders(play_content)
                        if drive_content:
                            time.sleep(5)
                            self.write_to_folders(drive_content)

                time.sleep(10)

            except Exception as err:
                print(f"Error in main loop: {err}")
                time.sleep(5)
            finally:
                if generator.db_connection:
                    generator.db_connection.close()
                    print("Database connection closed")
                    break
if __name__ == "__main__":
    tracker = NFLGameTracker()
    tracker.run()