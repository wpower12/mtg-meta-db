from mtgMetaDB import mapper_registry
from sqlalchemy_utils import database_exists, drop_database, create_database
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, insert
from decouple import config

from mtgMetaDB import player_table, deck_table, game_table, games_played_table, league_table

from mtgMetaDB.queries import player_summary, player_deck_summary
from mtgMetaDB.queries import deck_leaderboard_overall, user_leaderboard_per_league, user_leaderboard_overall

user   = config('DB_USER')
pw     = config('DB_PASSWORD')
schema = config('DB_TEST_SCHEMA_NAME')
engine_url = f"mysql+mysqlconnector://{user}:{pw}@localhost/{schema}"

print("checking if db exists")
if database_exists(engine_url):
    print("it does, dropping it.")
    drop_database(engine_url)

print("creating db.")
create_database(engine_url)
engine = create_engine(engine_url)
mapper_registry.metadata.create_all(engine)

players = ["Apawl", "Bill", "Chuck", "Drnick", "Enathan", "Greg", "Fannie"]

decks = [
    ["AUNTIE", "Auntie Blyth, Bad Influence", "R", "Red pain.", "Bill"],
    ["KUMENA", "Kumena, Tyrant of Orzaca", "UG", "Mermaids and simic shit.", "Bill"],
    ["BROWN", "Radaghast", "G", "Craterhoof into creatures.", "Chuck"],
    ["KRENKO", "Krenko, Mob Boss", "R", "Goblins.", "Apawl"],
    ["SAURON", "Sauron, Dark Lord", "RBU", "Orcs.", "Drnick"],
    ["FLICKER", "That weird girl", "WUB", "Flicker stuff.", "Enathan"],
    ["CEDH-R", "Krenko, Mob Boss", "R", "cedh proxy deck.", "Chuck"],
    ["CEDH-U", "Ocatvia, Living Thesis", "U", "cedh proxy deck.", "Greg"],
    ["CEDH-W", "Thalia", "W", "cedh proxy deck.", "Bill"],
    ["CEDH-G", "Radaghast", "G", "cedh red proxy deck.", "Fannie"]
]

leagues = [
    ["cedh-2023-fall", "2023-01-01", "First cedh league. Yay."],
    ["comm-2023-fall", "2023-01-01", "Typical, KTM wtf you want league but not cedh plz, magic."],
]

games = [
    ["2023-01-01", "comm-2023-fall", "fun stuff happened.", [
        ["Bill",   "AUNTIE", 1, 3, "win and a 2 kills."],
        ["Chuck",  "BROWN",  2, 1, "1 kill"],
        ["Apawl",  "KRENKO", 3, 0, ""],
        ["Drnick", "SAURON", 4, 1, "played a 4+ cmc spell turn 1."]]],
    ["2023-01-02", "comm-2023-fall", "fun stuff happened again.", [
        ["Bill",   "AUNTIE", 2, 0, ""],
        ["Chuck",  "BROWN",  1, 5, "win, 3 kills. attacked with 20+ creatures."],
        ["Apawl",  "KRENKO", 3, 0, ""],
        ["Drnick", "SAURON", 4, 0, ""]]],
    ["2023-01-01", "comm-2023-fall", "fun stuff happened.", [
        ["Bill",    "KUMENA",  3, 0, ""],
        ["Chuck",   "BROWN",   2, 2, "2 kills"],
        ["Apawl",   "KRENKO",  1, 2, "win, 1 kill"],
        ["Enathan", "FLICKER", 4, 0, ""]]],
    ["2023-01-01", "comm-2023-fall", "fun stuff happened.", [
        ["Enathan", "FLICKER", 1, 4, "win, 3 kills"],
        ["Chuck",   "BROWN",   2, 0, ""],
        ["Apawl",   "KRENKO",  3, 0, ""],
        ["Drnick",  "SAURON",  4, 0, ""]]],
    ["2023-01-01", "comm-2023-fall", "fun stuff happened.", [
        ["Bill",    "AUNTIE",  2, 0, ""],
        ["Chuck",   "BROWN",   1, 4, "win, 3 kills"],
        ["Enathan", "FLICKER", 3, 0, ""],
        ["Drnick",  "SAURON",  4, 0, ""]]],
    ["2023-01-05", "cedh-2023-fall", "fun stuff happened.", [
        ["Bill",   "CEDH-W", 2, 1, "1 kill"],
        ["Chuck",  "CEDH-R", 1, 2, "win, 1 kill"],
        ["Greg",   "CEDH-U", 3, 1, "1 kill"],
        ["Fannie", "CEDH-G", 4, 0, ""]]],
    ["2023-01-05", "cedh-2023-fall", "fun stuff happened.", [
        ["Bill",   "CEDH-W", 1, 2, "win, 1 kill"],
        ["Chuck",  "CEDH-R", 2, 2, "2 kills"],
        ["Greg",   "CEDH-U", 4, 0, ""],
        ["Fannie", "CEDH-G", 3, 0, ""]]]
]

print("populating tables")
print(f"\tplayers")
with Session(engine) as session, session.begin():
    stmt = insert(player_table).prefix_with('IGNORE')
    for player in players:
        session.execute(stmt, {
            'idplayer': player
        })

print(f"\tdecks")
with Session(engine) as session, session.begin():
    stmt = insert(deck_table)
    for iddeck, comm, color, desc, creator in decks:
        session.execute(stmt, {
            'iddeck': iddeck,
            'creator': creator,
            'commander': comm,
            'color': color,
            'desc': desc
        })

print(f"\tleagues")
with Session(engine) as session, session.begin():
    stmt = insert(league_table)
    for idleague, created, notes in leagues:
        session.execute(stmt, {
            'idleague': idleague,
            'date_created': created,
            'notes': notes
        })

print(f"\ttest games")
with Session(engine) as session, session.begin():
    game_stmt = insert(game_table)
    gp_stmt = insert(games_played_table)
    for date_played, game_league, notes, player_records in games:
        res = session.execute(game_stmt, {
            'date': date_played,
            'format': 'commander',
            'league': game_league,
            'notes': notes
        })
        g_id = res.inserted_primary_key[0]

        for player, deck, finish, points, point_note in player_records:
            session.execute(gp_stmt, {
                'idgame': g_id,
                'idplayer': player,
                'iddeck': deck,
                'finish': finish,
                'points': points,
                'point_notes': point_note
            })

print("testing 'complicated' stat queries.")
print("\nDeck Leaderboard:")
with Session(engine) as session:
    res = session.execute(deck_leaderboard_overall())
    for (deck_id, color, desc, comm, wins, plays) in res.fetchall():
        print(f"{deck_id} - {comm} - {wins}/{plays}")

print("\nUser Leaderboard")
with Session(engine) as session:
    res = session.execute(user_leaderboard_overall())
    for (user, wins, played) in res.fetchall():
        print(f"{user} - {wins}/{played}")

player_id = "Chuck"
print(f"\ntesting user summary. player_id: {player_id}")
with Session(engine) as session:
    res = session.execute(player_summary(player_id))
    user_stats = res.fetchall()
    res = session.execute(player_deck_summary(player_id))
    deck_stats = res.fetchall()

    wins, plays = user_stats[0]
    print(f"{player_id}: {wins}/{plays}")
    print("decks:")
    for iddeck, comm, desc, color, wins, plays in deck_stats:
        print(f"{iddeck}, {comm}, {wins}/{plays}")

league_id = "comm-2023-fall"
print(f"\nLeague Leaderboard: {league_id}")
with Session(engine) as session:
    res = session.execute(user_leaderboard_per_league(league_id))
    user_records = res.fetchall()
    for (user, points, wins, played) in user_records:
        print(f"{user} - {points} - {wins}/{played}")
