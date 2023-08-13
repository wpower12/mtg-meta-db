from mtgMetaDB import mapper_registry
from sqlalchemy_utils import database_exists, drop_database, create_database
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, insert, text
from decouple import config

from mtgMetaDB import player_table, deck_table, game_table, games_played_table
from mtgMetaDB.sql import sql_user_lb, sql_get_user_summary, sql_deck_lb, sql_get_user_deck_summary

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

players = ["Apawl", "Bill", "Chuck", "Drnick", "Enathan"]

decks = [
    ["AUNTIE", "Auntie Blyth, Bad Influence", "R", "Red pain.", "Bill"],
    ["KUMENA", "Kumena, Tyrant of Orzaca", "UG", "Mermaids and simic shit.", "Bill"],
    ["BROWN", "Radaghast", "G", "Craterhoof into creatures.", "Chuck"],
    ["KRENKO", "Krenko, Mob Boss", "R", "Goblins.", "Apawl"],
    ["SAURON", "Sauron, Dark Lord", "RBU", "Orcs.", "Drnick"],
    ["FLICKER", "That weird girl", "WUB", "Flicker stuff.", "Enathan"]
]

games = [
    ["2023-01-01", "commander", "fun stuff happened.", [["Bill", "AUNTIE", 1],
                                                        ["Chuck", "BROWN", 2],
                                                        ["Apawl", "KRENKO", 3],
                                                        ["Drnick", "SAURON", 4]]],
    ["2023-01-02", "commander", "fun stuff happened again.", [["Bill", "AUNTIE", 2],
                                                        ["Chuck", "BROWN", 1],
                                                        ["Apawl", "KRENKO", 3],
                                                        ["Drnick", "SAURON", 4]]],
    ["2023-01-01", "commander", "fun stuff happened.", [["Bill", "KUMENA", 3],
                                                        ["Chuck", "BROWN", 2],
                                                        ["Apawl", "KRENKO", 1],
                                                        ["Enathan", "FLICKER", 4]]],
    ["2023-01-01", "commander", "fun stuff happened.", [["Enathan", "FLICKER", 1],
                                                        ["Chuck", "BROWN", 2],
                                                        ["Apawl", "KRENKO", 3],
                                                        ["Drnick", "SAURON", 4]]],
    ["2023-01-01", "commander", "fun stuff happened.", [["Bill", "AUNTIE", 2],
                                                        ["Chuck", "BROWN", 1],
                                                        ["Enathan", "FLICKER", 3],
                                                        ["Drnick", "SAURON", 4]]]
]

print("populating tables")
with Session(engine) as session, session.begin():
    print(f"\tplayers")
    stmt = insert(player_table).prefix_with('IGNORE')
    for player in players:
        session.execute(stmt, {
            'idplayer': player
        })

with Session(engine) as session, session.begin():
    print(f"\tdecks")
    stmt = insert(deck_table)
    for iddeck, comm, color, desc, creator in decks:
        session.execute(stmt, {
            'iddeck': iddeck,
            'creator': creator,
            'commander': comm,
            'color': color,
            'desc': desc
        })

with Session(engine) as session, session.begin():
    print(f"\ttest games")
    game_stmt = insert(game_table)
    gp_stmt = insert(games_played_table)
    for date_played, game_format, notes, player_records in games:
        res = session.execute(game_stmt, {
            'date': date_played,
            'format': game_format,
            'notes': notes
        })
        g_id = res.inserted_primary_key[0]

        for player, deck, finish in player_records:
            session.execute(gp_stmt, {
                'idgame': g_id,
                'idplayer': player,
                'iddeck': deck,
                'finish': finish
            })

print("testing 'complicated' stat queries.")

with Session(engine) as session:
    res = session.execute(text(sql_deck_lb))
    decks = res.fetchall()
    print("\nDeck Leaderboard:")
    for (deck_id, color, desc, comm, wins, plays) in decks:
        print(f"{deck_id} - {comm} - {wins}/{plays}")

with Session(engine) as session:
    res = session.execute(text(sql_user_lb))
    user_records = res.fetchall()
    print("\nUser Leaderboard")
    for (user, wins, played) in user_records:
        print(f"{user} - {wins}/{played}")

user_id = "Bill"
print(f"\ntesting user summary. player_id: {user_id}")
with Session(engine) as session:
    res = session.execute(text(sql_get_user_summary), {'idplayer': user_id})
    user_stats = res.fetchall()
    res = session.execute(text(sql_get_user_deck_summary), {'idplayer': user_id})
    deck_stats = res.fetchall()

    wins, plays = user_stats[0]
    print(f"{user_id}: {wins}/{plays}")
    print("decks:")
    for iddeck, comm, desc, color, wins, plays in deck_stats:
        print(f"{iddeck}, {comm}, {wins}/{plays}")