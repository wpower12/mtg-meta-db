"""
These are the queries that I'm worried won't be easily handled by the SQLAlchemy recipie of using the chained
methods to build up a simple query. The others might be possible, but I worry about the COUNT(IF()) patterns.
"""

sql_deck_lb = """
SELECT 
    deck.iddeck, deck.color, deck.desc, deck.commander, 
    COUNT(IF(gp.finish=1, 1, Null)) as 'wins', COUNT(*) as 'plays'
FROM games_played as gp
LEFT JOIN deck on gp.iddeck = deck.iddeck
GROUP BY deck.iddeck
order by COUNT(IF(gp.finish=1, 1, Null)) DESC;
"""

sql_user_lb = """
SELECT player.idplayer, COUNT(IF(games_played.finish=1, 1, Null)) as 'wins', COUNT(*) as 'plays' FROM player
LEFT JOIN games_played ON games_played.idplayer=player.idplayer
GROUP BY player.idplayer
order by COUNT(IF(games_played.finish=1, 1, Null)) DESC;
"""

sql_get_deck_cards = """
SELECT count, card.name, card.mana_cost, card.typeline, card.scryfall_uri FROM cards
JOIN card on card.oracle_id=cards.oracle_id
WHERE cards.iddeck=:iddeck
order by card.typeline, card.name asc;
"""


sql_get_user_summary = """
SELECT COUNT(IF(games_played.finish=1, 1, Null)) as 'wins', COUNT(*) as 'plays' FROM player
LEFT JOIN games_played ON games_played.idplayer=player.idplayer
WHERE player.idplayer=:idplayer
GROUP BY player.idplayer
"""

sql_get_user_deck_summary = """
SELECT deck.iddeck, deck.commander, deck.desc, deck.color, COUNT(IF(gp.finish=1, 1, NULL)) as 'wins', COUNT(*) as 'plays'
FROM player 
JOIN deck on deck.creator=player.idplayer
JOIN games_played as gp on gp.iddeck=deck.iddeck
WHERE player.idplayer=:idplayer
GROUP BY deck.iddeck, deck.desc, deck.color;
"""