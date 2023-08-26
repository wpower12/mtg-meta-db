"""
These are the queries that I'm worried won't be easily handled by the SQLAlchemy recipie of using the chained
methods to build up a simple query. The others might be possible, but I worry about the COUNT(IF()) patterns.
"""

sql_get_deck_cards = """
SELECT count, card.name, card.mana_cost, card.typeline, card.scryfall_uri FROM cards
JOIN card on card.oracle_id=cards.oracle_id
WHERE cards.iddeck=:iddeck
order by card.typeline, card.name asc;
"""
