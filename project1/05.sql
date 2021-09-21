-- 05. Print the average level of Pokémon Red caught.
SELECT AVG(level)
FROM Trainer, CatchedPokemon
WHERE Trainer.id = CatchedPokemon.owner_id AND Trainer.name = 'Red';