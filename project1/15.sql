-- 15. Print out the names of all trainers from Sangnok city who caught a Pokémon whose name starts with P in alphabetical order.
SELECT DISTINCT Trainer.name
FROM Trainer, CatchedPokemon, Pokemon
WHERE Trainer.id = CatchedPokemon.owner_id AND Pokemon.id = CatchedPokemon.pid AND Pokemon.name LIKE 'P%'
ORDER BY Trainer.name;