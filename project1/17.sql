-- 17. Print the average level of all water-type Pokémon caught by the trainer.
SELECT AVG(level)
FROM CatchedPokemon, Pokemon
WHERE CatchedPokemon.pid = Pokemon.id AND Pokemon.type = 'Water';