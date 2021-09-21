-- 33. Print the names of the trainers who caught Psychic-type Pokémon in alphabetical order.
SELECT Trainer.name
FROM Trainer, CatchedPokemon, Pokemon
WHERE Trainer.id = CatchedPokemon.owner_id AND CatchedPokemon.pid = Pokemon.id AND Pokemon.type = 'Psychic'
ORDER BY Trainer.name;