-- 39. Print the names of Pokémon caught by the gym leader in Rainbow City in alphabetical order.
SELECT Pokemon.name
FROM Pokemon, Trainer, Gym, CatchedPokemon
WHERE Pokemon.id = CatchedPokemon.pid AND Trainer.id = CatchedPokemon.owner_id AND Gym.leader_id = Trainer.id AND Gym.city = 'Rainbow City'
ORDER BY Pokemon.name;