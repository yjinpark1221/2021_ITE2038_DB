-- 11. Among the Pokémon caught by the gym leader of Sangnok City, print the nicknames of the Pokémon whose type is water in alphabetical order.
SELECT CatchedPokemon.nickname
FROM Pokemon, Gym, CatchedPokemon
WHERE CatchedPokemon.owner_id = Gym.leader_id AND CatchedPokemon.pid = Pokemon.id AND Pokemon.type = 'Water' AND Gym.city = 'Sangnok City'
ORDER BY CatchedPokemon.nickname;