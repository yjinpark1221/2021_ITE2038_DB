-- 27. Among the catched Pokémon, print the nicknames of the Pokémon with a level 40 or higher and an owner_id of 5 or higher in alphabetical order.
SELECT nickname
FROM CatchedPokemon
WHERE level >= 40 AND owner_id >= 5
ORDER BY nickname;