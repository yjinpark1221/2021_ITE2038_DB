-- 37. Print the total level of Pokémon of type fire among the catched Pokémon.
SELECT SUM(level)
FROM Pokemon, CatchedPokemon
WHERE Pokemon.id = CatchedPokemon.pid
GROUP BY type
HAVING type = 'Fire';