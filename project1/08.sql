-- 08. Print the number of Pokémon caught by each type in alphabetical order by type name.
SELECT COUNT(*)
FROM Pokemon, CatchedPokemon
WHERE Pokemon.id = CatchedPokemon.pid
GROUP BY type
ORDER BY type;