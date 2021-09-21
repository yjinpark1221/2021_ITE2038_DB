-- 10. Print the names of Pokémon that have not been caught by any trainer in alphabetical order.
SELECT name
FROM Pokemon
WHERE id NOT IN (
  SELECT pid FROM CatchedPokemon
)
ORDER BY name;