-- 18. Print the names of Grass-type Pokémon in alphabetical order.
SELECT name
FROM Pokemon
WHERE type = 'Grass'
ORDER BY name;