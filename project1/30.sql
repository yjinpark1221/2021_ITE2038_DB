-- 30. Print the names of Pokémon whose names end in ‘s’ in alphabetical order. (case sensitive x)
SELECT name
FROM Pokemon
WHERE name LIKE '%s' OR name LIKE '%S'
ORDER BY name;