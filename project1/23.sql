-- 23. Print the names of Pokémon with two-digit Pokémon IDs in alphabetical order.
SELECT name
FROM Pokemon
WHERE id BETWEEN 10 AND 99
ORDER BY name;