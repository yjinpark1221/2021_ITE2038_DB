-- 24. Print the names of Pokémon whose names start with an alphabetic vowel in alphabetical order.
SELECT name
FROM Pokemon
WHERE name LIKE 'A%' OR name LIKE 'E%' OR name LIKE 'I%' OR name LIKE 'O%' OR name LIKE 'U%' OR name LIKE 'a%' OR name LIKE 'e%' OR name LIKE 'i%' OR name LIKE 'o%' OR name LIKE 'u%'
ORDER BY name;