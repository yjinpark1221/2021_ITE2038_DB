-- 13. Print the number of Pokémon whose type is not fire.
SELECT COUNT(*)
FROM Pokemon
WHERE type <> 'Fire';